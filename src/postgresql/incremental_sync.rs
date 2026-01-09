use crate::sync::{ChangeStream, IncrementalSource, SourceDatabase, SyncCheckpoint};
use crate::{SourceOpts, SurrealOpts};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use surreal_sync_surreal::{
    apply_change, surreal_connect, Change, ChangeOp, SurrealOpts as SurrealConnOpts,
};
use surrealdb_types::{convert_id_with_database_schema, json_to_surreal_with_table_schema};
use sync_core::DatabaseSchema;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::debug;

/// Run incremental sync from PostgreSQL to SurrealDB
pub async fn run_incremental_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    from_checkpoint: crate::sync::SyncCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    target_checkpoint: Option<crate::sync::SyncCheckpoint>,
) -> Result<()> {
    log::debug!("🎯 ENTERING run_incremental_sync function with checkpoint: {from_checkpoint:?}");
    info!(
        "Starting PostgreSQL incremental sync from checkpoint: {}",
        from_checkpoint.to_string()
    );

    // Create PostgreSQL incremental source using trigger-based tracking (reliable, no special config needed)
    // Extract sequence_id from checkpoint
    let sequence_id = from_checkpoint.to_postgresql_sequence_id()?;

    log::debug!("🚀 Creating PostgreSQL incremental source");
    let client = super::client::new_postgresql_client(&from_opts.source_uri).await?;
    let mut source = super::incremental_sync::PostgresIncrementalSource::new(client, sequence_id);
    log::debug!("PostgreSQL incremental source created");

    // Initialize source (schema collection)
    log::debug!("🔧 Initializing source");
    source.initialize().await?;
    log::debug!("Source initialized");

    // Get list of user tables to track
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&from_opts.source_uri, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let tables = super::autoconf::get_user_tables(
        &pg_client,
        from_opts.source_database.as_deref().unwrap_or("public"),
    )
    .await?;
    info!("Setting up tracking for tables: {tables:?}");

    // Set up trigger-based tracking - this creates audit tables and triggers
    log::debug!("🔨 Setting up tracking for tables: {tables:?}");
    source.setup_tracking(tables).await?;
    log::debug!("Tracking setup completed");

    let surreal_conn_opts = SurrealConnOpts {
        surreal_endpoint: to_opts.surreal_endpoint.clone(),
        surreal_username: to_opts.surreal_username.clone(),
        surreal_password: to_opts.surreal_password.clone(),
    };
    let surreal = surreal_connect(&surreal_conn_opts, &to_namespace, &to_database).await?;

    // Get change stream
    log::debug!("📡 Getting change stream");
    let mut stream = source.get_changes().await?;
    log::debug!("Change stream obtained");

    info!("Starting to consume PostgreSQL change stream...");

    let mut change_count = 0;
    while let Some(result) = stream.next().await {
        log::debug!("🔄 Main loop: Got result from stream.next(), change_count: {change_count}");
        match result {
            Ok(change) => {
                debug!("Received change: {:?}", change);

                // Check if we've reached the deadline
                if chrono::Utc::now() >= deadline {
                    info!("Reached deadline: {deadline}, stopping incremental sync");
                    break;
                }

                // Check if we've reached the target checkpoint
                if let Some(ref target) = target_checkpoint {
                    if let (
                        Some(crate::sync::SyncCheckpoint::PostgreSQL {
                            sequence_id: current_seq,
                            ..
                        }),
                        crate::sync::SyncCheckpoint::PostgreSQL {
                            sequence_id: target_seq,
                            ..
                        },
                    ) = (stream.checkpoint(), target)
                    {
                        if current_seq >= *target_seq {
                            info!(
                                "Reached target checkpoint: {}, stopping incremental sync",
                                target.to_string()
                            );
                            break;
                        }
                    }
                }

                apply_change(&surreal, &change).await?;

                change_count += 1;
                if change_count % 100 == 0 {
                    info!("Processed {change_count} changes");
                }
            }
            Err(e) => {
                warn!("Error reading change stream: {e}");
                break;
            }
        }

        // Stop after reasonable number of changes to avoid infinite loop
        if change_count >= 1000 {
            info!("Processed {change_count} changes, stopping to prevent infinite loop");
            break;
        }
    }

    info!("PostgreSQL incremental sync completed. Processed {change_count} changes");

    // Cleanup
    log::debug!("Starting cleanup");
    source.cleanup().await?;
    log::debug!("Cleanup completed");

    log::debug!("run_incremental_sync about to return Ok(())");
    Ok(())
}

/// Configuration for tracking a table in PostgreSQL
#[derive(Debug, Clone)]
pub struct TableTrackingConfig {
    /// The table name to track
    pub table_name: String,
    /// The primary key column name for this table
    /// Must be explicitly specified - no auto-detection
    pub id_columns: Vec<String>,
}

/// PostgreSQL incremental sync implementation using trigger-based change tracking
///
/// This implementation provides reliable incremental synchronization from PostgreSQL to SurrealDB.
///
/// The implementation uses database triggers and audit tables to capture data changes.
///
/// Changes are captured by creating an audit table `surreal_sync_changes` that tracks INSERT, UPDATE,
/// and DELETE operations. Database triggers automatically populate this table when data changes occur.
/// The system maintains sequence-based checkpoints for reliable resumption after failures.
/// During incremental sync, the implementation polls the audit table for new changes since the last checkpoint.
///
/// The approach uses only the [`tokio-postgres`] driver, minimizing external dependencies while
/// providing reliable change data capture suitable for most production workloads.
///
/// ## See Also
/// - [`crate::sync::IncrementalSource`] - The trait this implements
/// - [`crate::sync::SyncCheckpoint::PostgreSQL`] - Sequence-based checkpoint format
/// - [`crate::mysql_incremental::MySQLIncrementalSource`] - Similar approach for MySQL
pub struct PostgresIncrementalSource {
    client: Arc<Mutex<Client>>,
    tracking_table: String,
    last_sequence: i64,
    database_schema: Option<DatabaseSchema>,
    /// Mapping of table names to their primary key column names
    /// Used for schema-aware ID type conversion in the change stream
    pk_columns: HashMap<String, Vec<String>>,
}

impl PostgresIncrementalSource {
    pub fn new(client: Arc<Mutex<Client>>, initial_sequence_id: i64) -> Self {
        let tracking_table = "surreal_sync_changes".to_string();

        Self {
            client,
            tracking_table,
            last_sequence: initial_sequence_id,
            database_schema: None,
            pk_columns: HashMap::new(),
        }
    }

    async fn create_audit_table(&self) -> Result<()> {
        let client = self.client.lock().await;

        // Create tracking table
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                sequence_id BIGSERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                row_id JSONB NOT NULL,
                old_data JSONB,
                new_data JSONB,
                changed_at TIMESTAMPTZ DEFAULT NOW()
            )",
            self.tracking_table
        );

        client.simple_query(&create_table).await?;
        info!("Created PostgreSQL audit table: {}", self.tracking_table);
        Ok(())
    }

    pub async fn get_current_sequence(&self) -> Result<i64> {
        let client = self.client.lock().await;

        // Check if audit table exists first
        let table_exists: Vec<tokio_postgres::Row> = client
            .query(
                "SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public'",
                &[&self.tracking_table]
            )
            .await?;

        if table_exists.is_empty() {
            // Audit table doesn't exist yet, return 0
            return Ok(0);
        }

        let rows = client
            .query(
                &format!(
                    "SELECT COALESCE(MAX(sequence_id), 0) FROM {}",
                    self.tracking_table
                ),
                &[],
            )
            .await?;

        if rows.is_empty() {
            return Ok(0);
        }

        let sequence: i64 = rows[0].get(0);
        Ok(sequence)
    }

    /// Query and return all primary key columns for a table (supports composite keys)
    async fn query_composite_primary_id_columns(
        client: &Client,
        table_name: &str,
    ) -> Result<Vec<String>> {
        let pk_check = format!(
            "SELECT a.attname as pk_column
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p' AND c.conrelid = '{table_name}'::regclass
            ORDER BY array_position(c.conkey, a.attnum)"
        );

        let rows = client.query(&pk_check, &[]).await.map_err(|e| {
            anyhow!("Failed to detect primary key columns for table '{table_name}': {e}",)
        })?;

        if rows.is_empty() {
            return Err(anyhow!(
                "Table '{table_name}' has no primary key defined. Primary key is required for incremental sync.",
            ));
        }

        let columns: Vec<String> = rows
            .iter()
            .map(|row| row.get::<_, String>("pk_column"))
            .collect();

        Ok(columns)
    }

    /// Create a PostgreSQL trigger function for tracking changes with explicit PK columns (supports composite keys)
    fn create_trigger_function_for_table_with_pk_columns(
        target_table_name: &str,
        tracking_table_name: &str,
        pk_columns: &[String],
    ) -> (String, String) {
        // Build the pk_value as a JSON array expression (works for single or composite keys)
        let pk_parts: Vec<String> = pk_columns
            .iter()
            .map(|col| format!("row_json->>'{col}'"))
            .collect();
        let pk_concat = format!("jsonb_build_array({})", pk_parts.join(", "));

        let func_name = format!("surreal_sync_track_changes_{target_table_name}");

        let sql = format!(
            "CREATE OR REPLACE FUNCTION {func_name}() RETURNS TRIGGER AS $$
            DECLARE
                pk_json jsonb;
                row_json jsonb;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    row_json := to_jsonb(NEW);
                    pk_json := {pk_concat};
                    -- Check if any PK value is NULL
                    IF pk_json @> 'null'::jsonb THEN
                        RAISE EXCEPTION 'Primary key columns have NULL value in table %', TG_TABLE_NAME;
                    END IF;
                    INSERT INTO {tracking_table_name} (table_name, operation, row_id, new_data)
                    VALUES (TG_TABLE_NAME, TG_OP, pk_json, row_json);
                    RETURN NEW;
                ELSIF TG_OP = 'UPDATE' THEN
                    row_json := to_jsonb(NEW);
                    pk_json := {pk_concat};
                    -- Check if any PK value is NULL
                    IF pk_json @> 'null'::jsonb THEN
                        RAISE EXCEPTION 'Primary key columns have NULL value in table %', TG_TABLE_NAME;
                    END IF;
                    INSERT INTO {tracking_table_name} (table_name, operation, row_id, old_data, new_data)
                    VALUES (TG_TABLE_NAME, TG_OP, pk_json, to_jsonb(OLD), row_json);
                    RETURN NEW;
                ELSIF TG_OP = 'DELETE' THEN
                    row_json := to_jsonb(OLD);
                    pk_json := {pk_concat};
                    -- Check if any PK value is NULL
                    IF pk_json @> 'null'::jsonb THEN
                        RAISE EXCEPTION 'Primary key columns have NULL value in table %', TG_TABLE_NAME;
                    END IF;
                    INSERT INTO {tracking_table_name} (table_name, operation, row_id, old_data)
                    VALUES (TG_TABLE_NAME, TG_OP, pk_json, row_json);
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"
        );

        (func_name, sql)
    }

    /// Set up trigger-based tracking for specified tables (backward compatibility)
    /// Auto-detects primary key columns for each table
    pub async fn setup_tracking(&mut self, tables: Vec<String>) -> Result<()> {
        let client = self.client.lock().await;
        let mut configs: Vec<TableTrackingConfig> = Vec::new();

        // Auto-detect primary key for each table
        for table_name in tables {
            let id_columns = Self::query_composite_primary_id_columns(&client, &table_name).await?;

            // Store PK columns for schema-aware ID conversion in change stream
            self.pk_columns
                .insert(table_name.clone(), id_columns.clone());

            configs.push(TableTrackingConfig {
                table_name,
                id_columns,
            });
        }

        drop(client); // Release the lock before calling setup_tracking_with_config
        self.setup_tracking_with_config(configs).await
    }

    /// Set up trigger-based tracking for specified tables with custom configuration
    pub async fn setup_tracking_with_config(
        &self,
        configs: Vec<TableTrackingConfig>,
    ) -> Result<()> {
        // Create audit table first
        self.create_audit_table().await?;

        let client = self.client.lock().await;

        // Drop any existing triggers and function first to ensure clean state
        let legacy_cleanup_sql = "
            DROP FUNCTION IF EXISTS surreal_sync_track_changes() CASCADE;
            DROP TABLE IF EXISTS surreal_sync_table_id_config CASCADE;
        ";

        if let Err(e) = client.simple_query(legacy_cleanup_sql).await {
            warn!("Failed to cleanup old triggers/functions: {e}");
        }

        // Create triggers on specified tables (excluding the audit table itself)
        for config in &configs {
            let table = &config.table_name;

            // Skip the audit table to prevent infinite recursion
            if table == &self.tracking_table {
                info!("Skipping audit table: {table}");
                continue;
            }

            // Skip legacy config table as well
            if table == "surreal_sync_table_id_config" {
                info!("Skipping config table: {table}");
                continue;
            }

            // Create a robust trigger function that looks up ID column from config
            let (func, trigger_function) = Self::create_trigger_function_for_table_with_pk_columns(
                table,
                &self.tracking_table,
                &config.id_columns,
            );

            // Create the trigger function
            client.simple_query(&trigger_function).await?;
            info!("Created PostgreSQL trigger function");

            info!("Processing table for trigger creation: {table}");

            let trigger = format!("surreal_sync_trigger_{table}");

            // Drop existing trigger first
            let drop_trigger = format!("DROP TRIGGER IF EXISTS {trigger} ON {table}");
            let _ = client.simple_query(&drop_trigger).await;

            let trigger = format!(
                "CREATE TRIGGER {trigger}
                AFTER INSERT OR UPDATE OR DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION {func}()"
            );

            info!("About to create trigger for table: {table}");
            match client.simple_query(&trigger).await {
                Ok(_) => info!("Successfully created tracking trigger for table: {table}"),
                Err(e) => {
                    error!("Failed to create trigger for table {table}: {e}");
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn cleanup_impl(&self) -> Result<()> {
        log::debug!("🧹 PostgresIncrementalSource::cleanup_impl called");
        let client = self.client.lock().await;

        // Drop tracking table and function
        let cleanup = format!(
            "DROP TABLE IF EXISTS {} CASCADE;
            DROP FUNCTION IF EXISTS surreal_sync_track_changes() CASCADE;",
            self.tracking_table
        );

        if let Err(e) = client.simple_query(&cleanup).await {
            warn!("Failed to cleanup tracking resources: {e}");
        }

        Ok(())
    }
}

#[async_trait]
impl IncrementalSource for PostgresIncrementalSource {
    fn source_type(&self) -> SourceDatabase {
        SourceDatabase::PostgreSQL
    }

    async fn initialize(&mut self) -> Result<()> {
        // Collect database schema for type-aware conversion
        let client = self.client.lock().await;
        let schema = super::schema::collect_postgresql_database_schema(&client).await?;
        drop(client); // Release lock before storing schema
        self.database_schema = Some(schema);
        info!("Collected PostgreSQL database schema for type-aware conversion");

        Ok(())
    }

    async fn get_changes(&mut self) -> Result<Box<dyn ChangeStream>> {
        let stream = PostgresChangeStream::new(
            self.client.clone(),
            self.tracking_table.clone(),
            self.last_sequence,
            self.database_schema.clone(),
            self.pk_columns.clone(),
        )
        .await?;

        Ok(Box::new(stream))
    }

    async fn get_checkpoint(&self) -> Result<SyncCheckpoint> {
        Ok(SyncCheckpoint::PostgreSQL {
            sequence_id: self.last_sequence,
            timestamp: Utc::now(),
        })
    }

    async fn cleanup(self) -> Result<()> {
        log::debug!("🧹 PostgresIncrementalSource::cleanup trait method called");
        // Call the implementation method (different name to avoid recursion)
        self.cleanup_impl().await
    }
}

pub struct PostgresChangeStream {
    client: Arc<Mutex<Client>>,
    tracking_table: String,
    last_sequence: i64,
    buffer: Vec<Change>,
    empty_poll_count: usize,
    database_schema: Option<DatabaseSchema>,
    /// Mapping of table names to their primary key column names
    pk_columns: HashMap<String, Vec<String>>,
}

impl PostgresChangeStream {
    async fn new(
        client: Arc<Mutex<Client>>,
        tracking_table: String,
        start_sequence: i64,
        database_schema: Option<DatabaseSchema>,
        pk_columns: HashMap<String, Vec<String>>,
    ) -> Result<Self> {
        Ok(Self {
            client,
            tracking_table,
            last_sequence: start_sequence,
            buffer: Vec::new(),
            empty_poll_count: 0,
            database_schema,
            pk_columns,
        })
    }

    async fn fetch_changes(&mut self) -> Result<Vec<Change>> {
        log::debug!(
            "PostgresChangeStream::fetch_changes() called, last_sequence: {}",
            self.last_sequence
        );
        let client = self.client.lock().await;

        // Check if audit table exists first
        let table_exists: Vec<tokio_postgres::Row> = client
            .query(
                "SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public'",
                &[&self.tracking_table]
            )
            .await?;

        if table_exists.is_empty() {
            // Audit table doesn't exist yet, return empty changes
            return Ok(Vec::new());
        }

        let query = format!(
            "SELECT sequence_id, table_name, operation, row_id, old_data, new_data, changed_at
            FROM {}
            WHERE sequence_id > $1
            ORDER BY sequence_id
            LIMIT 100",
            self.tracking_table
        );

        let rows = client.query(&query, &[&self.last_sequence]).await?;
        log::debug!(
            "PostgresChangeStream::fetch_changes() got {} rows from audit table",
            rows.len()
        );
        let mut changes = Vec::new();

        for row in rows {
            let sequence_id: i64 = row.get(0);
            let table_name: String = row.get(1);
            let operation: String = row.get(2);
            let row_id: Option<JsonValue> = row.get(3);
            let old_data: Option<JsonValue> = row.get(4);
            let new_data: Option<JsonValue> = row.get(5);
            let _changed_at: DateTime<Utc> = row.get(6);

            let json_data = match operation.as_str() {
                "INSERT" | "UPDATE" => new_data,
                "DELETE" => old_data,
                _ => None,
            };

            let op = match operation.as_str() {
                "INSERT" => ChangeOp::Create,
                "UPDATE" => ChangeOp::Update,
                "DELETE" => ChangeOp::Delete,
                _ => {
                    warn!("Unknown operation type in audit table: {operation:?}");
                    continue;
                }
            };

            // Get PK columns for this table (needed for filtering content data)
            let pk_cols = self.pk_columns.get(&table_name).ok_or_else(|| {
                anyhow!(
                    "No PK column information available for table '{table_name}'. \
                    Was setup_tracking() called for this table?"
                )
            })?;

            let surreal_data = if let Some(json_value) = json_data {
                // Use schema-aware conversion if schema is available
                if let Some(table_schema) = self
                    .database_schema
                    .as_ref()
                    .and_then(|s| s.get_table(&table_name))
                {
                    // Convert JSON object using schema information
                    // NOTE: We must filter out ALL primary key column(s) from the content data.
                    // SurrealDB's `UPSERT $record_id CONTENT $content` will fail if `id` is
                    // present in the content (error: "Found X for the `id` field, but a
                    // specific record has been specified").
                    //
                    // We use the pk_columns mapping to know which columns to filter, and validate
                    // that the filtered values match the row_id JSONB array.
                    match json_value {
                        serde_json::Value::Object(map) => {
                            let mut m = std::collections::HashMap::new();
                            for (key, val) in map {
                                // Check if this column is a primary key column
                                if let Some(pk_index) = pk_cols.iter().position(|col| col == &key) {
                                    // Validate that the JSON value matches the corresponding row_id element
                                    if let Some(serde_json::Value::Array(arr)) = &row_id {
                                        if pk_index < arr.len() {
                                            let json_val_str = match &val {
                                                serde_json::Value::Number(n) => n.to_string(),
                                                serde_json::Value::String(s) => s.clone(),
                                                other => format!("{other}"),
                                            };
                                            let row_id_str = match &arr[pk_index] {
                                                serde_json::Value::Number(n) => n.to_string(),
                                                serde_json::Value::String(s) => s.clone(),
                                                other => format!("{other}"),
                                            };
                                            if json_val_str != row_id_str {
                                                anyhow::bail!(
                                                    "row_id and JSON PK field mismatch in table '{table_name}': \
                                                    row_id[{pk_index}]={row_id_str}, json_{key}={json_val_str}."
                                                );
                                            }
                                        }
                                    }
                                    // Skip PK columns - they're used as record ID, not content
                                    continue;
                                }
                                let v = json_to_surreal_with_table_schema(val, &key, table_schema)?;
                                m.insert(key, v);
                            }
                            m
                        }
                        _ => {
                            return Err(anyhow!(
                                "Unexpected non-object data in audit table for table '{table_name}'",
                            ));
                        }
                    }
                } else {
                    anyhow::bail!(
                        "No schema information available for table '{table_name}'. Cannot convert data.",
                    );
                }
            } else {
                HashMap::new()
            };

            // Convert row_id to proper type using schema information
            // The row_id is a JSONB array (supports composite PKs) where each element
            // is extracted via row_json->>'column', which always returns text in PostgreSQL.
            //
            // We use the pk_columns mapping (populated during setup_tracking) to know which
            // column names correspond to each position in the row_id array, then use the
            // database_schema to look up the proper type for conversion.
            let record_id = match row_id {
                Some(serde_json::Value::Array(arr)) => {
                    // pk_cols was already retrieved above for filtering content data
                    if arr.len() != pk_cols.len() {
                        anyhow::bail!(
                            "Mismatch between row_id array length ({}) and PK columns ({}) for table '{table_name}'",
                            arr.len(),
                            pk_cols.len()
                        );
                    }

                    if arr.len() == 1 {
                        // Single primary key - use schema-aware conversion
                        let id_str = match &arr[0] {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            other => format!("{other}"),
                        };

                        let pk_column = &pk_cols[0];

                        // Use schema-aware conversion if schema is available
                        if let Some(schema) = &self.database_schema {
                            convert_id_with_database_schema(
                                &id_str,
                                &table_name,
                                pk_column,
                                schema,
                            )?
                        } else {
                            // Fallback to value-based inference if no schema
                            if let Ok(n) = id_str.parse::<i64>() {
                                surrealdb::sql::Id::Number(n)
                            } else if let Ok(uuid) = uuid::Uuid::parse_str(&id_str) {
                                surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(uuid))
                            } else {
                                surrealdb::sql::Id::from(surrealdb::sql::Strand::from(id_str))
                            }
                        }
                    } else {
                        // Composite primary key - convert each value using schema
                        let mut surrealdb_values_array = Vec::new();
                        for (i, item) in arr.into_iter().enumerate() {
                            let id_str = match item {
                                serde_json::Value::String(s) => s,
                                serde_json::Value::Number(n) => n.to_string(),
                                other => format!("{other}"),
                            };

                            let pk_column = &pk_cols[i];

                            // Use schema-aware conversion if schema is available
                            let surreal_id = if let Some(schema) = &self.database_schema {
                                convert_id_with_database_schema(
                                    &id_str,
                                    &table_name,
                                    pk_column,
                                    schema,
                                )?
                            } else {
                                // Fallback to string if no schema
                                surrealdb::sql::Id::String(id_str)
                            };

                            // Convert Id to Value for the array
                            let value = match surreal_id {
                                surrealdb::sql::Id::Number(n) => {
                                    surrealdb::sql::Value::Number(surrealdb::sql::Number::Int(n))
                                }
                                surrealdb::sql::Id::String(s) => {
                                    surrealdb::sql::Value::Strand(s.into())
                                }
                                surrealdb::sql::Id::Uuid(u) => surrealdb::sql::Value::Uuid(u),
                                other => {
                                    anyhow::bail!(
                                        "Unsupported ID type in composite PK for table '{table_name}': {other:?}"
                                    );
                                }
                            };
                            surrealdb_values_array.push(value);
                        }
                        surrealdb::sql::Id::from(surrealdb_values_array)
                    }
                }
                value => return Err(anyhow!("Unsupported JSON value type: {value:?}")),
            };

            info!("Change event: record_id: {record_id:?}");

            changes.push(Change::record(
                op,
                surrealdb::sql::Thing::from((table_name, record_id)),
                surreal_data,
            ));

            self.last_sequence = sequence_id;
        }

        Ok(changes)
    }
}

#[async_trait]
impl ChangeStream for PostgresChangeStream {
    async fn next(&mut self) -> Option<Result<Change>> {
        log::debug!(
            "🔄 PostgresChangeStream::next() called, empty_poll_count: {}, last_sequence: {}",
            self.empty_poll_count,
            self.last_sequence
        );
        loop {
            // Return buffered changes first
            if !self.buffer.is_empty() {
                return Some(Ok(self.buffer.remove(0)));
            }

            // Fetch new changes
            match self.fetch_changes().await {
                Ok(mut changes) => {
                    if changes.is_empty() {
                        // No more changes, add a small delay to avoid busy waiting
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        self.empty_poll_count += 1;

                        // Return None after 3 empty polls to prevent infinite loops in tests
                        if self.empty_poll_count >= 3 {
                            return None;
                        }
                        // Continue the loop to keep polling
                    } else {
                        // Reset empty poll count when we find changes
                        self.empty_poll_count = 0;
                        // Buffer the changes and return the first one
                        self.buffer.append(&mut changes);
                        return Some(Ok(self.buffer.remove(0)));
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn checkpoint(&self) -> Option<SyncCheckpoint> {
        Some(SyncCheckpoint::PostgreSQL {
            sequence_id: self.last_sequence,
            timestamp: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sequence_parsing() {
        let sequence_id = 12345i64;
        let checkpoint = SyncCheckpoint::PostgreSQL {
            sequence_id,
            timestamp: Utc::now(),
        };

        match checkpoint {
            SyncCheckpoint::PostgreSQL {
                sequence_id: parsed_seq,
                ..
            } => {
                assert_eq!(parsed_seq, sequence_id);
            }
            _ => panic!("Wrong checkpoint type"),
        }
    }
}
