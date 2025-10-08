use crate::sync::{
    ChangeEvent, ChangeStream, IncrementalSource, Operation, SourceDatabase, SyncCheckpoint,
};
use crate::types::SurrealValue;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

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
    database_schema: Option<crate::schema::DatabaseSchema>,
}

impl PostgresIncrementalSource {
    pub fn new(client: Arc<Mutex<Client>>, initial_sequence_id: i64) -> Self {
        let tracking_table = "surreal_sync_changes".to_string();

        Self {
            client,
            tracking_table,
            last_sequence: initial_sequence_id,
            database_schema: None,
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
    pub async fn setup_tracking(&self, tables: Vec<String>) -> Result<()> {
        let client = self.client.lock().await;
        let mut configs: Vec<TableTrackingConfig> = Vec::new();

        // Auto-detect primary key for each table
        for table_name in tables {
            let id_columns = Self::query_composite_primary_id_columns(&client, &table_name).await?;

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
        let schema = super::schema::collect_postgresql_schema(&client, "public").await?;
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
    buffer: Vec<ChangeEvent>,
    empty_poll_count: usize,
    database_schema: Option<crate::schema::DatabaseSchema>,
}

impl PostgresChangeStream {
    async fn new(
        client: Arc<Mutex<Client>>,
        tracking_table: String,
        start_sequence: i64,
        database_schema: Option<crate::schema::DatabaseSchema>,
    ) -> Result<Self> {
        Ok(Self {
            client,
            tracking_table,
            last_sequence: start_sequence,
            buffer: Vec::new(),
            empty_poll_count: 0,
            database_schema,
        })
    }

    async fn fetch_changes(&mut self) -> Result<Vec<ChangeEvent>> {
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

            let data = match operation.as_str() {
                "INSERT" | "UPDATE" => new_data,
                "DELETE" => old_data,
                _ => None,
            };

            let op = match operation.as_str() {
                "INSERT" => Operation::Create,
                "UPDATE" => Operation::Update,
                "DELETE" => Operation::Delete,
                _ => {
                    warn!("Unknown operation type in audit table: {operation:?}");
                    continue;
                }
            };

            let bindable_data = if let Some(json_data) = data {
                // Use schema-aware conversion if schema is available
                if let (Some(_schema), Some(table_schema)) = (
                    &self.database_schema,
                    self.database_schema
                        .as_ref()
                        .and_then(|s| s.tables.get(&table_name)),
                ) {
                    // Convert JSON object using schema information
                    match json_data {
                        serde_json::Value::Object(map) => {
                            let mut m = std::collections::HashMap::new();
                            for (key, val) in map {
                                let v = crate::json_to_sureral(val, &key, table_schema)?;
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
                    // Fallback to basic JSON conversion if no schema available
                    crate::json_to_bindable_map(json_data)?
                }
            } else {
                HashMap::new()
            };

            let record_id = match row_id {
                Some(serde_json::Value::Array(arr)) => {
                    if arr.len() == 1 {
                        // Single primary key value
                        match crate::types::json_value_to_bindable(arr[0].clone())? {
                            SurrealValue::String(s) => {
                                surrealdb::sql::Id::from(surrealdb::sql::Strand::from(s))
                            }
                            SurrealValue::Int(i) => surrealdb::sql::Id::from(i),
                            v => {
                                anyhow::bail!(
                                    "Unsupported row_id type in audit table for table '{table_name}': {v:?}",
                                );
                            }
                        }
                    } else {
                        let mut surrealdb_values_array = Vec::new();
                        for item in arr {
                            match crate::types::json_value_to_bindable(item)? {
                                SurrealValue::String(s) => {
                                    surrealdb_values_array
                                        .push(surrealdb::sql::Value::Strand(s.into()));
                                }
                                v => {
                                    anyhow::bail!(
                                        "Unsupported row_id array item type in audit table for table '{table_name}': {v:?}",
                                    );
                                }
                            }
                        }
                        surrealdb::sql::Id::from(surrealdb_values_array)
                    }
                }
                // Some(serde_json::Value::String(s)) => {
                //     surrealdb::sql::Id::from(surrealdb::sql::Strand::from(s))
                // }
                // Some(serde_json::Value::Number(n)) if n.is_i64() => {
                //     surrealdb::sql::Id::from(n.as_i64().unwrap())
                // }
                value => return Err(anyhow!("Unsupported JSON value type: {value:?}")),
            };

            info!("Change event: record_id: {record_id:?}");

            changes.push(ChangeEvent::record(
                op,
                surrealdb::sql::Thing::from((table_name, record_id)),
                bindable_data,
            ));

            self.last_sequence = sequence_id;
        }

        Ok(changes)
    }
}

#[async_trait]
impl ChangeStream for PostgresChangeStream {
    async fn next(&mut self) -> Option<Result<ChangeEvent>> {
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
