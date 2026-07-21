//! MySQL full sync implementation using TypedValue conversion path.
//!
//! This module uses the unified type conversion flow:
//! MySQL Row → TypedValue (mysql-types) → UniversalRow (sync-core) → SurrealDB (surreal sink)

use crate::{SourceOpts, SyncOpts};
use anyhow::Result;
use async_trait::async_trait;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use mysql_async::{prelude::*, Params, Pool, Row, Value};
use mysql_types::{row_to_typed_values_with_config, RowConversionConfig};
use std::collections::HashMap;
use std::sync::Arc;
use surreal_sink::SurrealSink;
use sync_core::{UniversalRow, UniversalType, UniversalValue};
use sync_transform::{
    run_source_runtime_with, write_rows, ApplyOpts, Pipeline, RowChunkDriver, RowChunkSource,
    SourceRuntimeOpts,
};
use tracing::{debug, info, warn};

/// Sanitize connection string for logging (hide password)
fn sanitize_connection_string(uri: &str) -> String {
    // Replace password in mysql://user:pass@host/db format
    if let Some(at_pos) = uri.find('@') {
        if let Some(colon_pos) = uri[..at_pos].rfind(':') {
            // Check if this looks like a URL with credentials
            if uri[..colon_pos].rfind('/').is_some() || uri[..colon_pos].contains("://") {
                // mysql://user:pass@host -> mysql://user:***@host
                return format!("{}:***{}", &uri[..colon_pos], &uri[at_pos..]);
            }
        }
    }
    uri.to_string()
}

/// Main entry point for MySQL to SurrealDB migration with checkpoint support (identity).
pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_with_transforms(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Full sync with an explicit transform pipeline.
pub async fn run_full_sync_with_transforms<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Starting MySQL migration to SurrealDB");
    if pipeline.is_identity() {
        debug!("Full sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            "Full sync using transform pipeline"
        );
    }

    // Create connection pool with better error context
    let pool = Pool::from_url(&from_opts.source_uri).map_err(|e| {
        anyhow::anyhow!(
            "Failed to create MySQL connection pool from URI '{}': {}",
            sanitize_connection_string(&from_opts.source_uri),
            e
        )
    })?;

    let mut conn = pool.get_conn().await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to connect to MySQL at '{}': {}",
            sanitize_connection_string(&from_opts.source_uri),
            e
        )
    })?;

    // Get database name from options or connection
    let database_name = if let Some(db) = &from_opts.source_database {
        db.clone()
    } else {
        // Extract from connection string if possible
        let current_db: Option<String> = conn.query_first("SELECT DATABASE()").await?;
        current_db.ok_or_else(|| anyhow::anyhow!("No database selected"))?
    };

    // Switch to the target database
    conn.query_drop(format!("USE {database_name}")).await?;

    // Emit checkpoint t1 (before full sync starts) if configured
    if let Some(manager) = sync_manager {
        // Set up triggers and audit table FIRST to establish incremental sync infrastructure
        super::change_tracking::setup_mysql_change_tracking(&mut conn, &database_name).await?;
        info!("Set up MySQL triggers and audit table for incremental sync");

        // Get current sequence_id from the NOW-EXISTING audit table
        let checkpoint = super::checkpoint::get_current_checkpoint(&mut conn).await?;

        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;

        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_cli_string()
        );
    }

    // Collect schema information for boolean column detection
    let schema_info = collect_schema_info(&mut conn, &database_name).await?;
    info!("Collected MySQL schema information");

    // Get list of tables to migrate (excluding system tables)
    let tables = get_user_tables(&mut conn, &database_name).await?;

    info!("Found {} tables to migrate", tables.len());

    let mut total_migrated = 0;

    // Migrate each table
    for table_name in &tables {
        info!("Migrating table: {}", table_name);

        let boolean_paths = from_opts.mysql_boolean_paths.clone().unwrap_or_default();
        let count = migrate_table(
            &mut conn,
            surreal,
            table_name,
            schema_info.get(table_name),
            &MigrateTableOpts {
                sync_opts,
                json_path_overrides: &boolean_paths,
                pipeline,
                apply_opts,
            },
        )
        .await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(manager) = sync_manager {
        // Get current checkpoint after migration
        let checkpoint = super::checkpoint::get_current_checkpoint(&mut conn).await?;

        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;

        info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_cli_string()
        );
    }

    // Clean up connection
    drop(conn);
    pool.disconnect().await?;

    info!(
        "MySQL migration completed: {} total records migrated",
        total_migrated
    );
    Ok(())
}

/// Schema information for a table, used for type-aware conversion
#[derive(Debug, Clone, Default)]
struct TableSchemaInfo {
    /// Columns that should be treated as boolean (TINYINT(1))
    boolean_columns: Vec<String>,
    /// Columns that are SET type (comma-separated values -> array)
    set_columns: Vec<String>,
    /// Columns that hold JSON (native MySQL JSON or MariaDB `LONGTEXT`-backed
    /// JSON), so they convert to a nested JSON value rather than text.
    json_columns: Vec<String>,
}

/// Collect schema information for all tables in the database
async fn collect_schema_info(
    conn: &mut mysql_async::Conn,
    database_name: &str,
) -> Result<HashMap<String, TableSchemaInfo>> {
    let mut schema_info: HashMap<String, TableSchemaInfo> = HashMap::new();

    // Query to find TINYINT(1) columns which are typically boolean
    let boolean_rows: Vec<Row> = conn
        .query(
            r#"
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND COLUMN_TYPE = 'tinyint(1)'
            ORDER BY TABLE_NAME, COLUMN_NAME
            "#,
        )
        .await?;

    for row in boolean_rows {
        let table_name: String = row.get("TABLE_NAME").unwrap_or_default();
        let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();

        schema_info
            .entry(table_name)
            .or_default()
            .boolean_columns
            .push(column_name);
    }

    // Query to find SET columns
    let set_rows: Vec<Row> = conn
        .query(
            r#"
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND DATA_TYPE = 'set'
            ORDER BY TABLE_NAME, COLUMN_NAME
            "#,
        )
        .await?;

    for row in set_rows {
        let table_name: String = row.get("TABLE_NAME").unwrap_or_default();
        let column_name: String = row.get("COLUMN_NAME").unwrap_or_default();

        schema_info
            .entry(table_name)
            .or_default()
            .set_columns
            .push(column_name);
    }

    // Detect JSON columns (native MySQL JSON + MariaDB `json_valid`-checked
    // LONGTEXT) so full-sync reads convert them to nested JSON on both engines.
    let json_columns_by_table = crate::json_columns::get_json_columns(conn, database_name).await?;
    for (table_name, columns) in json_columns_by_table {
        schema_info.entry(table_name).or_default().json_columns = columns;
    }

    Ok(schema_info)
}

/// Get list of user tables (excluding system tables and our audit table)
async fn get_user_tables(conn: &mut mysql_async::Conn, database: &str) -> Result<Vec<String>> {
    let rows: Vec<Row> = conn
        .query(format!(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{database}' \
             AND TABLE_TYPE = 'BASE TABLE' \
             AND TABLE_NAME NOT IN ('surreal_sync_audit')"
        ))
        .await?;

    let tables: Vec<String> = rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("TABLE_NAME"))
        .collect();

    Ok(tables)
}

/// Get primary key column names for a table, in key ordinal order (returns an
/// empty vec if the table has no primary key).
pub async fn get_primary_key_columns(
    conn: &mut mysql_async::Conn,
    database: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows: Vec<Row> = conn
        .query(format!(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}' \
             AND CONSTRAINT_NAME = 'PRIMARY' \
             ORDER BY ORDINAL_POSITION"
        ))
        .await?;

    let pk_columns: Vec<String> = rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>("COLUMN_NAME"))
        .collect();

    Ok(pk_columns)
}

/// Get the current database name
async fn get_current_database(conn: &mut mysql_async::Conn) -> Result<String> {
    let db: Option<String> = conn.query_first("SELECT DATABASE()").await?;
    db.ok_or_else(|| anyhow::anyhow!("No database selected"))
}

/// Options for [`migrate_table`] (keeps the call site under clippy's arg limit).
struct MigrateTableOpts<'a> {
    sync_opts: &'a SyncOpts,
    /// Reserved for JSON path overrides (currently unused by trigger full sync).
    #[allow(dead_code)]
    json_path_overrides: &'a [String],
    pipeline: &'a Pipeline,
    apply_opts: &'a ApplyOpts,
}

/// Migrate a single table from MySQL to SurrealDB
async fn migrate_table<S: SurrealSink>(
    conn: &mut mysql_async::Conn,
    surreal: &S,
    table_name: &str,
    schema_info: Option<&TableSchemaInfo>,
    opts: &MigrateTableOpts<'_>,
) -> Result<usize> {
    // Get primary key columns for this table
    let database = get_current_database(conn).await?;
    let pk_columns = get_primary_key_columns(conn, &database, table_name).await?;

    debug!("Table {} primary key columns: {:?}", table_name, pk_columns);

    // Extract boolean and SET columns from schema info
    let boolean_columns: Vec<String> = schema_info
        .map(|s| s.boolean_columns.clone())
        .unwrap_or_default();
    let set_columns: Vec<String> = schema_info
        .map(|s| s.set_columns.clone())
        .unwrap_or_default();
    let json_columns: Vec<String> = schema_info
        .map(|s| s.json_columns.clone())
        .unwrap_or_default();

    if !set_columns.is_empty() {
        debug!("Table {} has SET columns: {:?}", table_name, set_columns);
    }

    let config = RowConversionConfig {
        boolean_columns: boolean_columns.clone(),
        set_columns: set_columns.clone(),
        json_columns: json_columns.clone(),
        json_config: None,
    };

    let batch_size = opts.sync_opts.batch_size.max(1);

    if !pk_columns.is_empty() {
        if opts.sync_opts.dry_run {
            let mut total_processed = 0usize;
            let mut after: Option<Vec<UniversalValue>> = None;
            loop {
                let chunk = read_table_chunk(
                    conn,
                    table_name,
                    &pk_columns,
                    after.as_deref(),
                    batch_size,
                    &config,
                )
                .await?;
                if chunk.rows.is_empty() {
                    break;
                }
                let n = chunk.rows.len();
                debug!("Dry-run: Would insert {n} records into {table_name}");
                total_processed += n;
                after = chunk.last_pk;
                if n < batch_size {
                    break;
                }
            }
            return Ok(total_processed);
        }

        struct MysqlKeysetChunks<'a> {
            conn: &'a mut mysql_async::Conn,
            table_name: &'a str,
            pk_columns: &'a [String],
            after: Option<Vec<UniversalValue>>,
            batch_size: usize,
            config: &'a RowConversionConfig,
            exhausted: bool,
        }

        #[async_trait]
        impl RowChunkSource for MysqlKeysetChunks<'_> {
            async fn next_chunk(&mut self) -> Result<Option<Vec<UniversalRow>>> {
                if self.exhausted {
                    return Ok(None);
                }
                let chunk = read_table_chunk(
                    self.conn,
                    self.table_name,
                    self.pk_columns,
                    self.after.as_deref(),
                    self.batch_size,
                    self.config,
                )
                .await?;
                if chunk.rows.is_empty() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let n = chunk.rows.len();
                self.after = chunk.last_pk;
                if n < self.batch_size {
                    self.exhausted = true;
                }
                Ok(Some(chunk.rows))
            }
        }

        let chunks = MysqlKeysetChunks {
            conn,
            table_name,
            pk_columns: &pk_columns,
            after: None,
            batch_size,
            config: &config,
            exhausted: false,
        };
        let mut driver = RowChunkDriver::new(chunks);
        let transformer = Arc::new(opts.pipeline.clone());
        let runtime_opts = SourceRuntimeOpts::new();
        run_source_runtime_with(
            &mut driver,
            surreal,
            transformer,
            opts.apply_opts,
            &runtime_opts,
        )
        .await?;
        return Ok(driver.sunk_count() as usize);
    }

    warn!(
        "Table '{table_name}' has no primary key; falling back to full SELECT * (loads table into memory)"
    );

    // Query all data from table
    let rows: Vec<Row> = conn
        .query(format!("SELECT * FROM {table_name}"))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query table {table_name}: {e}"))?;

    let count = rows.len();
    debug!("Retrieved {} rows from table {}", count, table_name);

    let mut batch = Vec::new();
    let mut total_processed = 0;

    // Process each row
    for (row_index, row) in rows.iter().enumerate() {
        // Convert MySQL row to TypedValues using schema-aware config
        let typed_values = row_to_typed_values_with_config(row, &config)?;

        // Extract UniversalValues from TypedValues
        let values: HashMap<String, UniversalValue> = typed_values
            .into_iter()
            .map(|(k, tv)| (k, tv.value))
            .collect();

        // Extract primary key value for the record ID
        let id = extract_primary_key_value(&values, &pk_columns).unwrap_or_else(|_| {
            UniversalValue::Int64(row_index as i64)
        });

        // Build non-PK fields map
        let fields: HashMap<String, UniversalValue> = values
            .into_iter()
            .filter(|(k, _)| !pk_columns.contains(k))
            .collect();

        // Create UniversalRow
        let universal_row = UniversalRow::new(table_name.to_string(), row_index as u64, id, fields);
        batch.push(universal_row);

        // Process batch when it reaches the configured size
        if batch.len() >= batch_size {
            let n = batch.len();

            if !opts.sync_opts.dry_run {
                write_rows(
                    surreal,
                    opts.pipeline,
                    std::mem::take(&mut batch),
                    opts.apply_opts,
                )
                .await?;
            } else {
                debug!(
                    "Dry-run: Would insert {} records into {}",
                    n, table_name
                );
                batch.clear();
            }
            total_processed += n;
        }
    }

    // Process remaining records
    if !batch.is_empty() {
        let n = batch.len();

        if !opts.sync_opts.dry_run {
            write_rows(surreal, opts.pipeline, batch, opts.apply_opts).await?;
        } else {
            debug!(
                "Dry-run: Would insert {} records into {}",
                n, table_name
            );
        }
        total_processed += n;
    }

    Ok(total_processed)
}

/// A primary-key-ordered chunk of rows read from a table, together with the
/// cursor needed to continue keyset pagination.
#[derive(Debug, Clone)]
pub struct TableChunk {
    /// The converted rows, in primary-key order.
    pub rows: Vec<UniversalRow>,
    /// Primary-key values of the last row in `rows`, in primary-key column
    /// order. `None` when no rows were returned. Pass this back as `after` to
    /// read the following chunk.
    pub last_pk: Option<Vec<UniversalValue>>,
}

/// Read a single primary-key-ordered chunk of a table using keyset pagination.
///
/// Rows are ordered by the table's primary key column(s). When `after` is
/// `None`, reading starts from the beginning; otherwise only rows strictly
/// greater than `after` (by row-value comparison over the primary-key columns)
/// are returned. A single primary-key column degenerates to `pk > ?`; a
/// composite primary key uses `(a, b, ...) > (?, ?, ...)`. At most `limit` rows
/// are returned.
///
/// This is an additive, chunked alternative to the monolithic `SELECT *` full
/// sync; it does not write to a sink. `config` supplies the schema-aware
/// boolean/SET column handling used elsewhere in this module.
pub async fn read_table_chunk(
    conn: &mut mysql_async::Conn,
    table_name: &str,
    pk_columns: &[String],
    after: Option<&[UniversalValue]>,
    limit: usize,
    config: &RowConversionConfig,
) -> Result<TableChunk> {
    if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table '{table_name}' has no primary key columns; keyset chunk reads require a primary key"
        ));
    }

    let order_by = pk_columns.join(", ");

    let (where_clause, bind_values): (String, Vec<Value>) = match after {
        Some(cursor) => {
            if cursor.len() != pk_columns.len() {
                return Err(anyhow::anyhow!(
                    "Keyset cursor length ({}) does not match primary key column count ({}) for table '{table_name}'",
                    cursor.len(),
                    pk_columns.len()
                ));
            }
            let placeholders = vec!["?"; pk_columns.len()];
            let clause = if pk_columns.len() == 1 {
                format!("WHERE {} > ?", pk_columns[0])
            } else {
                format!(
                    "WHERE ({}) > ({})",
                    pk_columns.join(", "),
                    placeholders.join(", ")
                )
            };
            let values = cursor
                .iter()
                .map(pk_value_to_mysql_value)
                .collect::<Result<Vec<_>>>()?;
            (clause, values)
        }
        None => (String::new(), Vec::new()),
    };

    let query =
        format!("SELECT * FROM {table_name} {where_clause} ORDER BY {order_by} LIMIT {limit}");
    debug!("Chunk-reading table {table_name} with: {query}");

    let rows: Vec<Row> = conn
        .exec(query, Params::Positional(bind_values))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read chunk from table {table_name}: {e}"))?;

    let mut out = Vec::with_capacity(rows.len());
    let mut last_pk: Option<Vec<UniversalValue>> = None;

    for (row_index, row) in rows.iter().enumerate() {
        let typed_values = row_to_typed_values_with_config(row, config)?;
        let values: HashMap<String, UniversalValue> = typed_values
            .into_iter()
            .map(|(k, tv)| (k, tv.value))
            .collect();

        let id = extract_primary_key_value(&values, pk_columns)?;

        let mut cursor_values = Vec::with_capacity(pk_columns.len());
        for col in pk_columns {
            let v = values.get(col).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Primary key column '{col}' not found in row for table '{table_name}'"
                )
            })?;
            cursor_values.push(v);
        }
        last_pk = Some(cursor_values);

        let fields: HashMap<String, UniversalValue> = values
            .into_iter()
            .filter(|(k, _)| !pk_columns.contains(k))
            .collect();

        out.push(UniversalRow::new(
            table_name.to_string(),
            row_index as u64,
            id,
            fields,
        ));
    }

    Ok(TableChunk { rows: out, last_pk })
}

/// Convert a primary-key `UniversalValue` into a MySQL bind value for keyset
/// pagination.
fn pk_value_to_mysql_value(value: &UniversalValue) -> Result<Value> {
    Ok(match value {
        UniversalValue::Int8 { value, .. } => Value::Int(*value as i64),
        UniversalValue::Int16(v) => Value::Int(*v as i64),
        UniversalValue::Int32(v) => Value::Int(*v as i64),
        UniversalValue::Int64(v) => Value::Int(*v),
        UniversalValue::Text(v) => Value::Bytes(v.clone().into_bytes()),
        UniversalValue::VarChar { value, .. } => Value::Bytes(value.clone().into_bytes()),
        UniversalValue::Char { value, .. } => Value::Bytes(value.clone().into_bytes()),
        UniversalValue::Uuid(v) => Value::Bytes(v.to_string().into_bytes()),
        other => {
            return Err(anyhow::anyhow!(
                "Unsupported primary key value type for keyset pagination: {other:?}"
            ))
        }
    })
}

/// Extract primary key value from values map
fn extract_primary_key_value(
    values: &HashMap<String, UniversalValue>,
    pk_columns: &[String],
) -> Result<UniversalValue> {
    if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table has no primary key defined - primary key is required for sync"
        ));
    }

    if pk_columns.len() == 1 {
        // Single primary key column
        let pk_col = &pk_columns[0];
        values
            .get(pk_col)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Primary key column '{pk_col}' not found in row"))
    } else {
        // Composite primary key - create an array
        let mut pk_values = Vec::new();
        for col in pk_columns {
            let v = values
                .get(col)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Primary key column '{col}' not found in row"))?;
            pk_values.push(v);
        }
        Ok(UniversalValue::Array {
            elements: pk_values,
            element_type: Box::new(UniversalType::Text),
        })
    }
}
