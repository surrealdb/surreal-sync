//! MySQL full sync implementation using TypedValue conversion path.
//!
//! This module uses the unified type conversion flow:
//! MySQL MysqlRow → TypedValue (surreal-sync-mysql) → Row (sync-core) → SurrealDB (surreal sink)

use crate::from_trigger::{SourceOpts, SyncOpts};
use crate::{get_primary_key_columns, read_table_chunk};
use crate::{row_to_typed_values_with_config, RowConversionConfig};
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{prelude::*, Row as MysqlRow};
use std::collections::HashMap;
use std::sync::Arc;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sync_core::{Row, Value};
use surreal_sync_runtime::{
    run_source_runtime_with, ApplyOpts, Pipeline, RowChunkDriver, RowChunkSource, SourceRuntimeOpts,
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
    let pool = super::client::new_mysql_pool_with_ssl(&from_opts.source_uri, &from_opts.ssl)
        .await
        .map_err(|e| {
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
    let boolean_rows: Vec<MysqlRow> = conn
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
    let set_rows: Vec<MysqlRow> = conn
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
    let json_columns_by_table =
        crate::from_trigger::json_columns::get_json_columns(conn, database_name).await?;
    for (table_name, columns) in json_columns_by_table {
        schema_info.entry(table_name).or_default().json_columns = columns;
    }

    Ok(schema_info)
}

/// Get list of user tables (excluding system tables and our audit table)
async fn get_user_tables(conn: &mut mysql_async::Conn, database: &str) -> Result<Vec<String>> {
    let rows: Vec<MysqlRow> = conn
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
            let mut after: Option<Vec<Value>> = None;
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
            after: Option<Vec<Value>>,
            batch_size: usize,
            config: &'a RowConversionConfig,
            exhausted: bool,
        }

        #[async_trait]
        impl RowChunkSource for MysqlKeysetChunks<'_> {
            async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>> {
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

    warn!("Table '{table_name}' has no primary key; streaming via LIMIT/OFFSET chunks");

    if opts.sync_opts.dry_run {
        let mut total_processed = 0usize;
        let mut offset = 0usize;
        loop {
            let rows: Vec<MysqlRow> = conn
                .query(format!(
                    "SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to query table {table_name}: {e}"))?;
            if rows.is_empty() {
                break;
            }
            let n = rows.len();
            debug!("Dry-run: Would insert {n} records into {table_name}");
            total_processed += n;
            offset += n;
            if n < batch_size {
                break;
            }
        }
        return Ok(total_processed);
    }

    struct MysqlOffsetChunks<'a> {
        conn: &'a mut mysql_async::Conn,
        table_name: &'a str,
        batch_size: usize,
        offset: usize,
        config: &'a RowConversionConfig,
        exhausted: bool,
    }

    #[async_trait]
    impl RowChunkSource for MysqlOffsetChunks<'_> {
        async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>> {
            if self.exhausted {
                return Ok(None);
            }
            let rows: Vec<MysqlRow> = self
                .conn
                .query(format!(
                    "SELECT * FROM `{}` LIMIT {} OFFSET {}",
                    self.table_name, self.batch_size, self.offset
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to query table {}: {e}", self.table_name))?;
            if rows.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }
            let mut batch = Vec::with_capacity(rows.len());
            for (i, row) in rows.iter().enumerate() {
                let row_index = (self.offset + i) as u64;
                let typed_values = row_to_typed_values_with_config(row, self.config)?;
                let values: HashMap<String, Value> = typed_values
                    .into_iter()
                    .map(|(k, tv)| (k, tv.value))
                    .collect();
                batch.push(Row::new(
                    self.table_name.to_string(),
                    row_index,
                    Value::Int64(row_index as i64),
                    values,
                ));
            }
            let n = batch.len();
            self.offset += n;
            if n < self.batch_size {
                self.exhausted = true;
            }
            Ok(Some(batch))
        }
    }

    let chunks = MysqlOffsetChunks {
        conn,
        table_name,
        batch_size,
        offset: 0,
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
    Ok(driver.sunk_count() as usize)
}
