//! MySQL full sync implementation using TypedValue conversion path.
//!
//! This module uses the unified type conversion flow:
//! MySQL Row → TypedValue (mysql-types) → UniversalRow (sync-core) → SurrealDB (surreal sink)

use crate::{SourceOpts, SyncOpts};
use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use mysql_async::{prelude::*, Pool, Row};
use mysql_types::{row_to_typed_values_with_config, RowConversionConfig};
use std::collections::HashMap;
use surreal_sink::SurrealSink;
use sync_core::{UniversalRow, UniversalType, UniversalValue};
use tracing::{debug, info};

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

/// Main entry point for MySQL to SurrealDB migration with checkpoint support
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `from_opts` - Source database options
/// * `sync_opts` - Sync configuration options
/// * `sync_manager` - Optional sync manager for checkpoint emission
pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    info!("Starting MySQL migration to SurrealDB");

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
    let schema_info = collect_schema_info(&mut conn).await?;
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
            sync_opts,
            &boolean_paths,
            schema_info.get(table_name),
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
#[derive(Debug, Clone)]
struct TableSchemaInfo {
    /// Columns that should be treated as boolean (TINYINT(1))
    boolean_columns: Vec<String>,
    /// Columns that are SET type (comma-separated values -> array)
    set_columns: Vec<String>,
}

/// Collect schema information for all tables in the database
async fn collect_schema_info(
    conn: &mut mysql_async::Conn,
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
            .or_insert_with(|| TableSchemaInfo {
                boolean_columns: Vec::new(),
                set_columns: Vec::new(),
            })
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
            .or_insert_with(|| TableSchemaInfo {
                boolean_columns: Vec::new(),
                set_columns: Vec::new(),
            })
            .set_columns
            .push(column_name);
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

/// Get primary key column names for a table (returns empty vec if no PK or composite PK)
async fn get_primary_key_columns(
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

/// Migrate a single table from MySQL to SurrealDB
async fn migrate_table<S: SurrealSink>(
    conn: &mut mysql_async::Conn,
    surreal: &S,
    table_name: &str,
    sync_opts: &SyncOpts,
    _json_path_overrides: &[String],
    schema_info: Option<&TableSchemaInfo>,
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

    if !set_columns.is_empty() {
        debug!("Table {} has SET columns: {:?}", table_name, set_columns);
    }

    // Query all data from table
    let rows: Vec<Row> = conn
        .query(format!("SELECT * FROM {table_name}"))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query table {table_name}: {e}"))?;

    let count = rows.len();
    debug!("Retrieved {} rows from table {}", count, table_name);

    let mut batch = Vec::new();
    let mut total_processed = 0;

    // Build row conversion config with boolean and SET columns
    let config = RowConversionConfig {
        boolean_columns: boolean_columns.clone(),
        set_columns: set_columns.clone(),
        json_config: None,
    };

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
        let id = extract_primary_key_value(&values, &pk_columns)?;

        // Build non-PK fields map
        let fields: HashMap<String, UniversalValue> = values
            .into_iter()
            .filter(|(k, _)| !pk_columns.contains(k))
            .collect();

        // Create UniversalRow
        let universal_row = UniversalRow::new(table_name.to_string(), row_index as u64, id, fields);
        batch.push(universal_row);

        // Process batch when it reaches the configured size
        if batch.len() >= sync_opts.batch_size {
            let batch_size = batch.len();

            if !sync_opts.dry_run {
                surreal.write_universal_rows(&batch).await?;
            } else {
                debug!(
                    "Dry-run: Would insert {} records into {}",
                    batch_size, table_name
                );
            }
            batch.clear();
            total_processed += batch_size;
        }
    }

    // Process remaining records
    if !batch.is_empty() {
        let batch_size = batch.len();

        if !sync_opts.dry_run {
            surreal.write_universal_rows(&batch).await?;
        } else {
            debug!(
                "Dry-run: Would insert {} records into {}",
                batch_size, table_name
            );
        }
        total_processed += batch_size;
    }

    Ok(total_processed)
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
