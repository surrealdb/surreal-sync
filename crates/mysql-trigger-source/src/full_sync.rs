//! MySQL full sync implementation using TypedValue conversion path.
//!
//! This module uses the unified type conversion flow:
//! MySQL Row → TypedValue (mysql-types) → UniversalRow (sync-core) → SurrealDB (surreal crate)

use crate::{SourceOpts, SurrealOpts};
use anyhow::Result;
use checkpoint::{Checkpoint, SyncConfig, SyncManager, SyncPhase};
use mysql_async::{prelude::*, Pool, Row};
use mysql_types::{row_to_typed_values_with_config, JsonConversionConfig, RowConversionConfig};
use std::collections::HashMap;
use surreal_sync_surreal::write_universal_rows;
use sync_core::{TypedValue, UniversalRow, UniversalValue};
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
pub async fn run_full_sync(
    from_opts: &SourceOpts,
    to_opts: &SurrealOpts,
    sync_config: Option<SyncConfig>,
    surreal: &surreal_sync_surreal::Surreal<surreal_sync_surreal::SurrealEngine>,
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
    if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // Set up triggers and audit table FIRST to establish incremental sync infrastructure
        super::change_tracking::setup_mysql_change_tracking(&mut conn, &database_name).await?;
        info!("Set up MySQL triggers and audit table for incremental sync");

        // Get current sequence_id from the NOW-EXISTING audit table
        let checkpoint = super::checkpoint::get_current_checkpoint(&mut conn).await?;

        sync_manager
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

        let count = migrate_table(
            &mut conn,
            surreal,
            table_name,
            to_opts,
            &from_opts.mysql_boolean_paths,
            schema_info.get(table_name),
        )
        .await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // Get current checkpoint after migration
        let checkpoint = super::checkpoint::get_current_checkpoint(&mut conn).await?;

        sync_manager
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
    /// Columns that are SET type
    set_columns: Vec<String>,
    /// Primary key columns
    pk_columns: Vec<String>,
}

/// Collect schema information for all tables
async fn collect_schema_info(
    conn: &mut mysql_async::Conn,
) -> Result<HashMap<String, TableSchemaInfo>> {
    let query = "
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let rows: Vec<Row> = conn.query(query).await?;
    let mut tables: HashMap<String, TableSchemaInfo> = HashMap::new();

    for row in rows {
        let table_name: String = row
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Missing table name"))?;
        let column_name: String = row
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("Missing column name"))?;
        let data_type: String = row
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("Missing data type"))?;
        let column_type: String = row
            .get(3)
            .ok_or_else(|| anyhow::anyhow!("Missing column type"))?;

        let info = tables.entry(table_name).or_insert_with(|| TableSchemaInfo {
            boolean_columns: Vec::new(),
            set_columns: Vec::new(),
            pk_columns: Vec::new(),
        });

        // Detect boolean columns (TINYINT(1))
        if data_type.to_uppercase() == "TINYINT"
            && column_type.to_lowercase().starts_with("tinyint(1)")
        {
            info.boolean_columns.push(column_name.clone());
        }

        // Detect SET columns
        if data_type.to_uppercase() == "SET" {
            info.set_columns.push(column_name);
        }
    }

    // Collect primary key information for each table
    let pk_query = "
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
        AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let pk_rows: Vec<Row> = conn.query(pk_query).await?;

    for row in pk_rows {
        let table_name: String = row
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("Missing table name"))?;
        let column_name: String = row
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("Missing column name"))?;

        if let Some(info) = tables.get_mut(&table_name) {
            info.pk_columns.push(column_name);
        }
    }

    Ok(tables)
}

/// Get list of user tables from MySQL
async fn get_user_tables(conn: &mut mysql_async::Conn, database: &str) -> Result<Vec<String>> {
    let query = "
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = ?
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    ";

    let tables: Vec<String> = conn.exec(query, (database,)).await?;
    Ok(tables)
}

/// Migrate a single table from MySQL to SurrealDB
async fn migrate_table(
    conn: &mut mysql_async::Conn,
    surreal: &surreal_sync_surreal::Surreal<surreal_sync_surreal::SurrealEngine>,
    table_name: &str,
    to_opts: &SurrealOpts,
    boolean_paths: &Option<Vec<String>>,
    schema_info: Option<&TableSchemaInfo>,
) -> Result<usize> {
    // Query all data from the table
    let query = format!("SELECT * FROM {table_name}");
    let rows: Vec<Row> = conn.query(query).await?;

    if rows.is_empty() {
        return Ok(0);
    }

    // Build row conversion config with boolean, SET, and JSON configuration
    let row_config = build_row_conversion_config(boolean_paths, table_name, schema_info);

    // Get primary key columns
    let pk_columns: Vec<String> = schema_info
        .map(|s| s.pk_columns.clone())
        .unwrap_or_else(|| vec!["id".to_string()]);

    let mut batch = Vec::new();
    let mut total_processed = 0;

    for (row_index, row) in rows.iter().enumerate() {
        let record =
            convert_row_to_record(row, &pk_columns, table_name, &row_config, row_index as u64)?;
        batch.push(record);

        // Process batch when it reaches the configured size
        if batch.len() >= to_opts.batch_size {
            let batch_size = batch.len();

            if !to_opts.dry_run {
                write_universal_rows_batch(surreal, table_name, &batch).await?;
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

        if !to_opts.dry_run {
            write_universal_rows_batch(surreal, table_name, &batch).await?;
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

/// Build row conversion config from boolean paths and schema
fn build_row_conversion_config(
    boolean_paths: &Option<Vec<String>>,
    table_name: &str,
    schema_info: Option<&TableSchemaInfo>,
) -> RowConversionConfig {
    let mut config = RowConversionConfig::default();

    // Add boolean columns from schema
    if let Some(info) = schema_info {
        config.boolean_columns = info.boolean_columns.clone();
        config.set_columns = info.set_columns.clone();
    }

    // Build JSON config for nested boolean paths
    let mut json_config = JsonConversionConfig::default();
    let mut has_json_config = false;

    // Add boolean paths for this table (for JSON fields)
    if let Some(paths) = boolean_paths {
        for path in paths {
            if let Some((table_col, json_path)) = path.split_once('=') {
                if let Some((table, _col)) = table_col.split_once('.') {
                    if table == table_name {
                        json_config.boolean_paths.push(json_path.to_string());
                        has_json_config = true;
                    }
                }
            }
        }
    }

    if has_json_config {
        config.json_config = Some(json_config);
    }

    config
}

/// Convert a MySQL row to an UniversalRow using the unified type conversion path
fn convert_row_to_record(
    row: &Row,
    pk_columns: &[String],
    table_name: &str,
    row_config: &RowConversionConfig,
    row_index: u64,
) -> Result<UniversalRow> {
    // Step 1: Convert MySQL Row → HashMap<String, TypedValue>
    let typed_values = row_to_typed_values_with_config(row, row_config)
        .map_err(|e| anyhow::anyhow!("MySQL conversion error: {e}"))?;

    // Step 2: Extract the ID from typed values
    let id = extract_record_id(&typed_values, pk_columns, table_name)?;

    // Step 3: Remove the ID column from data (it's used as record ID)
    let mut data_values = typed_values;
    if pk_columns.len() == 1 {
        data_values.remove(&pk_columns[0]);
    }

    // Step 4: Convert HashMap<String, TypedValue> → HashMap<String, UniversalValue>
    let universal_data: HashMap<String, UniversalValue> = data_values
        .into_iter()
        .map(|(k, tv)| (k, tv.value))
        .collect();

    Ok(UniversalRow::new(
        table_name.to_string(),
        row_index,
        id,
        universal_data,
    ))
}

/// Extract record ID from typed values based on primary key columns
fn extract_record_id(
    typed_values: &HashMap<String, TypedValue>,
    pk_columns: &[String],
    _table_name: &str,
) -> Result<UniversalValue> {
    if pk_columns.is_empty() || (pk_columns.len() == 1 && pk_columns[0] == "id") {
        // Single primary key named 'id'
        let id_value = typed_values
            .get("id")
            .ok_or_else(|| anyhow::anyhow!("MySQL record must have an 'id' field"))?;

        typed_value_to_id(id_value)
    } else if pk_columns.len() == 1 {
        // Single primary key with different name
        let pk_col = &pk_columns[0];
        let id_value = typed_values
            .get(pk_col)
            .ok_or_else(|| anyhow::anyhow!("Primary key column '{pk_col}' not found"))?;

        typed_value_to_id(id_value)
    } else {
        // Composite primary key - concatenate values
        let parts: Vec<String> = pk_columns
            .iter()
            .filter_map(|col| typed_values.get(col))
            .map(typed_value_to_string)
            .collect();

        let composite_id = parts.join("_");
        Ok(UniversalValue::Text(composite_id))
    }
}

/// Convert a TypedValue to a UniversalValue for use as an ID
fn typed_value_to_id(tv: &TypedValue) -> Result<UniversalValue> {
    match &tv.value {
        UniversalValue::Int32(i) => Ok(UniversalValue::Int64(*i as i64)),
        UniversalValue::Int64(i) => Ok(UniversalValue::Int64(*i)),
        UniversalValue::Text(s) => {
            // Try to parse as integer first for numeric IDs stored as strings
            if let Ok(n) = s.parse::<i64>() {
                Ok(UniversalValue::Int64(n))
            } else {
                Ok(UniversalValue::Text(s.clone()))
            }
        }
        // VarChar and Char are strict 1:1 type-value variants that map to string IDs
        UniversalValue::VarChar { value: s, .. } => {
            if let Ok(n) = s.parse::<i64>() {
                Ok(UniversalValue::Int64(n))
            } else {
                Ok(UniversalValue::Text(s.clone()))
            }
        }
        UniversalValue::Char { value: s, .. } => {
            if let Ok(n) = s.parse::<i64>() {
                Ok(UniversalValue::Int64(n))
            } else {
                Ok(UniversalValue::Text(s.clone()))
            }
        }
        UniversalValue::Uuid(u) => Ok(UniversalValue::Uuid(*u)),
        _ => Err(anyhow::anyhow!("Unsupported ID type: {:?}", tv.sync_type)),
    }
}

/// Convert a TypedValue to a string (for composite keys)
fn typed_value_to_string(tv: &TypedValue) -> String {
    match &tv.value {
        UniversalValue::Int32(i) => i.to_string(),
        UniversalValue::Int64(i) => i.to_string(),
        UniversalValue::Text(s) => s.clone(),
        UniversalValue::VarChar { value: s, .. } => s.clone(),
        UniversalValue::Char { value: s, .. } => s.clone(),
        UniversalValue::Uuid(u) => u.to_string(),
        UniversalValue::Bool(b) => b.to_string(),
        UniversalValue::Float64(f) => f.to_string(),
        UniversalValue::Float32(f) => f.to_string(),
        UniversalValue::Int8 { value: i, .. } => i.to_string(),
        UniversalValue::Int16(i) => i.to_string(),
        _ => "null".to_string(),
    }
}

/// Write a batch of records to SurrealDB using UniversalRow
async fn write_universal_rows_batch(
    surreal: &surreal_sync_surreal::Surreal<surreal_sync_surreal::SurrealEngine>,
    table_name: &str,
    batch: &[UniversalRow],
) -> Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    write_universal_rows(surreal, batch).await?;

    tracing::debug!(
        "Completed migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}
