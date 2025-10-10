use crate::surreal::{SurrealTableSchema, SurrealType};
use crate::{SourceOpts, SurrealOpts, SurrealValue};
use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use mysql_async::{prelude::*, Pool, Row, Value};
use std::collections::HashMap;
use tracing::{debug, info};

/// Main entry point for MySQL to SurrealDB migration with checkpoint support
pub async fn run_full_sync(
    from_opts: &SourceOpts,
    to_opts: &SurrealOpts,
    sync_config: Option<crate::sync::SyncConfig>,
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
) -> Result<()> {
    info!("Starting MySQL migration to SurrealDB");

    // Create connection pool
    let pool = Pool::from_url(&from_opts.source_uri)?;
    let mut conn = pool.get_conn().await?;

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
        let sync_manager = crate::sync::SyncManager::new(config.clone());

        // Set up triggers and audit table FIRST to establish incremental sync infrastructure
        super::change_tracking::setup_mysql_change_tracking(&mut conn, &database_name).await?;
        info!("Set up MySQL triggers and audit table for incremental sync");

        // Get current sequence_id from the NOW-EXISTING audit table
        let checkpoint = super::checkpoint::get_current_checkpoint(&mut conn).await?;

        sync_manager
            .emit_checkpoint(&checkpoint, crate::sync::SyncPhase::FullSyncStart)
            .await?;

        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_string()
        );
    }

    // Collect database schema for type-aware conversion
    let database_schema = super::schema::collect_mysql_schema(&mut conn, &database_name).await?;
    info!("Collected MySQL database schema for type-aware conversion");

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
            database_schema.tables.get(table_name).unwrap(),
        )
        .await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = crate::sync::SyncManager::new(config.clone());

        // Get current checkpoint after migration
        let checkpoint = super::checkpoint::get_current_checkpoint(&mut conn).await?;

        sync_manager
            .emit_checkpoint(&checkpoint, crate::sync::SyncPhase::FullSyncEnd)
            .await?;

        info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_string()
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
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    to_opts: &SurrealOpts,
    boolean_paths: &Option<Vec<String>>,
    table_schema: &SurrealTableSchema,
) -> Result<usize> {
    // Get primary key column(s)
    let pk_columns = super::schema::get_primary_key_columns(conn, table_name).await?;

    // Query all data from the table
    let query = format!("SELECT * FROM {table_name}");
    let rows: Vec<Row> = conn.query(query).await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let mut batch = Vec::new();
    let mut total_processed = 0;

    for row in rows {
        let record =
            convert_row_to_record(row, &pk_columns, table_name, boolean_paths, table_schema)?;
        batch.push(record);

        // Process batch when it reaches the configured size
        if batch.len() >= to_opts.batch_size {
            let batch_size = batch.len();

            if !to_opts.dry_run {
                crate::write_records(surreal, table_name, &batch).await?;
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
            crate::write_records(surreal, table_name, &batch).await?;
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

// Convert a MySQL row to a surreal record, ensuring 'id' is a Thing
fn convert_row_to_record(
    row: Row,
    pk_columns: &[String],
    table_name: &str,
    boolean_paths: &Option<Vec<String>>,
    table_schema: &SurrealTableSchema,
) -> Result<crate::Record> {
    let mut data = _convert_row_to_keys_and_surreal_values(
        row,
        pk_columns,
        table_name,
        boolean_paths,
        table_schema,
    )?;

    // Extract original MySQL ID to ensure deterministic SurrealDB record IDs
    // NEVER use random UUIDs as fallback - this breaks UPSERT functionality!
    let v = match data.remove("id") {
        Some(v) => v,
        None => {
            return Err(anyhow::anyhow!(
                "MySQL record must have an 'id' field - deterministic IDs required"
            ))
        }
    };

    let id = match v {
        SurrealValue::String(s) => surrealdb::sql::Id::from(s),
        SurrealValue::Int(i) => surrealdb::sql::Id::from(i),
        _ => {
            anyhow::bail!("MySQL record ID field has unsupported type - deterministic IDs required")
        }
    };

    // Create proper SurrealDB Thing for the record
    let id = surrealdb::sql::Thing::from((table_name.to_string(), id));

    Ok(crate::Record { id, data })
}

/// Convert a MySQL row to a map of surreal values
fn _convert_row_to_keys_and_surreal_values(
    row: Row,
    pk_columns: &[String],
    table_name: &str,
    boolean_paths: &Option<Vec<String>>,
    table_schema: &SurrealTableSchema,
) -> Result<HashMap<String, SurrealValue>> {
    let mut record = HashMap::new();

    // Parse JSON boolean paths for this table
    let json_boolean_paths = parse_json_boolean_paths(boolean_paths, table_name);

    // Build json_set_paths from table schema
    let json_set_paths = build_json_set_paths(table_schema);

    // Get column information
    let columns = row.columns();

    // Generate ID from primary key columns
    let id = if pk_columns.is_empty() || (pk_columns.len() == 1 && pk_columns[0] == "id") {
        // Try to find 'id' column index
        if let Some(id_index) = columns.iter().position(|col| col.name_str() == "id") {
            match row.as_ref(id_index) {
                Some(Value::Int(i)) => i.to_string(),
                Some(Value::UInt(u)) => u.to_string(),
                Some(Value::Bytes(b)) => String::from_utf8_lossy(b).to_string(),
                _ => panic!(
                    "MySQL record ID field has unsupported type - deterministic IDs required"
                ),
            }
        } else {
            // Always require an id column for deterministic record IDs
            panic!("MySQL table must have an 'id' column - deterministic IDs required")
        }
    } else {
        // Composite primary key - concatenate values
        pk_columns
            .iter()
            .filter_map(|col_name| {
                columns
                    .iter()
                    .position(|col| col.name_str() == col_name.as_str())
                    .and_then(|idx| row.as_ref(idx))
                    .map(|val| match val {
                        Value::Int(i) => i.to_string(),
                        Value::UInt(u) => u.to_string(),
                        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                        _ => "null".to_string(),
                    })
            })
            .collect::<Vec<_>>()
            .join("_")
    };

    record.insert("id".to_string(), SurrealValue::String(id));

    // Convert all columns
    for (index, column) in columns.iter().enumerate() {
        let column_name = column.name_str();

        // Skip if it's the id column and we already added it
        if column_name == "id" && pk_columns.len() == 1 && pk_columns[0] == "id" {
            continue;
        }

        let value = convert_mysql_value(
            &row,
            index,
            &column_name,
            json_boolean_paths.get(&column_name.to_string()),
            &json_set_paths,
            table_schema.columns.get(&column_name.to_string()).unwrap(),
        )?;
        record.insert(column_name.to_string(), value);
    }

    Ok(record)
}

/// Parse boolean paths for JSON fields in the given table
/// Format: "table.column=json.path.to.bool"
fn parse_json_boolean_paths(
    boolean_paths: &Option<Vec<String>>,
    table_name: &str,
) -> HashMap<String, Vec<String>> {
    let mut column_paths: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(paths) = boolean_paths {
        for path in paths {
            if let Some((table_col, json_path)) = path.split_once('=') {
                // Format: table.column=json.path
                if let Some((table, col)) = table_col.split_once('.') {
                    if table == table_name {
                        // Add this JSON path for the column
                        column_paths
                            .entry(col.to_string())
                            .or_default()
                            .push(json_path.to_string());
                    }
                }
            }
        }
    }

    column_paths
}

/// Build json_set_paths from table schema - identifies columns with SET type
fn build_json_set_paths(table_schema: &SurrealTableSchema) -> Vec<String> {
    let mut set_paths = Vec::new();

    for (column_name, column_type) in &table_schema.columns {
        // Check if this column is a SET type (represented as Array of String)
        if matches!(column_type, SurrealType::Array(inner)
            if matches!(inner.as_ref(), SurrealType::String))
        {
            set_paths.push(column_name.clone());
        }
    }

    set_paths
}

/// Convert a MySQL value to a SurrealValue
fn convert_mysql_value(
    row: &Row,
    index: usize,
    _column_name: &str,
    json_boolean_paths: Option<&Vec<String>>,
    json_set_paths: &[String],
    column_type: &SurrealType,
) -> Result<SurrealValue> {
    let columns = row.columns();
    let column = &columns[index];
    let _column_name = column.name_str();

    // Get the raw value
    let raw_value = row
        .as_ref(index)
        .ok_or_else(|| anyhow::anyhow!("Failed to get value at index {index}"))?;

    if std::env::var("MYSQL_DEBUG").is_ok() {
        eprintln!(
            "DEBUG: Column '{}' type: {:?}, raw value type: {:?}",
            column.name_str(),
            column.column_type(),
            std::mem::discriminant(raw_value)
        );
    }

    // Add detailed value debugging
    if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
        match raw_value {
            Value::Bytes(bytes) => {
                eprintln!(
                    "DEBUG: Bytes content for {}: {:?} (len={})",
                    column.name_str(),
                    bytes,
                    bytes.len()
                );
                if bytes.len() <= 8 {
                    eprintln!(
                        "DEBUG: Bytes as base64: {}",
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
                    );
                }
            }
            _ => eprintln!(
                "DEBUG: Non-bytes value for {}: {:?}",
                column.name_str(),
                raw_value
            ),
        }
    }

    let result = match raw_value {
        Value::NULL => Ok(SurrealValue::Null),
        Value::Bytes(bytes) => {
            use mysql_async::consts::ColumnType;

            // Check column type to determine how to interpret bytes
            let col_type = column.column_type();

            match col_type {
                // Character types
                col_type if col_type.is_character_type() => {
                    // Text types - convert to string
                    let s = String::from_utf8_lossy(bytes).to_string();

                    // Check if this is a SET column from schema
                    if let SurrealType::Array(_) = column_type {
                        // SET type - split comma-separated values into array
                        if s.is_empty() {
                            Ok(SurrealValue::Array(Vec::new()))
                        } else {
                            let values: Vec<SurrealValue> = s
                                .split(',')
                                .map(|v| SurrealValue::String(v.to_string()))
                                .collect();
                            Ok(SurrealValue::Array(values))
                        }
                    } else {
                        Ok(SurrealValue::String(s))
                    }
                }
                ColumnType::MYSQL_TYPE_JSON => {
                    // JSON type - parse and convert to SurrealValue
                    let s = String::from_utf8_lossy(bytes).to_string();
                    // Parse JSON and convert to SurrealValue recursively
                    match serde_json::from_str::<serde_json::Value>(&s) {
                        Ok(json_value) => {
                            // MySQL can store boolean as 0/1 in JSON, so we need to handle this
                            convert_mysql_json_to_surreal_value(
                                json_value,
                                "",
                                json_boolean_paths.unwrap_or(&Vec::new()),
                                json_set_paths,
                            )
                        }
                        Err(e) => {
                            eprintln!("Failed to parse JSON column {}: {}", column.name_str(), e);
                            // Fall back to string if JSON parsing fails
                            Ok(SurrealValue::String(s))
                        }
                    }
                }
                ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    // DECIMAL type - MySQL returns as string bytes
                    let s = String::from_utf8_lossy(bytes).to_string();
                    eprintln!(
                        "Converting DECIMAL value '{}' from bytes for column {}",
                        s,
                        column.name_str()
                    );
                    s.parse::<f64>()
                        .map(SurrealValue::Float)
                        .or_else(|_| Ok(SurrealValue::String(s)))
                }
                ColumnType::MYSQL_TYPE_DOUBLE | ColumnType::MYSQL_TYPE_FLOAT => {
                    // FLOAT/DOUBLE types - MySQL returns as string bytes
                    let s = String::from_utf8_lossy(bytes).to_string();
                    eprintln!(
                        "Converting FLOAT/DOUBLE value '{}' from bytes for column {}",
                        s,
                        column.name_str()
                    );
                    s.parse::<f64>()
                        .map(SurrealValue::Float)
                        .or_else(|_| Ok(SurrealValue::String(s)))
                }
                ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_DATETIME => {
                    // TIMESTAMP/DATETIME types - MySQL might return as string bytes
                    let s = String::from_utf8_lossy(bytes).to_string();
                    eprintln!(
                        "Converting TIMESTAMP value '{}' from bytes for column {}",
                        s,
                        column.name_str()
                    );

                    // Parse MySQL datetime format: "YYYY-MM-DD HH:MM:SS"
                    use chrono::NaiveDateTime;
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        .map(|ndt| {
                            SurrealValue::DateTime(DateTime::<Utc>::from_naive_utc_and_offset(
                                ndt, Utc,
                            ))
                        })
                        .or_else(|_| Ok(SurrealValue::String(s)))
                }
                ColumnType::MYSQL_TYPE_TINY => {
                    // TINYINT - check if it's TINYINT(1) which is boolean
                    let s = String::from_utf8_lossy(bytes).to_string();
                    // Check the column flags to determine if this is TINYINT(1)
                    // In MySQL, TINYINT(1) is conventionally used as boolean
                    // We can check column length or just convert 0/1 to boolean
                    match s.parse::<i64>() {
                        Ok(0) => Ok(SurrealValue::Bool(false)),
                        Ok(1) => Ok(SurrealValue::Bool(true)),
                        Ok(n) => Ok(SurrealValue::Int(n)), // Other values stay as int
                        Err(_) => Ok(SurrealValue::String(s)),
                    }
                }
                ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_LONGLONG => {
                    // Other INTEGER types returned as ASCII string bytes
                    let s = String::from_utf8_lossy(bytes).to_string();
                    s.parse::<i64>()
                        .map(SurrealValue::Int)
                        .or_else(|_| Ok(SurrealValue::String(s)))
                }
                _ => {
                    // Unsupported column type - return error instead of silent base64 conversion
                    Err(anyhow::anyhow!(
                        "Unsupported MySQL column type '{:?}' for column '{}'. \
                        Please implement proper conversion for this type.",
                        col_type,
                        column.name_str()
                    ))
                }
            }
        }
        Value::Int(i) => {
            // For TINYINT values, MySQL may return them as Value::Int
            // Convert 0/1 to boolean for consistency
            if *i == 0 {
                Ok(SurrealValue::Bool(false))
            } else if *i == 1 {
                Ok(SurrealValue::Bool(true))
            } else {
                Ok(SurrealValue::Int(*i))
            }
        }
        Value::UInt(u) => {
            // Convert unsigned to signed, capping at i64::MAX
            if *u > i64::MAX as u64 {
                Ok(SurrealValue::String(u.to_string()))
            } else {
                Ok(SurrealValue::Int(*u as i64))
            }
        }
        Value::Float(f) => Ok(SurrealValue::Float(*f as f64)),
        Value::Double(d) => Ok(SurrealValue::Float(*d)),
        Value::Date(year, month, day, hour, minute, second, microsecond) => {
            // Convert MySQL date/datetime to chrono DateTime
            let date = NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
            let time = date
                .and_hms_micro_opt(*hour as u32, *minute as u32, *second as u32, *microsecond)
                .ok_or_else(|| anyhow::anyhow!("Invalid datetime"))?;
            let dt = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
            Ok(SurrealValue::DateTime(dt))
        }
        Value::Time(negative, days, hours, minutes, seconds, microseconds) => {
            // Convert MySQL time to string since SurrealDB doesn't have a pure time type
            let sign = if *negative { "-" } else { "" };
            let total_hours = *days * 24 + (*hours as u32);
            let time_str =
                format!("{sign}{total_hours}:{minutes:02}:{seconds:02}.{microseconds:06}");
            Ok(SurrealValue::String(time_str))
        }
    };

    // Log the final conversion result
    if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
        match &result {
            Ok(value) => eprintln!("DEBUG: Converted {} to: {:?}", column.name_str(), value),
            Err(e) => eprintln!("DEBUG: Conversion failed for {}: {}", column.name_str(), e),
        }
    }

    result
}

/// Convert MySQL JSON to SurrealValue, handling MySQL-specific quirks
pub fn convert_mysql_json_to_surreal_value(
    value: serde_json::Value,
    current_path: &str,
    json_boolean_paths: &[String],
    json_set_paths: &[String],
) -> Result<SurrealValue> {
    match value {
        serde_json::Value::Null => Ok(SurrealValue::Null),
        serde_json::Value::Bool(b) => Ok(SurrealValue::Bool(b)),
        serde_json::Value::Number(n) => {
            // Check if this path should be treated as boolean
            let should_convert_to_bool = json_boolean_paths.iter().any(|path| path == current_path);

            if let Some(i) = n.as_i64() {
                if should_convert_to_bool && (i == 0 || i == 1) {
                    // Convert 0/1 to boolean for specified paths
                    Ok(SurrealValue::Bool(i == 1))
                } else {
                    Ok(SurrealValue::Int(i))
                }
            } else if let Some(f) = n.as_f64() {
                Ok(SurrealValue::Float(f))
            } else {
                Ok(SurrealValue::String(n.to_string()))
            }
        }
        serde_json::Value::String(s) => {
            // Check if this path should be treated as a SET column
            let should_convert_to_array = json_set_paths.iter().any(|path| path == current_path);

            if should_convert_to_array {
                // Convert comma-separated SET values to array
                if s.is_empty() {
                    Ok(SurrealValue::Array(Vec::new()))
                } else {
                    let values: Vec<SurrealValue> = s
                        .split(',')
                        .map(|v| SurrealValue::String(v.to_string()))
                        .collect();
                    Ok(SurrealValue::Array(values))
                }
            } else {
                Ok(SurrealValue::String(s))
            }
        }
        serde_json::Value::Array(arr) => {
            let mut vs = Vec::new();
            for (idx, item) in arr.into_iter().enumerate() {
                // Arrays don't typically have boolean conversion, but pass path for completeness
                let item_path = format!("{current_path}[{idx}]");
                vs.push(convert_mysql_json_to_surreal_value(
                    item,
                    &item_path,
                    json_boolean_paths,
                    json_set_paths,
                )?);
            }
            Ok(SurrealValue::Array(vs))
        }
        serde_json::Value::Object(map) => {
            let mut kvs = std::collections::HashMap::new();
            for (key, val) in map {
                // Build the nested path for this field
                let nested_path = if current_path.is_empty() {
                    key.clone()
                } else {
                    format!("{current_path}.{key}")
                };
                kvs.insert(
                    key,
                    convert_mysql_json_to_surreal_value(
                        val,
                        &nested_path,
                        json_boolean_paths,
                        json_set_paths,
                    )?,
                );
            }
            Ok(SurrealValue::Object(kvs))
        }
    }
}
