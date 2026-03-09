//! PostgreSQL full sync utilities
//!
//! This module provides table migration functionality from PostgreSQL to SurrealDB,
//! including row conversion utilities.

use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use surreal_sink::SurrealSink;
use sync_core::{
    classify_table, DatabaseSchema, GeometryType, TableKind, UniversalRow, UniversalType,
    UniversalValue,
};
use tokio_postgres::{Client, Row};
use tracing::{debug, info, warn};

use crate::fk_transform;

/// Sync options (non-connection related)
#[derive(Clone, Debug)]
pub struct SyncOpts {
    /// Batch size for data migration
    pub batch_size: usize,
    /// Dry run mode - don't actually write data
    pub dry_run: bool,
}

/// Migrate a single table from PostgreSQL to SurrealDB.
///
/// When `schema` is provided and the table has foreign keys, FK column values
/// are automatically converted to SurrealDB record links.  If the table is
/// classified as a relation (join) table, rows are synced as graph edges
/// via `RELATE` instead of regular records.
pub async fn migrate_table<S: SurrealSink>(
    client: &Client,
    surreal: &S,
    table_name: &str,
    sync_opts: &SyncOpts,
    schema: Option<&DatabaseSchema>,
    relation_table_overrides: &[String],
) -> Result<usize> {
    // Get primary key column(s)
    let pk_columns = get_primary_key_columns(client, table_name).await?;

    // Determine table kind (entity vs relation)
    let table_kind = schema
        .and_then(|s| s.get_table(table_name))
        .map(|td| classify_table(td, relation_table_overrides));

    if let Some(TableKind::Relation { .. }) = &table_kind {
        info!("Table '{table_name}' classified as relation table, will sync as RELATE edges");
    }

    // Query all data from the table
    let query = format!("SELECT * FROM {table_name}");
    log::info!("Full sync querying table {table_name} with: {query}");
    let rows = client.query(&query, &[]).await?;
    log::info!(
        "Full sync found {} rows in table {}",
        rows.len(),
        table_name
    );

    if rows.is_empty() {
        log::info!("Table {table_name} is empty, skipping");
        return Ok(0);
    }

    let table_def = schema.and_then(|s| s.get_table(table_name));

    let mut row_batch = Vec::new();
    let mut rel_batch = Vec::new();
    let mut total_processed = 0;

    for (row_index, row) in rows.iter().enumerate() {
        match &table_kind {
            Some(TableKind::Relation { in_fk, out_fk }) => {
                // For relation tables, include ALL columns (including PK) so FK
                // extraction can find them. The relation ID is generated from the row index.
                let all_fields = convert_all_columns_to_universal_values(row)?;
                let rel_id = UniversalValue::Int64(row_index as i64);
                let relation = fk_transform::build_relation_from_row(
                    table_name,
                    rel_id,
                    all_fields,
                    in_fk,
                    out_fk,
                );
                rel_batch.push(relation);
            }
            _ => {
                let mut record =
                    convert_row_to_universal_row(table_name, row, &pk_columns, row_index as u64)?;
                if let Some(td) = table_def {
                    fk_transform::transform_fk_values(&mut record.fields, td);
                }
                row_batch.push(record);
            }
        }

        let current_batch_len = row_batch.len() + rel_batch.len();
        if current_batch_len >= sync_opts.batch_size {
            if !sync_opts.dry_run {
                if !row_batch.is_empty() {
                    surreal.write_universal_rows(&row_batch).await?;
                }
                if !rel_batch.is_empty() {
                    surreal.write_universal_relations(&rel_batch).await?;
                }
            } else {
                debug!(
                    "Dry-run: Would insert {} records/relations into {}",
                    current_batch_len, table_name
                );
            }
            total_processed += current_batch_len;
            row_batch.clear();
            rel_batch.clear();
        }
    }

    // Process remaining
    let remaining = row_batch.len() + rel_batch.len();
    if remaining > 0 {
        if !sync_opts.dry_run {
            if !row_batch.is_empty() {
                surreal.write_universal_rows(&row_batch).await?;
            }
            if !rel_batch.is_empty() {
                surreal.write_universal_relations(&rel_batch).await?;
            }
        } else {
            debug!(
                "Dry-run: Would insert {} records/relations into {}",
                remaining, table_name
            );
        }
        total_processed += remaining;
    }

    Ok(total_processed)
}

/// Get primary key columns for a table
async fn get_primary_key_columns(client: &Client, table_name: &str) -> Result<Vec<String>> {
    let query = format!(
        "
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table_name}'::regclass
        AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
    "
    );

    let rows = client.query(&query, &[]).await?;

    if rows.is_empty() {
        Err(anyhow::anyhow!(
            "Table '{table_name}' has no primary key defined - primary key is required for sync operations",
        ))
    } else {
        Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
    }
}

fn convert_row_to_universal_row(
    table: &str,
    row: &Row,
    pk_columns: &[String],
    row_index: u64,
) -> anyhow::Result<UniversalRow> {
    let (id, data) = convert_row_to_keys_and_universal_values(row, pk_columns)?;
    Ok(UniversalRow::new(table.to_string(), row_index, id, data))
}

/// Convert a PostgreSQL row to a map of universal values
fn convert_row_to_keys_and_universal_values(
    row: &Row,
    pk_columns: &[String],
) -> Result<(UniversalValue, HashMap<String, UniversalValue>)> {
    let mut record = HashMap::new();

    // Generate ID from primary key columns
    let id = if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table has no primary key defined - primary key is required for sync"
        ));
    } else if pk_columns.len() == 1 {
        // Single primary key column - extract its value
        let pk_col = &pk_columns[0];
        if let Ok(id) = row.try_get::<_, i64>(pk_col.as_str()) {
            UniversalValue::Int64(id)
        } else if let Ok(id) = row.try_get::<_, i32>(pk_col.as_str()) {
            UniversalValue::Int64(id as i64)
        } else if let Ok(id) = row.try_get::<_, String>(pk_col.as_str()) {
            UniversalValue::Text(id)
        } else if let Ok(id) = row.try_get::<_, uuid::Uuid>(pk_col.as_str()) {
            UniversalValue::Uuid(id)
        } else {
            return Err(anyhow::anyhow!(
                "Failed to extract primary key value from column '{pk_col}' - unsupported data type",
            ));
        }
    } else {
        let mut vs = Vec::new();
        for col in pk_columns {
            let v = if let Ok(val) = row.try_get::<_, String>(col.as_str()) {
                UniversalValue::Text(val)
            } else if let Ok(val) = row.try_get::<_, uuid::Uuid>(col.as_str()) {
                UniversalValue::Uuid(val)
            } else if let Ok(val) = row.try_get::<_, i64>(col.as_str()) {
                UniversalValue::Int64(val)
            } else if let Ok(val) = row.try_get::<_, i32>(col.as_str()) {
                UniversalValue::Int64(val as i64)
            } else {
                return Err(anyhow::anyhow!(
                    "Failed to extract composite primary key value from column '{col}' - unsupported data type",
                ));
            };
            vs.push(v);
        }
        UniversalValue::Array {
            elements: vs,
            element_type: Box::new(UniversalType::Text),
        }
    };

    // Convert all columns
    for (i, column) in row.columns().iter().enumerate() {
        let column_name = column.name();

        if pk_columns.contains(&column_name.to_string()) {
            // Skip primary key columns as they are already used in the ID
            continue;
        }

        let value = convert_postgres_value_to_universal(row, i)?;
        record.insert(column_name.to_string(), value);
    }

    Ok((id, record))
}

/// Convert all columns in a PostgreSQL row to UniversalValues (including PK columns).
/// Used for relation tables where FK columns may overlap with PK columns.
fn convert_all_columns_to_universal_values(
    row: &Row,
) -> Result<HashMap<String, UniversalValue>> {
    let mut record = HashMap::new();
    for (i, column) in row.columns().iter().enumerate() {
        let value = convert_postgres_value_to_universal(row, i)?;
        record.insert(column.name().to_string(), value);
    }
    Ok(record)
}

/// Convert a PostgreSQL value to an UniversalValue
fn convert_postgres_value_to_universal(row: &Row, index: usize) -> Result<UniversalValue> {
    use tokio_postgres::types::Type;

    let column = &row.columns()[index];
    let pg_type = column.type_();

    match *pg_type {
        Type::BOOL => match row.try_get::<_, Option<bool>>(index)? {
            Some(b) => Ok(UniversalValue::Bool(b)),
            None => Ok(UniversalValue::Null),
        },
        Type::INT2 => match row.try_get::<_, Option<i16>>(index)? {
            Some(i) => Ok(UniversalValue::Int16(i)),
            None => Ok(UniversalValue::Null),
        },
        Type::INT4 => match row.try_get::<_, Option<i32>>(index)? {
            Some(i) => Ok(UniversalValue::Int32(i)),
            None => Ok(UniversalValue::Null),
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(index)? {
            Some(i) => Ok(UniversalValue::Int64(i)),
            None => Ok(UniversalValue::Null),
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(index)? {
            Some(f) => Ok(UniversalValue::Float32(f)),
            None => Ok(UniversalValue::Null),
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(index)? {
            Some(f) => Ok(UniversalValue::Float64(f)),
            None => Ok(UniversalValue::Null),
        },
        Type::NUMERIC => {
            // PostgreSQL NUMERIC type - convert to Decimal
            match row.try_get::<_, Option<Decimal>>(index) {
                Ok(Some(decimal)) => {
                    // Get precision and scale from the decimal
                    let scale = decimal.scale() as u8;
                    let precision = 38; // Use max precision as default
                    Ok(UniversalValue::Decimal {
                        value: decimal.to_string(),
                        precision,
                        scale,
                    })
                }
                Ok(None) => Ok(UniversalValue::Null),
                Err(e) => {
                    warn!(
                        "Failed to get PostgreSQL NUMERIC as rust_decimal::Decimal: {}",
                        e
                    );
                    Err(anyhow::anyhow!("NUMERIC type conversion failed: {e}"))
                }
            }
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            match row.try_get::<_, Option<String>>(index)? {
                Some(s) => {
                    // Auto-detect ISO 8601 duration strings (PTxxxS format) and convert to Duration
                    if let Some(duration) = try_parse_iso8601_duration(&s) {
                        Ok(UniversalValue::Duration(duration))
                    } else {
                        Ok(UniversalValue::Text(s))
                    }
                }
                None => Ok(UniversalValue::Null),
            }
        }
        Type::TIMESTAMP => match row.try_get::<_, Option<NaiveDateTime>>(index)? {
            Some(ts) => {
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc);
                Ok(UniversalValue::LocalDateTime(dt))
            }
            None => Ok(UniversalValue::Null),
        },
        Type::TIMESTAMPTZ => match row.try_get::<_, Option<DateTime<Utc>>>(index)? {
            Some(dt) => Ok(UniversalValue::ZonedDateTime(dt)),
            None => Ok(UniversalValue::Null),
        },
        Type::DATE => match row.try_get::<_, Option<NaiveDate>>(index)? {
            Some(date) => {
                // Convert NaiveDate to DateTime<Utc> at midnight
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                Ok(UniversalValue::Date(dt))
            }
            None => Ok(UniversalValue::Null),
        },
        Type::TIME => match row.try_get::<_, Option<NaiveTime>>(index)? {
            Some(time) => {
                // Convert NaiveTime to DateTime<Utc> using epoch date as placeholder
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let dt = epoch.and_time(time);
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                Ok(UniversalValue::Time(dt))
            }
            None => Ok(UniversalValue::Null),
        },
        Type::JSON | Type::JSONB => match row.try_get::<_, Option<serde_json::Value>>(index)? {
            Some(json) => Ok(json_to_universal_value(json)),
            None => Ok(UniversalValue::Null),
        },
        Type::UUID => match row.try_get::<_, Option<uuid::Uuid>>(index)? {
            Some(uuid) => Ok(UniversalValue::Uuid(uuid)),
            None => Ok(UniversalValue::Null),
        },
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(index)? {
            Some(bytes) => Ok(UniversalValue::Bytes(bytes)),
            None => Ok(UniversalValue::Null),
        },
        Type::TEXT_ARRAY => match row.try_get::<_, Option<Vec<String>>>(index)? {
            Some(arr) => {
                let vals: Vec<UniversalValue> = arr.into_iter().map(UniversalValue::Text).collect();
                Ok(UniversalValue::Array {
                    elements: vals,
                    element_type: Box::new(UniversalType::Text),
                })
            }
            None => Ok(UniversalValue::Null),
        },
        Type::INT4_ARRAY => match row.try_get::<_, Option<Vec<i32>>>(index)? {
            Some(arr) => {
                let vals: Vec<UniversalValue> =
                    arr.into_iter().map(UniversalValue::Int32).collect();
                Ok(UniversalValue::Array {
                    elements: vals,
                    element_type: Box::new(UniversalType::Int32),
                })
            }
            None => Ok(UniversalValue::Null),
        },
        Type::INT8_ARRAY => match row.try_get::<_, Option<Vec<i64>>>(index)? {
            Some(arr) => {
                let vals: Vec<UniversalValue> =
                    arr.into_iter().map(UniversalValue::Int64).collect();
                Ok(UniversalValue::Array {
                    elements: vals,
                    element_type: Box::new(UniversalType::Int64),
                })
            }
            None => Ok(UniversalValue::Null),
        },
        Type::POINT => match row.try_get::<_, Option<geo_types::Point<f64>>>(index)? {
            Some(p) => {
                let geojson = serde_json::json!({
                    "type": "Point",
                    "coordinates": [p.x(), p.y()]
                });
                Ok(UniversalValue::geometry_geojson(
                    geojson,
                    GeometryType::Point,
                ))
            }
            None => Ok(UniversalValue::Null),
        },
        _ => {
            // For unknown types, try to get as string
            if let Ok(val) = row.try_get::<_, String>(index) {
                Ok(UniversalValue::Text(val))
            } else {
                Err(anyhow::anyhow!("Unsupported PostgreSQL type: {pg_type:?}",))
            }
        }
    }
}

/// Convert JSON value to UniversalValue
fn json_to_universal_value(value: serde_json::Value) -> UniversalValue {
    match value {
        serde_json::Value::Null => UniversalValue::Null,
        serde_json::Value::Bool(b) => UniversalValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                UniversalValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => UniversalValue::Text(s),
        serde_json::Value::Array(arr) => {
            let vals: Vec<UniversalValue> = arr.into_iter().map(json_to_universal_value).collect();
            UniversalValue::Array {
                elements: vals,
                element_type: Box::new(UniversalType::Json),
            }
        }
        serde_json::Value::Object(map) => {
            let obj: HashMap<String, UniversalValue> = map
                .into_iter()
                .map(|(k, v)| (k, json_to_universal_value(v)))
                .collect();
            UniversalValue::Object(obj)
        }
    }
}

/// Try to parse an ISO 8601 duration string (e.g., "PT181S" or "PT181.000000000S").
/// Returns Some(std::time::Duration) if the string matches the expected format.
fn try_parse_iso8601_duration(s: &str) -> Option<std::time::Duration> {
    let trimmed = s.trim();
    // Only accept "PTxS" or "PTx.xxxxxxxxxS" format
    if let Some(secs_str) = trimmed.strip_prefix("PT").and_then(|s| s.strip_suffix('S')) {
        if let Some(dot_pos) = secs_str.find('.') {
            // Has fractional seconds
            let secs: u64 = secs_str[..dot_pos].parse().ok()?;
            let nanos_str = &secs_str[dot_pos + 1..];
            let nanos: u32 = nanos_str.parse().ok()?;
            Some(std::time::Duration::new(secs, nanos))
        } else {
            let secs: u64 = secs_str.parse().ok()?;
            Some(std::time::Duration::from_secs(secs))
        }
    } else {
        None
    }
}
