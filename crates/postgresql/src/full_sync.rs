//! PostgreSQL full sync utilities
//!
//! This module provides table migration functionality from PostgreSQL to SurrealDB,
//! including row conversion utilities.

use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use surreal2_types::RecordWithSurrealValues as Record;
use surrealdb::sql::{Array, Datetime, Number, Strand, Value};
use tokio_postgres::{Client, Row};
use tracing::{debug, warn};

/// SurrealDB connection options
#[derive(Clone, Debug)]
pub struct SurrealOpts {
    /// SurrealDB endpoint URL
    pub surreal_endpoint: String,
    /// SurrealDB username
    pub surreal_username: String,
    /// SurrealDB password
    pub surreal_password: String,
    /// Batch size for data migration
    pub batch_size: usize,
    /// Dry run mode - don't actually write data
    pub dry_run: bool,
}

/// Migrate a single table from PostgreSQL to SurrealDB
pub async fn migrate_table(
    client: &Client,
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    to_opts: &SurrealOpts,
) -> Result<usize> {
    // Get primary key column(s)
    let pk_columns = get_primary_key_columns(client, table_name).await?;

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

    let mut batch = Vec::new();
    let mut total_processed = 0;

    for row in &rows {
        let record = convert_row_to_record(table_name, row, &pk_columns)?;
        batch.push(record);

        // Process batch when it reaches the configured size
        if batch.len() >= to_opts.batch_size {
            let batch_size = batch.len();

            if !to_opts.dry_run {
                surreal2_sink::write_records(surreal, table_name, &batch).await?;
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
            surreal2_sink::write_records(surreal, table_name, &batch).await?;
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

fn convert_row_to_record(table: &str, row: &Row, pk_columns: &[String]) -> anyhow::Result<Record> {
    let (id, data) = convert_row_to_keys_and_surreal_values(row, pk_columns)?;
    let id = surrealdb::sql::Thing::from((table, id));

    Ok(Record::new(id, data))
}

/// Convert a PostgreSQL row to a map of surreal values
fn convert_row_to_keys_and_surreal_values(
    row: &Row,
    pk_columns: &[String],
) -> Result<(surrealdb::sql::Id, HashMap<String, Value>)> {
    let mut record = HashMap::new();

    // Generate ID from primary key columns
    let id = if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table has no primary key defined - primary key is required for sync"
        ));
    } else if pk_columns.len() == 1 {
        // Single primary key column - extract its value
        let pk_col = &pk_columns[0];
        let id = if let Ok(id) = row.try_get::<_, i64>(pk_col.as_str()) {
            surrealdb::sql::Id::from(id)
        } else if let Ok(id) = row.try_get::<_, i32>(pk_col.as_str()) {
            surrealdb::sql::Id::from(id)
        } else if let Ok(id) = row.try_get::<_, String>(pk_col.as_str()) {
            surrealdb::sql::Id::from(id)
        } else if let Ok(id) = row.try_get::<_, uuid::Uuid>(pk_col.as_str()) {
            let uuid = surrealdb::sql::Uuid::from(id);
            surrealdb::sql::Id::from(uuid)
        } else {
            return Err(anyhow::anyhow!(
                "Failed to extract primary key value from column '{pk_col}' - unsupported data type",
            ));
        };
        id
    } else {
        let mut vs = Vec::new();
        for col in pk_columns {
            let v = if let Ok(val) = row.try_get::<_, String>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else if let Ok(val) = row.try_get::<_, uuid::Uuid>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else if let Ok(val) = row.try_get::<_, i64>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else if let Ok(val) = row.try_get::<_, i32>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else {
                return Err(anyhow::anyhow!(
                    "Failed to extract composite primary key value from column '{col}' - unsupported data type",
                ));
            };
            vs.push(v);
        }
        let ary = surrealdb::sql::Array::from(vs);
        surrealdb::sql::Id::from(ary)
    };

    // Convert all columns
    for (i, column) in row.columns().iter().enumerate() {
        let column_name = column.name();

        if pk_columns.contains(&column_name.to_string()) {
            // Skip primary key columns as they are already used in the ID
            continue;
        }

        let value = convert_postgres_value(row, i)?;
        record.insert(column_name.to_string(), value);
    }

    Ok((id, record))
}

/// Convert a PostgreSQL value to a surrealdb::sql::Value
fn convert_postgres_value(row: &Row, index: usize) -> Result<Value> {
    use tokio_postgres::types::Type;

    let column = &row.columns()[index];
    let pg_type = column.type_();

    match *pg_type {
        Type::BOOL => match row.try_get::<_, Option<bool>>(index)? {
            Some(b) => Ok(Value::Bool(b)),
            None => Ok(Value::Null),
        },
        Type::INT2 => match row.try_get::<_, Option<i16>>(index)? {
            Some(i) => Ok(Value::Number(Number::Int(i as i64))),
            None => Ok(Value::Null),
        },
        Type::INT4 => match row.try_get::<_, Option<i32>>(index)? {
            Some(i) => Ok(Value::Number(Number::Int(i as i64))),
            None => Ok(Value::Null),
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(index)? {
            Some(i) => Ok(Value::Number(Number::Int(i))),
            None => Ok(Value::Null),
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(index)? {
            Some(f) => Ok(Value::Number(Number::Float(f as f64))),
            None => Ok(Value::Null),
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(index)? {
            Some(f) => Ok(Value::Number(Number::Float(f))),
            None => Ok(Value::Null),
        },
        Type::NUMERIC => {
            // PostgreSQL NUMERIC type - maps to surrealdb::sql::Number
            match row.try_get::<_, Option<Decimal>>(index) {
                Ok(Some(decimal)) => {
                    // Convert rust_decimal::Decimal to surrealdb::sql::Number (preserves precision)
                    let decimal_str = decimal.to_string();
                    match Number::try_from(decimal_str.as_str()) {
                        Ok(surreal_num) => Ok(Value::Number(surreal_num)),
                        Err(e) => {
                            warn!(
                                "Failed to convert NUMERIC decimal '{}' to SurrealDB Number: {:?}",
                                decimal, e
                            );
                            Err(anyhow::anyhow!("NUMERIC conversion failed: {e:?}"))
                        }
                    }
                }
                Ok(None) => Ok(Value::Null),
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
                        Ok(Value::Duration(surrealdb::sql::Duration::from(duration)))
                    } else {
                        Ok(Value::Strand(Strand::from(s)))
                    }
                }
                None => Ok(Value::Null),
            }
        }
        Type::TIMESTAMP => match row.try_get::<_, Option<NaiveDateTime>>(index)? {
            Some(ts) => {
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc);
                Ok(Value::Datetime(Datetime::from(dt)))
            }
            None => Ok(Value::Null),
        },
        Type::TIMESTAMPTZ => match row.try_get::<_, Option<DateTime<Utc>>>(index)? {
            Some(dt) => Ok(Value::Datetime(Datetime::from(dt))),
            None => Ok(Value::Null),
        },
        Type::DATE => match row.try_get::<_, Option<NaiveDate>>(index)? {
            Some(date) => {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                Ok(Value::Datetime(Datetime::from(dt)))
            }
            None => Ok(Value::Null),
        },
        Type::TIME => match row.try_get::<_, Option<NaiveTime>>(index)? {
            Some(time) => Ok(Value::Strand(Strand::from(time.to_string()))),
            None => Ok(Value::Null),
        },
        Type::JSON | Type::JSONB => match row.try_get::<_, Option<serde_json::Value>>(index)? {
            Some(json) => Ok(json_to_value(json)),
            None => Ok(Value::Null),
        },
        Type::UUID => match row.try_get::<_, Option<uuid::Uuid>>(index)? {
            Some(uuid) => Ok(Value::Strand(Strand::from(uuid.to_string()))),
            None => Ok(Value::Null),
        },
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(index)? {
            Some(bytes) => Ok(Value::Strand(Strand::from(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                bytes,
            )))),
            None => Ok(Value::Null),
        },
        Type::TEXT_ARRAY => match row.try_get::<_, Option<Vec<String>>>(index)? {
            Some(arr) => {
                let vals: Vec<Value> = arr
                    .into_iter()
                    .map(|s| Value::Strand(Strand::from(s)))
                    .collect();
                Ok(Value::Array(Array::from(vals)))
            }
            None => Ok(Value::Null),
        },
        Type::INT4_ARRAY => match row.try_get::<_, Option<Vec<i32>>>(index)? {
            Some(arr) => {
                let vals: Vec<Value> = arr
                    .into_iter()
                    .map(|v| Value::Number(Number::Int(v as i64)))
                    .collect();
                Ok(Value::Array(Array::from(vals)))
            }
            None => Ok(Value::Null),
        },
        Type::INT8_ARRAY => match row.try_get::<_, Option<Vec<i64>>>(index)? {
            Some(arr) => {
                let vals: Vec<Value> = arr
                    .into_iter()
                    .map(|v| Value::Number(Number::Int(v)))
                    .collect();
                Ok(Value::Array(Array::from(vals)))
            }
            None => Ok(Value::Null),
        },
        Type::POINT => match row.try_get::<_, Option<geo_types::Point<f64>>>(index)? {
            Some(p) => Ok(Value::Geometry(surrealdb::sql::Geometry::Point(p))),
            None => Ok(Value::Null),
        },
        _ => {
            // For unknown types, try to get as string
            if let Ok(val) = row.try_get::<_, String>(index) {
                Ok(Value::Strand(Strand::from(val)))
            } else {
                Err(anyhow::anyhow!("Unsupported PostgreSQL type: {pg_type:?}",))
            }
        }
    }
}

/// Convert JSON value to surrealdb::sql::Value
fn json_to_value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(Number::Int(i))
            } else if let Some(f) = n.as_f64() {
                Value::Number(Number::Float(f))
            } else {
                Value::Strand(Strand::from(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Value::Strand(Strand::from(s)),
        serde_json::Value::Array(arr) => {
            let vals: Vec<Value> = arr.into_iter().map(json_to_value).collect();
            Value::Array(Array::from(vals))
        }
        serde_json::Value::Object(map) => {
            let mut obj = std::collections::BTreeMap::new();
            for (k, v) in map {
                obj.insert(k, json_to_value(v));
            }
            Value::Object(surrealdb::sql::Object::from(obj))
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
