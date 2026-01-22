//! PostgreSQL logical replication incremental sync implementation
//!
//! This module provides incremental synchronization from PostgreSQL to SurrealDB
//! using WAL-based logical replication with wal2json.

use crate::checkpoint::PostgreSQLLogicalCheckpoint;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;
use surrealdb::sql::{Array, Bytes, Datetime, Duration, Number, Object, Strand, Value};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::full_sync::SourceOpts;

/// Run incremental sync from PostgreSQL to SurrealDB
///
/// This function:
/// 1. Connects to PostgreSQL
/// 2. Starts logical replication from the given checkpoint position
/// 3. Streams and applies changes to SurrealDB
/// 4. Stops when timeout is reached or target checkpoint is hit
///
/// # Arguments
/// * `from_opts` - PostgreSQL source options
/// * `to_namespace` - Target SurrealDB namespace
/// * `to_database` - Target SurrealDB database
/// * `to_opts` - SurrealDB connection options
/// * `from_checkpoint` - Starting LSN position
/// * `to_checkpoint` - Optional stopping LSN position
/// * `timeout_secs` - Maximum runtime in seconds
pub async fn run_incremental_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: surreal_sync_postgresql::SurrealOpts,
    from_checkpoint: PostgreSQLLogicalCheckpoint,
    to_checkpoint: Option<PostgreSQLLogicalCheckpoint>,
    timeout_secs: u64,
) -> Result<()> {
    info!(
        "Starting PostgreSQL logical replication incremental sync from LSN: {}",
        from_checkpoint.lsn
    );

    if let Some(ref target) = to_checkpoint {
        info!("Target LSN: {}", target.lsn);
    }
    info!("Timeout: {} seconds", timeout_secs);

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {e}");
        }
    });

    // Connect to SurrealDB
    let surreal_endpoint = to_opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let surreal = surrealdb::engine::any::connect(surreal_endpoint).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;

    surreal.use_ns(&to_namespace).use_db(&to_database).await?;

    // Create logical replication client
    let pg_client = crate::Client::new(client, from_opts.tables.clone());

    // Ensure slot exists (it should have been created during full sync)
    pg_client.create_slot(&from_opts.slot_name).await?;

    // Start replication
    let slot = pg_client
        .start_replication(Some(&from_opts.slot_name))
        .await?;

    info!(
        "Logical replication started on slot: {}",
        from_opts.slot_name
    );

    // Advance to starting position if needed
    if !from_checkpoint.lsn.is_empty() && from_checkpoint.lsn != "0/0" {
        info!("Advancing slot to starting LSN: {}", from_checkpoint.lsn);
        slot.advance(&from_checkpoint.lsn).await?;
    }

    // Stream changes with timeout
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let mut total_changes = 0;

    loop {
        // Check timeout
        if std::time::Instant::now() >= deadline {
            info!("Timeout reached, stopping incremental sync");
            break;
        }

        // Peek at available changes
        match slot.peek().await {
            Ok((changes, nextlsn)) => {
                if changes.is_empty() {
                    // Check if we've reached target checkpoint
                    if let Some(ref target) = to_checkpoint {
                        if compare_lsn(&nextlsn, &target.lsn) >= 0 {
                            info!("Reached target LSN {} (current: {})", target.lsn, nextlsn);
                            break;
                        }
                    }

                    // No changes, wait before retrying
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }

                // Process all changes in the batch
                for change in &changes {
                    match change {
                        crate::Action::Insert(row) => {
                            debug!(
                                "INSERT: table={}, primary_key={:?}",
                                row.table, row.primary_key
                            );
                            upsert(&surreal, row).await?;
                            total_changes += 1;
                        }
                        crate::Action::Update(row) => {
                            debug!(
                                "UPDATE: table={}, primary_key={:?}",
                                row.table, row.primary_key
                            );
                            upsert(&surreal, row).await?;
                            total_changes += 1;
                        }
                        crate::Action::Delete(row) => {
                            debug!(
                                "DELETE: table={}, primary_key={:?}",
                                row.table, row.primary_key
                            );
                            delete(&surreal, row).await?;
                            total_changes += 1;
                        }
                        crate::Action::Begin { .. } | crate::Action::Commit { .. } => {
                            // Skip transaction markers
                        }
                    }
                }

                // Advance slot after processing
                slot.advance(&nextlsn).await?;

                // Check if we've reached target checkpoint
                if let Some(ref target) = to_checkpoint {
                    if compare_lsn(&nextlsn, &target.lsn) >= 0 {
                        info!("Reached target LSN {} (current: {})", target.lsn, nextlsn);
                        break;
                    }
                }
            }
            Err(e) => {
                // Error handling: Retry on transient errors
                //
                // pg_logical_slot_peek_changes() only returns committed transactions,
                // so long-running transactions will NOT cause errors - they simply
                // remain invisible until they commit.
                //
                // Errors here are typically:
                // - Transient connection issues
                // - Network timeouts
                // - WAL parsing errors (bugs in wal2json or our code)
                // - Data corruption
                //
                // We retry after 1 second to handle transient issues. The overall
                // timeout (from the outer loop) prevents infinite retries.
                warn!("Error peeking changes: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    info!(
        "PostgreSQL logical replication incremental sync completed: {} changes applied",
        total_changes
    );
    Ok(())
}

/// Compare two LSN strings
/// Returns: -1 if lsn1 < lsn2, 0 if equal, 1 if lsn1 > lsn2
fn compare_lsn(lsn1: &str, lsn2: &str) -> i32 {
    // LSN format: "segment/offset" (e.g., "0/1949850")
    let parse_lsn = |lsn: &str| -> Option<(u64, u64)> {
        let parts: Vec<&str> = lsn.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let segment = u64::from_str_radix(parts[0], 16).ok()?;
        let offset = u64::from_str_radix(parts[1], 16).ok()?;
        Some((segment, offset))
    };

    match (parse_lsn(lsn1), parse_lsn(lsn2)) {
        (Some((s1, o1)), Some((s2, o2))) => {
            if s1 < s2 || (s1 == s2 && o1 < o2) {
                -1
            } else if s1 == s2 && o1 == o2 {
                0
            } else {
                1
            }
        }
        _ => {
            // Fallback to string comparison
            lsn1.cmp(lsn2) as i32
        }
    }
}

/// Convert PostgreSQL Value enum to SurrealDB Value
///
/// This function handles all PostgreSQL type variants and converts them
/// to appropriate SurrealDB Value types with proper type preservation.
fn convert_postgres_value_to_surreal(value: &crate::Value) -> Result<Value> {
    match value {
        // Numeric types
        crate::Value::SmallInt(v) => Ok(Value::Number(Number::Int(*v as i64))),
        crate::Value::Integer(v) => Ok(Value::Number(Number::Int(*v as i64))),
        crate::Value::BigInt(v) => Ok(Value::Number(Number::Int(*v))),
        crate::Value::Real(v) => Ok(Value::Number(Number::Float(*v as f64))),
        crate::Value::Double(v) => Ok(Value::Number(Number::Float(*v))),
        crate::Value::Numeric(s) => {
            // Parse high-precision decimal
            match Decimal::from_str(s) {
                Ok(dec) => Ok(Value::Number(Number::Decimal(dec))),
                Err(_) => Ok(Value::Strand(Strand::from(s.clone()))),
            }
        }

        // String types
        crate::Value::Text(s) | crate::Value::Varchar(s) | crate::Value::Char(s) => {
            Ok(Value::Strand(Strand::from(s.clone())))
        }

        // Boolean
        crate::Value::Boolean(b) => Ok(Value::Bool(*b)),

        // Binary
        crate::Value::Bytea(bytes) => Ok(Value::Bytes(Bytes::from(bytes.clone()))),

        // UUID
        crate::Value::Uuid(uuid) => match uuid.to_uuid() {
            Ok(u) => Ok(Value::Uuid(surrealdb::sql::Uuid::from(u))),
            Err(_) => Ok(Value::Strand(Strand::from(uuid.0.clone()))),
        },

        // JSON types - use recursive helper
        crate::Value::Json(j) | crate::Value::Jsonb(j) => Ok(json_to_surreal(j)),

        // Date/Time types
        crate::Value::Timestamp(ts) => match ts.to_chrono_datetime_utc() {
            Ok(chrono_dt) => {
                // Convert chrono DateTime to SurrealDB Datetime
                let datetime_str = chrono_dt.to_rfc3339();
                match Datetime::from_str(&datetime_str) {
                    Ok(dt) => Ok(Value::Datetime(dt)),
                    Err(_) => Ok(Value::Strand(Strand::from(ts.0.clone()))),
                }
            }
            Err(_) => Ok(Value::Strand(Strand::from(ts.0.clone()))),
        },
        crate::Value::TimestampTz(ts) => match ts.to_chrono_datetime_utc() {
            Ok(chrono_dt) => {
                // Convert chrono DateTime to SurrealDB Datetime
                let datetime_str = chrono_dt.to_rfc3339();
                match Datetime::from_str(&datetime_str) {
                    Ok(dt) => Ok(Value::Datetime(dt)),
                    Err(_) => Ok(Value::Strand(Strand::from(ts.0.clone()))),
                }
            }
            Err(_) => Ok(Value::Strand(Strand::from(ts.0.clone()))),
        },
        crate::Value::Date(d) => Ok(Value::Strand(Strand::from(d.0.clone()))),
        crate::Value::Time(t) => Ok(Value::Strand(Strand::from(t.0.clone()))),
        crate::Value::TimeTz(t) => Ok(Value::Strand(Strand::from(t.0.clone()))),
        crate::Value::Interval(i) => {
            // Try to parse as duration, fallback to string
            match try_parse_interval_to_duration(&i.0) {
                Ok(dur) => Ok(Value::Duration(dur)),
                Err(_) => Ok(Value::Strand(Strand::from(i.0.clone()))),
            }
        }

        // Arrays - recursive conversion
        crate::Value::Array(arr) => {
            let converted: Result<Vec<Value>> = arr
                .iter()
                .map(convert_postgres_value_to_surreal)
                .collect();
            Ok(Value::Array(Array::from(converted?)))
        }

        // Null
        crate::Value::Null => Ok(Value::None),
    }
}

/// Helper to convert serde_json::Value to SurrealDB Value (for JSON/JSONB)
///
/// Recursively converts JSON objects and arrays to SurrealDB native types.
fn json_to_surreal(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::None,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(Number::Int(i))
            } else if let Some(f) = n.as_f64() {
                Value::Number(Number::Float(f))
            } else {
                Value::None
            }
        }
        serde_json::Value::String(s) => Value::Strand(Strand::from(s.clone())),
        serde_json::Value::Array(arr) => Value::Array(Array::from(
            arr.iter().map(json_to_surreal).collect::<Vec<_>>(),
        )),
        serde_json::Value::Object(map) => {
            let mut obj = BTreeMap::new();
            for (k, v) in map {
                obj.insert(k.clone(), json_to_surreal(v));
            }
            Value::Object(Object::from(obj))
        }
    }
}

/// Helper to parse PostgreSQL interval to Duration
///
/// Uses the existing Interval::to_duration() method from the Interval wrapper
/// and converts std::time::Duration to surrealdb::sql::Duration.
fn try_parse_interval_to_duration(interval_str: &str) -> Result<Duration> {
    let interval = crate::value::Interval(interval_str.to_string());
    let std_duration = interval.to_duration().map_err(|e| anyhow::anyhow!(e))?;

    // Convert std::time::Duration to surrealdb::sql::Duration
    let nanos = std_duration.as_nanos();
    Ok(Duration::from_nanos(nanos as u64))
}

async fn upsert(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    row: &crate::Row,
) -> Result<()> {
    let mut surrealql = String::from("UPSERT type::thing($tb, $id) SET ");

    for (i, (col, _)) in row.columns.iter().enumerate() {
        if i > 0 {
            surrealql.push_str(", ");
        }
        surrealql.push_str(&format!("{col} = ${col}{i}"));
    }

    let mut query = surreal.query(surrealql);

    for (i, (col, val)) in row.columns.iter().enumerate() {
        let field = format!("{col}{i}");
        let surreal_value = convert_postgres_value_to_surreal(val)
            .with_context(|| format!("Failed to convert column '{col}' value"))?;

        query = query.bind((field.clone(), surreal_value));
    }

    let id: Value = match &row.primary_key {
        crate::Value::SmallInt(v) => Value::Number(Number::Int(*v as i64)),
        crate::Value::Integer(v) => Value::Number(Number::Int(*v as i64)),
        crate::Value::BigInt(v) => Value::Number(Number::Int(*v)),
        crate::Value::Real(v) => Value::Number(Number::Float(*v as f64)),
        crate::Value::Double(v) => Value::Number(Number::Float(*v)),
        crate::Value::Numeric(s) => match Decimal::from_str(s) {
            Ok(dec) => Value::Number(Number::Decimal(dec)),
            Err(_) => Value::Strand(Strand::from(s.clone())),
        },
        crate::Value::Text(s) | crate::Value::Varchar(s) | crate::Value::Char(s) => {
            Value::Strand(Strand::from(s.clone()))
        }
        crate::Value::Uuid(u) => match u.to_uuid() {
            Ok(uuid) => Value::Uuid(surrealdb::sql::Uuid::from(uuid)),
            Err(_) => Value::Strand(Strand::from(u.0.clone())),
        },
        crate::Value::Array(arr) => {
            // Composite primary key - convert array
            let converted: Result<Vec<Value>> = arr
                .iter()
                .map(convert_postgres_value_to_surreal)
                .collect();
            Value::Array(Array::from(converted?))
        }
        _ => anyhow::bail!(
            "Unsupported primary key type for upsert: {:?}",
            row.primary_key
        ),
    };

    let _ = query
        .bind(("tb", row.table.clone()))
        .bind(("id", id))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to upsert data into {}: {}", &row.table, e))?;

    Ok(())
}

async fn delete(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    row: &crate::Row,
) -> Result<()> {
    let id: Value = match &row.primary_key {
        crate::Value::SmallInt(v) => Value::Number(Number::Int(*v as i64)),
        crate::Value::Integer(v) => Value::Number(Number::Int(*v as i64)),
        crate::Value::BigInt(v) => Value::Number(Number::Int(*v)),
        crate::Value::Real(v) => Value::Number(Number::Float(*v as f64)),
        crate::Value::Double(v) => Value::Number(Number::Float(*v)),
        crate::Value::Numeric(s) => match Decimal::from_str(s) {
            Ok(dec) => Value::Number(Number::Decimal(dec)),
            Err(_) => Value::Strand(Strand::from(s.clone())),
        },
        crate::Value::Text(s) | crate::Value::Varchar(s) | crate::Value::Char(s) => {
            Value::Strand(Strand::from(s.clone()))
        }
        crate::Value::Uuid(u) => match u.to_uuid() {
            Ok(uuid) => Value::Uuid(surrealdb::sql::Uuid::from(uuid)),
            Err(_) => Value::Strand(Strand::from(u.0.clone())),
        },
        crate::Value::Array(arr) => {
            // Composite primary key - convert array
            let converted: Result<Vec<Value>> = arr
                .iter()
                .map(convert_postgres_value_to_surreal)
                .collect();
            Value::Array(Array::from(converted?))
        }
        _ => anyhow::bail!(
            "Unsupported primary key type for delete: {:?}",
            row.primary_key
        ),
    };

    surreal
        .query("DELETE type::thing($tb, $id)")
        .bind(("tb", row.table.clone()))
        .bind(("id", id))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete data from {}: {}", row.table, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_lsn_equal() {
        assert_eq!(compare_lsn("0/1949850", "0/1949850"), 0);
    }

    #[test]
    fn test_compare_lsn_less_than() {
        assert_eq!(compare_lsn("0/100", "0/200"), -1);
        assert_eq!(compare_lsn("0/FF", "1/0"), -1);
    }

    #[test]
    fn test_compare_lsn_greater_than() {
        assert_eq!(compare_lsn("0/200", "0/100"), 1);
        assert_eq!(compare_lsn("1/0", "0/FF"), 1);
    }
}
