//! PostgreSQL logical replication incremental sync implementation
//!
//! This module provides incremental synchronization from PostgreSQL to SurrealDB
//! using WAL-based logical replication with wal2json.

use crate::checkpoint::PostgreSQLLogicalCheckpoint;
use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use surrealdb::sql::{Number, Strand, Value};
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
        let surreal_value: Value = match val {
            crate::Value::Integer(v) => Value::Number(Number::Int(v.to_i64().unwrap())),
            crate::Value::Double(v) => Value::Number(Number::Float(*v)),
            crate::Value::Text(v) => Value::Strand(Strand::from(v.to_owned())),
            crate::Value::Boolean(v) => Value::Bool(*v),
            crate::Value::Null => Value::None,
            v => {
                anyhow::bail!("Unsupported value type for upsert {v:?}");
            }
        };

        query = query.bind((field.clone(), surreal_value));
    }

    let id: Value = match &row.primary_key {
        crate::Value::Integer(v) => Value::Number(Number::Int(v.to_i64().unwrap())),
        crate::Value::Text(v) => Value::Strand(Strand::from(v.to_owned())),
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
        crate::Value::Integer(v) => Value::Number(Number::Int(v.to_i64().unwrap())),
        crate::Value::Text(v) => Value::Strand(Strand::from(v.to_owned())),
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
