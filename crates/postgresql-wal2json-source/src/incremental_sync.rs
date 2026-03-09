//! PostgreSQL logical replication incremental sync implementation
//!
//! This module provides incremental synchronization from PostgreSQL to SurrealDB
//! using WAL-based logical replication with wal2json.

use crate::checkpoint::PostgreSQLLogicalCheckpoint;
use anyhow::Result;
use checkpoint::{CheckpointID, CheckpointStore, Surreal2Store};
use surreal_sink::SurrealSink;
use sync_core::{
    classify_table, DatabaseSchema, TableKind, UniversalChange, UniversalChangeOp,
    UniversalRelationChange,
};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::full_sync::SourceOpts;

/// Read t1 checkpoint from SurrealDB
///
/// Reads the full_sync_start checkpoint from the specified table.
#[allow(dead_code)]
pub async fn read_t1_checkpoint_from_surrealdb(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
) -> Result<PostgreSQLLogicalCheckpoint> {
    let store = Surreal2Store::new(surreal.clone(), table_name.to_string());

    let id = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };

    let stored = store.read_checkpoint(&id).await?.ok_or_else(|| {
        anyhow::anyhow!("No t1 checkpoint found in SurrealDB table '{table_name}'")
    })?;

    let checkpoint: PostgreSQLLogicalCheckpoint = serde_json::from_str(&stored.checkpoint_data)?;
    Ok(checkpoint)
}

/// Run incremental sync from PostgreSQL to SurrealDB
///
/// This function:
/// 1. Connects to PostgreSQL
/// 2. Starts logical replication from the checkpoint position
/// 3. Streams and applies changes to SurrealDB
/// 4. Stops when deadline is reached or target checkpoint is hit
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `from_opts` - PostgreSQL source options
/// * `from_checkpoint` - Starting LSN position
/// * `deadline` - When to stop syncing
/// * `to_checkpoint` - Optional stopping LSN position
pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: PostgreSQLLogicalCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    to_checkpoint: Option<PostgreSQLLogicalCheckpoint>,
) -> Result<()> {
    info!(
        "Starting PostgreSQL logical replication incremental sync from LSN: {}",
        from_checkpoint.lsn
    );

    if let Some(ref target) = to_checkpoint {
        info!("Target LSN: {}", target.lsn);
    }
    let duration_until_deadline = deadline.signed_duration_since(chrono::Utc::now());
    info!(
        "Deadline in {} seconds",
        duration_until_deadline.num_seconds()
    );

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {e}");
        }
    });

    // Collect schema with FK info for record link and relation conversion
    let db_schema =
        surreal_sync_postgresql::schema::collect_database_schema_with_fks(&client).await?;
    let relation_table_overrides = &from_opts.relation_tables;

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

    // Stream changes with deadline
    let mut total_changes = 0;

    loop {
        // Check deadline
        if chrono::Utc::now() >= deadline {
            info!("Deadline reached, stopping incremental sync");
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
                    let (row, op) = match change {
                        crate::Action::Insert(row) => {
                            debug!("INSERT: table={}, primary_key={:?}", row.table, row.primary_key);
                            (row, UniversalChangeOp::Create)
                        }
                        crate::Action::Update(row) => {
                            debug!("UPDATE: table={}, primary_key={:?}", row.table, row.primary_key);
                            (row, UniversalChangeOp::Update)
                        }
                        crate::Action::Delete(row) => {
                            debug!("DELETE: table={}, primary_key={:?}", row.table, row.primary_key);
                            (row, UniversalChangeOp::Delete)
                        }
                        crate::Action::Begin { .. } | crate::Action::Commit { .. } => {
                            continue;
                        }
                    };

                    apply_change_with_fk_transform(
                        surreal,
                        row,
                        op,
                        &db_schema,
                        relation_table_overrides,
                    )
                    .await?;
                    total_changes += 1;
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

/// Convert a Row to UniversalChange
fn row_to_universal_change(row: &crate::Row, op: UniversalChangeOp) -> UniversalChange {
    let data = if op == UniversalChangeOp::Delete {
        None
    } else {
        Some(row.columns.clone())
    };
    UniversalChange::new(op, row.table.clone(), row.primary_key.clone(), data)
}

/// Apply a change with FK-to-Thing transformation and relation-table routing.
async fn apply_change_with_fk_transform<S: SurrealSink>(
    surreal: &S,
    row: &crate::Row,
    op: UniversalChangeOp,
    db_schema: &DatabaseSchema,
    relation_table_overrides: &[String],
) -> Result<()> {
    let table_def = db_schema.get_table(&row.table);
    let table_kind = table_def.map(|td| classify_table(td, relation_table_overrides));

    match table_kind {
        Some(TableKind::Relation { ref in_fk, ref out_fk }) => {
            let data = if op == UniversalChangeOp::Delete {
                std::collections::HashMap::new()
            } else {
                row.columns.clone()
            };
            let relation = surreal_sync_postgresql::fk_transform::build_relation_from_change(
                &row.table,
                row.primary_key.clone(),
                data,
                in_fk,
                out_fk,
            );
            let rel_change = UniversalRelationChange::new(op, relation);
            surreal.apply_universal_relation_change(&rel_change).await?;
        }
        _ => {
            let mut change = row_to_universal_change(row, op);
            if let (Some(td), Some(ref mut data)) = (table_def, change.data.as_mut()) {
                surreal_sync_postgresql::fk_transform::transform_fk_values(data, td);
            }
            surreal.apply_universal_change(&change).await?;
        }
    }
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
