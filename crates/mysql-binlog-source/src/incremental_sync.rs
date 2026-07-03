//! MySQL binlog incremental sync (apply then commit).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use binlog_protocol::{CdcChange, EventBody, TableMapEvent};
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::{DateTime, Utc};
use surreal_sink::SurrealSink;
use tracing::{debug, info};

use crate::change::cdc_change_to_universal;
use crate::checkpoint::BinlogCheckpoint;
use crate::client::{
    connect_binlog_client, new_mysql_pool, resolve_database, start_binlog_from_checkpoint,
    use_database,
};
use crate::schema::{collect_mysql_database_schema, get_table_column_names_ordinal};
use crate::SourceOpts;

#[derive(Clone, Debug)]
pub enum IncrementalRunMode {
    Batch {
        deadline: Option<DateTime<Utc>>,
        until: Option<BinlogCheckpoint>,
    },
    Follow {
        deadline: Option<DateTime<Utc>>,
    },
}

#[derive(Clone, Debug)]
pub struct IncrementalSyncOptions {
    pub run_mode: IncrementalRunMode,
    pub checkpoint_interval: Duration,
    /// Cooperative cancellation signal. When cancelled, the sync loop flushes a
    /// final resumable checkpoint and returns cleanly (SIGINT/SIGTERM in the CLI;
    /// tests can trigger it directly). Defaults to a never-cancelled token.
    pub cancel: tokio_util::sync::CancellationToken,
}

impl IncrementalSyncOptions {
    pub fn batch(deadline: Option<DateTime<Utc>>, until: Option<BinlogCheckpoint>) -> Self {
        Self {
            run_mode: IncrementalRunMode::Batch { deadline, until },
            checkpoint_interval: Duration::from_secs(10),
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }

    pub fn follow(deadline: Option<DateTime<Utc>>) -> Self {
        Self {
            run_mode: IncrementalRunMode::Follow { deadline },
            checkpoint_interval: Duration::from_secs(10),
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// Attach a cancellation token so SIGINT/SIGTERM (or a test) can stop the
    /// stream gracefully.
    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
}

pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: BinlogCheckpoint,
) -> Result<()> {
    run_incremental_sync_with_checkpoints::<S, checkpoint::NullStore>(
        surreal,
        from_opts,
        from_checkpoint,
        IncrementalSyncOptions::batch(None, None),
        None,
    )
    .await
}

pub async fn run_incremental_sync_with_checkpoints<S, St>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: BinlogCheckpoint,
    options: IncrementalSyncOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
) -> Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    info!(
        "Starting MySQL binlog incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    let pool = new_mysql_pool(&from_opts.connection_string)?;
    let database = resolve_database(&pool, &from_opts).await?;
    let mut conn = pool.get_conn().await?;
    use_database(&mut conn, &database).await?;

    let mut schema = collect_mysql_database_schema(&mut conn).await?;
    let mut json_columns =
        surreal_sync_mysql_trigger_source::json_columns::get_json_columns(&mut conn, &database)
            .await?;

    let mut column_names_cache: HashMap<String, Vec<String>> = HashMap::new();
    let mut table_filter: Option<Vec<String>> = if from_opts.tables.is_empty() {
        None
    } else {
        Some(from_opts.tables.clone())
    };

    let mut client = connect_binlog_client(&from_opts).await?;
    start_binlog_from_checkpoint(&mut client, &from_checkpoint).await?;

    let mut table_maps: HashMap<u64, TableMapEvent> = HashMap::new();
    let mut total_changes = 0u64;
    let mut last_persisted_checkpoint: Option<BinlogCheckpoint> = None;
    let mut last_checkpoint_emit = std::time::Instant::now();

    loop {
        // Graceful cancellation (SIGINT/SIGTERM): flush a final resumable
        // checkpoint and stop cleanly. Checked in both batch and follow modes.
        if options.cancel.is_cancelled() {
            persist_checkpoint(
                checkpoint_manager,
                &client,
                &mut last_persisted_checkpoint,
                true,
            )
            .await?;
            info!("Cancellation requested, stopping incremental sync (checkpoint flushed)");
            break;
        }

        if let Some(deadline) = options_deadline(&options) {
            if Utc::now() >= deadline {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Deadline reached, stopping incremental sync");
                break;
            }
        }

        if let Some(target) = options_until(&options) {
            if client.current_position() >= target.position {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Reached target checkpoint, stopping incremental sync");
                break;
            }
        }

        if should_emit_checkpoint(checkpoint_manager.is_some(), &options, last_checkpoint_emit) {
            persist_checkpoint(
                checkpoint_manager,
                &client,
                &mut last_persisted_checkpoint,
                false,
            )
            .await?;
            last_checkpoint_emit = std::time::Instant::now();
        }

        let events = tokio::select! {
            result = client.next_events(32) => {
                result.map_err(|e| anyhow::anyhow!("binlog read failed: {e}"))?
            }
            _ = options.cancel.cancelled() => {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Cancellation received, stopping incremental sync (checkpoint flushed)");
                break;
            }
        };

        if events.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }

        for event in events {
            match event.body {
                EventBody::Query(query) => {
                    if crate::ddl::is_table_affecting_ddl(&query, &database) {
                        info!("Refreshing MySQL schema metadata after DDL: {}", query.sql);
                        // Keep the synced-table filter aligned with RENAME TABLE so
                        // events on the new name are not silently filtered out.
                        apply_renames_to_filter(
                            &mut table_filter,
                            &crate::ddl::parse_table_renames(&query.sql),
                        );
                        schema = collect_mysql_database_schema(&mut conn).await?;
                        json_columns =
                            surreal_sync_mysql_trigger_source::json_columns::get_json_columns(
                                &mut conn, &database,
                            )
                            .await?;
                        column_names_cache.clear();
                        table_maps.clear();
                    }
                }
                EventBody::TableMap(tm) => {
                    table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows) => {
                    // A row event must always be preceded by its TableMap in the
                    // same stream. A missing map means we would silently drop a
                    // change — fail loudly instead (never `continue` past a row).
                    let table_map = table_maps.get(&rows.table_id).cloned().ok_or_else(|| {
                        anyhow::anyhow!(
                            "binlog row event for table_id {} has no preceding TableMap; \
                             refusing to silently drop the change",
                            rows.table_id
                        )
                    })?;
                    if table_map.database != database {
                        continue;
                    }
                    if let Some(ref tables) = table_filter {
                        if !tables.contains(&table_map.table) {
                            continue;
                        }
                    }

                    for row_change in rows.rows {
                        let position = client.current_position();
                        if let Some(target) = options_until(&options) {
                            if position >= target.position {
                                persist_checkpoint(
                                    checkpoint_manager,
                                    &client,
                                    &mut last_persisted_checkpoint,
                                    true,
                                )
                                .await?;
                                info!("Reached target checkpoint, stopping incremental sync");
                                pool.disconnect().await?;
                                return Ok(());
                            }
                        }

                        let change = CdcChange {
                            position: position.clone(),
                            database: table_map.database.clone(),
                            table: table_map.table.clone(),
                            operation: row_change,
                            xid: None,
                            gtid: None,
                        };

                        let column_names = if let Some(names) =
                            column_names_cache.get(&change.table)
                        {
                            names.clone()
                        } else {
                            let names =
                                get_table_column_names_ordinal(&mut conn, &change.table).await?;
                            column_names_cache.insert(change.table.clone(), names.clone());
                            names
                        };

                        let universal = cdc_change_to_universal(
                            &change,
                            &table_map,
                            &column_names,
                            &schema,
                            &json_columns,
                        )?;

                        surreal.apply_universal_change(&universal).await?;
                        client.commit(position);
                        total_changes += 1;
                        persist_checkpoint(
                            checkpoint_manager,
                            &client,
                            &mut last_persisted_checkpoint,
                            false,
                        )
                        .await?;
                        last_checkpoint_emit = std::time::Instant::now();

                        if total_changes.is_multiple_of(100) {
                            debug!("Processed {total_changes} binlog changes");
                        }
                    }
                }
                _ => {}
            }
        }
    }

    persist_checkpoint(
        checkpoint_manager,
        &client,
        &mut last_persisted_checkpoint,
        true,
    )
    .await?;
    info!("MySQL binlog incremental sync completed: {total_changes} changes applied");
    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

/// Update the synced-table filter so a `RENAME TABLE old TO new` keeps tracking
/// the renamed table under its new name. If the filter is `None` (all tables)
/// there is nothing to adjust. When a synced `old` is renamed, `new` is added
/// and `old` removed (no more events will ever arrive for `old`).
fn apply_renames_to_filter(filter: &mut Option<Vec<String>>, renames: &[crate::ddl::TableRename]) {
    let Some(tables) = filter.as_mut() else {
        return;
    };
    for rename in renames {
        if tables.iter().any(|t| t == &rename.old) {
            tables.retain(|t| t != &rename.old);
            if !tables.iter().any(|t| t == &rename.new) {
                tables.push(rename.new.clone());
            }
            tracing::info!(
                "RENAME TABLE '{}' -> '{}': tracking the renamed table under its new name",
                rename.old,
                rename.new
            );
        }
    }
}

fn options_deadline(options: &IncrementalSyncOptions) -> Option<DateTime<Utc>> {
    match &options.run_mode {
        IncrementalRunMode::Batch { deadline, .. } | IncrementalRunMode::Follow { deadline } => {
            *deadline
        }
    }
}

fn options_until(options: &IncrementalSyncOptions) -> Option<&BinlogCheckpoint> {
    match &options.run_mode {
        IncrementalRunMode::Batch { until, .. } => until.as_ref(),
        IncrementalRunMode::Follow { .. } => None,
    }
}

fn should_emit_checkpoint(
    enabled: bool,
    options: &IncrementalSyncOptions,
    last_emit: std::time::Instant,
) -> bool {
    enabled
        && options.checkpoint_interval > Duration::ZERO
        && last_emit.elapsed() >= options.checkpoint_interval
}

async fn persist_checkpoint<St: CheckpointStore>(
    manager: Option<&SyncManager<St>>,
    client: &binlog_protocol::BinlogClient,
    last: &mut Option<BinlogCheckpoint>,
    force: bool,
) -> Result<()> {
    let Some(manager) = manager else {
        return Ok(());
    };
    let checkpoint = crate::checkpoint::get_current_checkpoint(client)?;
    if !force
        && last
            .as_ref()
            .is_some_and(|prev| prev.position == checkpoint.position)
    {
        return Ok(());
    }
    manager
        .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
        .await?;
    *last = Some(checkpoint);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::TableRename;

    #[test]
    fn rename_updates_synced_table_filter() {
        let mut filter = Some(vec!["orders".to_string(), "users".to_string()]);
        apply_renames_to_filter(
            &mut filter,
            &[TableRename {
                old: "orders".into(),
                new: "orders_v2".into(),
            }],
        );
        let tables = filter.unwrap();
        assert!(tables.contains(&"orders_v2".to_string()));
        assert!(!tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"users".to_string()));
    }

    #[test]
    fn rename_of_unsynced_table_is_ignored() {
        let mut filter = Some(vec!["orders".to_string()]);
        apply_renames_to_filter(
            &mut filter,
            &[TableRename {
                old: "audit".into(),
                new: "audit_v2".into(),
            }],
        );
        assert_eq!(filter.unwrap(), vec!["orders".to_string()]);
    }

    #[test]
    fn rename_with_no_filter_is_noop() {
        let mut filter: Option<Vec<String>> = None;
        apply_renames_to_filter(
            &mut filter,
            &[TableRename {
                old: "orders".into(),
                new: "orders_v2".into(),
            }],
        );
        assert!(filter.is_none());
    }
}
