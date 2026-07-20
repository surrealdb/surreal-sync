//! MySQL binlog replication tail (post-handoff steady CDC).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use binlog_protocol::{BinlogPosition, CdcChange, EventBody, TableMapEvent};
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::{DateTime, Utc};
use mysql_async::prelude::Queryable;
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::SnapshotTransforms;
use sync_transform::{ApplyContext, ApplyOpts, Pipeline};
use tracing::{debug, info};

use crate::catch_up::{
    effective_sync_tables, emit_catch_up_progress, read_catch_up_progress, CatchUpProgress,
    CoverageKind,
};
use crate::change::cdc_change_to_universal;
use crate::checkpoint::BinlogCheckpoint;
use crate::client::{
    connect_binlog_client_with_poll, get_pool_conn, new_mysql_pool, resolve_database,
    start_binlog_from_checkpoint, use_database, DEFAULT_BINLOG_POLL_TIMEOUT,
};
use crate::schema::{collect_mysql_database_schema, get_table_column_names_ordinal};
use crate::signal::{
    acknowledge_execute_snapshot_signal, create_signal_table_sql,
    read_pending_execute_snapshot_signals,
};
use crate::watermark_source::{
    run_adhoc_snapshots_for_tables, write_catch_up_for_tables, BinlogWatermarkSource,
};
use crate::SourceOpts;

/// Default chunk size for ad-hoc snapshots during steady-state streaming.
pub const DEFAULT_STREAM_CHUNK_SIZE: usize = 1024;

/// Default `next_events` batch size in the replication tail loop.
pub const DEFAULT_BINLOG_EVENT_BATCH_SIZE: usize = 32;

/// Default sleep when a replication tail poll returns no events.
pub const DEFAULT_REPLICATION_TAIL_IDLE_SLEEP: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    /// Optional wall-clock stop for the stream phase.
    pub deadline: Option<DateTime<Utc>>,
    /// Optional exact binlog/GTID stop bound for the stream phase.
    pub until: Option<BinlogCheckpoint>,
    pub checkpoint_interval: Duration,
    /// Rows read per keyset chunk when handling ad-hoc snapshot signals.
    pub chunk_size: usize,
    /// Max events requested per `next_events` call in the replication tail loop.
    pub event_batch_size: usize,
    /// Sleep when a poll returns no events before retrying.
    pub idle_sleep: Duration,
    /// Blocking read timeout for binlog packet polls on the replication client.
    pub binlog_poll_timeout: Duration,
    /// Cooperative cancellation signal. When cancelled, the sync loop flushes a
    /// final resumable checkpoint and returns cleanly (SIGINT/SIGTERM in the CLI;
    /// tests can trigger it directly). Defaults to a never-cancelled token.
    pub cancel: tokio_util::sync::CancellationToken,
}

impl ReplicationTailOptions {
    pub fn stream(deadline: Option<DateTime<Utc>>, until: Option<BinlogCheckpoint>) -> Self {
        Self {
            deadline,
            until,
            checkpoint_interval: Duration::from_secs(10),
            chunk_size: DEFAULT_STREAM_CHUNK_SIZE,
            event_batch_size: DEFAULT_BINLOG_EVENT_BATCH_SIZE,
            idle_sleep: DEFAULT_REPLICATION_TAIL_IDLE_SLEEP,
            binlog_poll_timeout: DEFAULT_BINLOG_POLL_TIMEOUT,
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

pub async fn run_replication_tail<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: BinlogCheckpoint,
) -> Result<()> {
    run_replication_tail_with_checkpoints::<S, checkpoint::NullStore>(
        surreal,
        from_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(None, None),
        None,
    )
    .await
}

/// Replication tail with identity transforms (preserves pre-transform behavior).
pub async fn run_replication_tail_with_checkpoints<S, St>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: BinlogCheckpoint,
    options: ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
) -> Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_replication_tail_with_transforms(
        surreal,
        from_opts,
        from_checkpoint,
        options,
        checkpoint_manager,
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Replication tail through the transform apply framework.
///
/// Uses [`ApplyContext`] (not [`sync_transform::run_change_feed`]) so the
/// existing control loop can keep handling DDL, ad-hoc snapshot signals,
/// deadlines, and cancellation between polls. Sink success still gates
/// [`binlog_protocol::BinlogClient::commit`] and CatchUpProgress persistence:
/// store checkpoints advance only from the last successfully sunk position
/// (never from read-ahead [`binlog_protocol::BinlogClient::current_position`]
/// while transform batches remain buffered or in flight).
pub async fn run_replication_tail_with_transforms<S, St>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: BinlogCheckpoint,
    options: ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    info!(
        "Starting MySQL binlog incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );
    if pipeline.is_identity() {
        debug!("Incremental sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            batch_size = apply_opts.batch_size,
            "Incremental sync using transform pipeline"
        );
    }

    let pool = new_mysql_pool(&from_opts.connection_string)?;
    let database = resolve_database(&pool, &from_opts).await?;
    let mut conn = get_pool_conn(&pool, &from_opts.connection_string).await?;
    use_database(&mut conn, &database).await?;
    conn.query_drop(create_signal_table_sql()).await?;

    let mut schema = collect_mysql_database_schema(&mut conn).await?;
    let mut json_columns =
        surreal_sync_mysql_trigger_source::json_columns::get_json_columns(&mut conn, &database)
            .await?;

    let mut column_names_cache: HashMap<String, Vec<String>> = HashMap::new();

    let catch_up = if from_opts.tables.is_empty() {
        None
    } else if let Some(manager) = checkpoint_manager {
        read_catch_up_progress(manager).await?
    } else {
        None
    };
    let effective_tables = effective_sync_tables(&from_opts.tables, catch_up.as_ref());
    let mut table_filter = effective_tables.clone();

    let mut client =
        connect_binlog_client_with_poll(&from_opts, options.binlog_poll_timeout).await?;
    start_binlog_from_checkpoint(&mut client, &from_checkpoint).await?;

    if let (Some(manager), Some(ref effective)) = (checkpoint_manager, &effective_tables) {
        if let Some(mut progress) = catch_up {
            let before = progress.covered_tables.len();
            progress.prune_to_requested(effective);
            if progress.covered_tables.len() != before {
                emit_catch_up_progress(manager, &progress).await?;
            }
        }
    }

    let mut table_maps: HashMap<u64, TableMapEvent> = HashMap::new();
    let mut total_changes = 0u64;
    // Last position successfully sunk + committed (not merely read from binlog).
    let mut last_sunk_checkpoint: Option<BinlogCheckpoint> = None;
    let mut last_persisted_checkpoint: Option<BinlogCheckpoint> = None;
    let mut last_checkpoint_emit = std::time::Instant::now();

    let transforms = SnapshotTransforms::from_refs(pipeline, apply_opts);
    let transformer = Arc::new(pipeline.clone());
    let mut apply_ctx = ApplyContext::new(surreal, transformer, apply_opts);

    loop {
        if options.cancel.is_cancelled() {
            flush_apply_ctx(
                &mut apply_ctx,
                &mut client,
                &mut total_changes,
                &mut last_sunk_checkpoint,
                checkpoint_manager,
                &mut last_persisted_checkpoint,
                true,
            )
            .await?;
            info!("Cancellation requested, stopping incremental sync (checkpoint flushed)");
            break;
        }

        if let Some(deadline) = options.deadline {
            if Utc::now() >= deadline {
                flush_apply_ctx(
                    &mut apply_ctx,
                    &mut client,
                    &mut total_changes,
                    &mut last_sunk_checkpoint,
                    checkpoint_manager,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Deadline reached, stopping incremental sync");
                break;
            }
        }

        if let Some(target) = options.until.as_ref() {
            if client.current_position() >= target.position {
                flush_apply_ctx(
                    &mut apply_ctx,
                    &mut client,
                    &mut total_changes,
                    &mut last_sunk_checkpoint,
                    checkpoint_manager,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Reached target checkpoint, stopping incremental sync");
                break;
            }
        }

        // Drain buffered transforms before any store checkpoint emit and before
        // handing the client to ad-hoc snapshots. Persist only after flush so
        // CatchUpProgress cannot advance past unsunk buffered batches.
        flush_apply_ctx(
            &mut apply_ctx,
            &mut client,
            &mut total_changes,
            &mut last_sunk_checkpoint,
            checkpoint_manager,
            &mut last_persisted_checkpoint,
            false,
        )
        .await?;

        if should_emit_checkpoint(checkpoint_manager.is_some(), &options, last_checkpoint_emit) {
            emit_store_checkpoint(
                checkpoint_manager,
                &client,
                &apply_ctx,
                &last_sunk_checkpoint,
                &mut last_persisted_checkpoint,
                false,
            )
            .await?;
            last_checkpoint_emit = std::time::Instant::now();
        }

        client = handle_snapshot_signals(
            surreal,
            &pool,
            &database,
            &from_opts,
            client,
            &mut table_filter,
            &options,
            checkpoint_manager,
            &transforms,
            &apply_ctx,
            &last_sunk_checkpoint,
        )
        .await?;

        let events = tokio::select! {
            result = client.next_events(options.event_batch_size) => {
                result.map_err(|e| anyhow::anyhow!("binlog read failed: {e}"))?
            }
            _ = options.cancel.cancelled() => {
                flush_apply_ctx(
                    &mut apply_ctx,
                    &mut client,
                    &mut total_changes,
                    &mut last_sunk_checkpoint,
                    checkpoint_manager,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Cancellation received, stopping incremental sync (checkpoint flushed)");
                break;
            }
        };

        if events.is_empty() {
            // Idle: flush a partial transform batch after batch_max_wait, else sleep.
            if apply_ctx.buffer_len() > 0 {
                tokio::time::sleep(apply_opts.batch_max_wait).await;
                flush_apply_ctx(
                    &mut apply_ctx,
                    &mut client,
                    &mut total_changes,
                    &mut last_sunk_checkpoint,
                    checkpoint_manager,
                    &mut last_persisted_checkpoint,
                    false,
                )
                .await?;
            } else {
                tokio::time::sleep(options.idle_sleep).await;
            }
            continue;
        }

        for event in events {
            match event.body {
                EventBody::Query(query) => {
                    if crate::ddl::is_table_affecting_ddl(&query, &database) {
                        info!("Refreshing MySQL schema metadata after DDL: {}", query.sql);
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
                        if let Some(target) = options.until.as_ref() {
                            if position >= target.position {
                                flush_apply_ctx(
                                    &mut apply_ctx,
                                    &mut client,
                                    &mut total_changes,
                                    &mut last_sunk_checkpoint,
                                    checkpoint_manager,
                                    &mut last_persisted_checkpoint,
                                    true,
                                )
                                .await?;
                                info!("Reached target checkpoint, stopping incremental sync");
                                drop(conn);
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

                        // Transform → sink; commit binlog position only after sink OK.
                        if let Some(pos) = apply_ctx.push_change(universal, position).await? {
                            note_sunk_position(
                                &mut client,
                                pos,
                                &mut last_sunk_checkpoint,
                            );
                            total_changes = total_changes
                                .saturating_add(apply_ctx.take_sunk_change_count());
                            persist_checkpoint_at(
                                checkpoint_manager,
                                last_sunk_checkpoint.as_ref(),
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
                }
                _ => {}
            }
        }
    }

    flush_apply_ctx(
        &mut apply_ctx,
        &mut client,
        &mut total_changes,
        &mut last_sunk_checkpoint,
        checkpoint_manager,
        &mut last_persisted_checkpoint,
        true,
    )
    .await?;
    info!("MySQL binlog incremental sync completed: {total_changes} changes applied");
    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

fn commit_binlog_position(client: &mut binlog_protocol::BinlogClient, position: BinlogPosition) {
    client.commit(position);
}

fn note_sunk_position(
    client: &mut binlog_protocol::BinlogClient,
    position: BinlogPosition,
    last_sunk: &mut Option<BinlogCheckpoint>,
) {
    commit_binlog_position(client, position.clone());
    *last_sunk = Some(BinlogCheckpoint {
        flavor: client.flavor(),
        position,
        timestamp: Utc::now(),
    });
}

/// Whether [`ApplyContext`] still holds work that has not been sunk.
///
/// Includes completed transform results waiting for ordered sink apply — not
/// only the input buffer and in-flight JoinSet tasks.
fn apply_has_unsunk_work<S, T, P>(apply_ctx: &ApplyContext<'_, S, T, P>) -> bool
where
    S: SurrealSink,
    T: sync_transform::BatchTransformer + 'static,
    P: Clone + Send + Sync + 'static,
{
    apply_ctx.buffer_len() > 0
        || apply_ctx.in_flight_count() > 0
        || apply_ctx.completed_waiting_count() > 0
}

/// Position safe to write to CatchUpProgress.
///
/// While transform/apply still has buffered, in-flight, or completed-waiting
/// work, never advance past `last_sunk` — `current` may already reflect
/// read-ahead events that have not been sunk yet. When there is no unsunk work,
/// `current` is safe (it may include filtered-only advances with nothing to apply).
fn store_checkpoint_to_emit(
    last_sunk: Option<&BinlogCheckpoint>,
    current: &BinlogCheckpoint,
    has_unsunk_work: bool,
) -> Option<BinlogCheckpoint> {
    if has_unsunk_work {
        last_sunk.cloned()
    } else {
        Some(current.clone())
    }
}

/// CatchUpProgress position for ad-hoc table coverage merges.
///
/// Never persists read-ahead `current` while apply still has unsunk work.
/// Falls back to the existing progress position (table coverage only) when
/// there is unsunk work and no `last_sunk` yet.
fn adhoc_catch_up_checkpoint(
    last_sunk: Option<&BinlogCheckpoint>,
    current: &BinlogCheckpoint,
    has_unsunk_work: bool,
    existing: Option<&CatchUpProgress>,
) -> BinlogCheckpoint {
    store_checkpoint_to_emit(last_sunk, current, has_unsunk_work)
        .or_else(|| existing.map(|p| p.position.clone()))
        .unwrap_or_else(|| current.clone())
}

async fn emit_store_checkpoint<S, T, St, P>(
    manager: Option<&SyncManager<St>>,
    client: &binlog_protocol::BinlogClient,
    apply_ctx: &ApplyContext<'_, S, T, P>,
    last_sunk: &Option<BinlogCheckpoint>,
    last_persisted: &mut Option<BinlogCheckpoint>,
    force: bool,
) -> Result<()>
where
    S: SurrealSink,
    T: sync_transform::BatchTransformer + 'static,
    St: CheckpointStore,
    P: Clone + Send + Sync + 'static,
{
    let has_unsunk = apply_has_unsunk_work(apply_ctx);
    let current = crate::checkpoint::get_current_checkpoint(client)?;
    let Some(cp) = store_checkpoint_to_emit(last_sunk.as_ref(), &current, has_unsunk) else {
        return Ok(());
    };
    persist_checkpoint_at(manager, Some(&cp), last_persisted, force).await
}

async fn flush_apply_ctx<S, T, St>(
    apply_ctx: &mut ApplyContext<'_, S, T, BinlogPosition>,
    client: &mut binlog_protocol::BinlogClient,
    total_changes: &mut u64,
    last_sunk: &mut Option<BinlogCheckpoint>,
    checkpoint_manager: Option<&SyncManager<St>>,
    last_persisted: &mut Option<BinlogCheckpoint>,
    force_persist: bool,
) -> Result<()>
where
    S: SurrealSink,
    T: sync_transform::BatchTransformer + 'static,
    St: CheckpointStore,
{
    if !apply_has_unsunk_work(apply_ctx) {
        if force_persist {
            emit_store_checkpoint(
                checkpoint_manager,
                client,
                apply_ctx,
                last_sunk,
                last_persisted,
                true,
            )
            .await?;
        }
        return Ok(());
    }
    if let Some(pos) = apply_ctx.flush().await? {
        note_sunk_position(client, pos, last_sunk);
        *total_changes = total_changes.saturating_add(apply_ctx.take_sunk_change_count());
        persist_checkpoint_at(
            checkpoint_manager,
            last_sunk.as_ref(),
            last_persisted,
            force_persist,
        )
        .await?;
    } else if force_persist {
        emit_store_checkpoint(
            checkpoint_manager,
            client,
            apply_ctx,
            last_sunk,
            last_persisted,
            true,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_snapshot_signals<S, T, St, P>(
    surreal: &S,
    pool: &mysql_async::Pool,
    database: &str,
    from_opts: &SourceOpts,
    client: binlog_protocol::BinlogClient,
    table_filter: &mut Option<Vec<String>>,
    options: &ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
    transforms: &SnapshotTransforms,
    apply_ctx: &ApplyContext<'_, S, T, P>,
    last_sunk: &Option<BinlogCheckpoint>,
) -> Result<binlog_protocol::BinlogClient>
where
    S: SurrealSink,
    T: sync_transform::BatchTransformer + 'static,
    St: CheckpointStore,
    P: Clone + Send + Sync + 'static,
{
    let signals = read_pending_execute_snapshot_signals(pool, database).await?;
    if signals.is_empty() {
        return Ok(client);
    }

    let mut wm = BinlogWatermarkSource::wrap_active_binlog_client(
        pool.clone(),
        database.to_string(),
        from_opts,
        client,
        options.cancel.clone(),
    )
    .await?;

    for signal in signals {
        info!(
            "ad-hoc snapshot signal {} requesting tables {:?}",
            signal.id, signal.tables
        );
        if let Some(ref mut tables) = table_filter {
            for name in &signal.tables {
                if !tables.contains(name) {
                    tables.push(name.clone());
                }
            }
        }
        run_adhoc_snapshots_for_tables(
            &mut wm,
            surreal,
            &signal.tables,
            options.chunk_size,
            checkpoint_manager,
            transforms,
        )
        .await?;

        acknowledge_execute_snapshot_signal(pool, database, &signal.id).await?;

        if let Some(manager) = checkpoint_manager {
            // Same policy as interval CatchUpProgress: never persist read-ahead
            // current_position while ApplyContext still has unsunk work.
            let current = wm.current_checkpoint()?;
            let existing = read_catch_up_progress(manager).await?;
            let checkpoint = adhoc_catch_up_checkpoint(
                last_sunk.as_ref(),
                &current,
                apply_has_unsunk_work(apply_ctx),
                existing.as_ref(),
            );
            write_catch_up_for_tables(
                manager,
                signal.tables.clone(),
                CoverageKind::AdHoc,
                &checkpoint,
                existing,
            )
            .await?;
        }
    }

    Ok(wm.into_binlog_client())
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

fn should_emit_checkpoint(
    enabled: bool,
    options: &ReplicationTailOptions,
    last_emit: std::time::Instant,
) -> bool {
    enabled
        && options.checkpoint_interval > Duration::ZERO
        && last_emit.elapsed() >= options.checkpoint_interval
}

async fn persist_checkpoint_at<St: CheckpointStore>(
    manager: Option<&SyncManager<St>>,
    checkpoint: Option<&BinlogCheckpoint>,
    last: &mut Option<BinlogCheckpoint>,
    force: bool,
) -> Result<()> {
    let Some(manager) = manager else {
        return Ok(());
    };
    let Some(checkpoint) = checkpoint else {
        return Ok(());
    };
    if !force
        && last
            .as_ref()
            .is_some_and(|prev| prev.position == checkpoint.position)
    {
        return Ok(());
    }
    let existing = read_catch_up_progress(manager).await?;
    let mut progress = existing.unwrap_or_else(|| CatchUpProgress::new(checkpoint.clone()));
    progress.update_position(checkpoint.clone());
    manager
        .emit_checkpoint(&progress, SyncPhase::CatchUpProgress)
        .await?;
    *last = Some(checkpoint.clone());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::TableRename;
    use crate::flavor::Flavor;
    use binlog_protocol::BinlogPosition;

    fn ckpt(pos: u64) -> BinlogCheckpoint {
        BinlogCheckpoint {
            flavor: Flavor::MySql,
            position: BinlogPosition::file_pos("mysql-bin.000001", pos),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn store_checkpoint_refuses_to_advance_past_unsunk_buffer() {
        let sunk = ckpt(100);
        let current = ckpt(200);

        // Buffered / in-flight / completed-waiting: persist last sunk only.
        let emitted = store_checkpoint_to_emit(Some(&sunk), &current, true).unwrap();
        assert_eq!(emitted.position, sunk.position);

        // No prior sink while buffered: skip persist entirely.
        assert!(store_checkpoint_to_emit(None, &current, true).is_none());

        // Nothing unsunk: current is safe (may include filtered-only advances).
        let emitted = store_checkpoint_to_emit(Some(&sunk), &current, false).unwrap();
        assert_eq!(emitted.position, current.position);
    }

    #[test]
    fn adhoc_catch_up_uses_last_sunk_when_unsunk_work_remains() {
        let sunk = ckpt(100);
        let current = ckpt(200);
        let existing = CatchUpProgress::new(ckpt(50));

        // Unsung work + last_sunk → never write read-ahead current.
        let cp = adhoc_catch_up_checkpoint(Some(&sunk), &current, true, Some(&existing));
        assert_eq!(cp.position, sunk.position);

        // Unsung work, no last_sunk → keep existing progress position (coverage only).
        let cp = adhoc_catch_up_checkpoint(None, &current, true, Some(&existing));
        assert_eq!(cp, existing.position);

        // Fully drained → current is safe (same as interval path).
        let cp = adhoc_catch_up_checkpoint(Some(&sunk), &current, false, Some(&existing));
        assert_eq!(cp.position, current.position);
    }

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
