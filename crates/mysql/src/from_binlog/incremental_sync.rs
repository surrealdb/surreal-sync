//! MySQL binlog replication tail (post-handoff steady CDC).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::binlog_protocol::{BinlogPosition, CdcChange, EventBody, RawEvent, TableMapEvent};
use anyhow::Result;
use chrono::{DateTime, Utc};
use mysql_async::prelude::Queryable;
use surreal_sync_core::DatabaseSchema;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sync_runtime::{
    AdhocApply, ApplyOpts, CheckpointPolicy, ControlSignal, Pipeline, PositionedEvent,
    SourceDriver, SourceRuntimeOpts, StopReason,
};
use surreal_sync_runtime::{SnapshotSignal, SnapshotTransforms};
use tracing::{debug, info};

use crate::from_binlog::catch_up::{
    effective_sync_tables, emit_catch_up_progress, read_catch_up_progress, CatchUpProgress,
    CoverageKind,
};
use crate::from_binlog::change::cdc_to_change;
use crate::from_binlog::checkpoint::BinlogCheckpoint;
use crate::from_binlog::client::{
    connect_binlog_client_with_poll, get_pool_conn, new_mysql_pool_with_ssl, resolve_database,
    start_binlog_from_checkpoint, use_database, DEFAULT_BINLOG_POLL_TIMEOUT,
};
use crate::from_binlog::schema::{collect_mysql_database_schema, get_table_column_names_ordinal};
use crate::from_binlog::signal::{
    acknowledge_execute_snapshot_signal, create_signal_table_sql,
    read_pending_execute_snapshot_signals,
};
use crate::from_binlog::watermark_source::{
    run_adhoc_snapshots_for_tables, write_catch_up_for_tables, BinlogWatermarkSource,
};
use crate::from_binlog::SourceOpts;

/// Default chunk size for ad-hoc snapshots during steady-state streaming.
pub const DEFAULT_STREAM_CHUNK_SIZE: usize = 1024;

/// Default `next_events` batch size in the replication tail loop.
pub const DEFAULT_BINLOG_EVENT_BATCH_SIZE: usize = 32;

/// Default sleep when a replication tail poll returns no events.
pub const DEFAULT_REPLICATION_TAIL_IDLE_SLEEP: Duration = Duration::from_millis(100);

/// Max filtered-only `next_events` batches before `poll_work` yields to the
/// runtime (drain / interval persist / ad-hoc), so catch-up through unrelated
/// GTID noise cannot block control-plane work forever.
const MAX_FILTERED_POLL_BATCHES: usize = 16;

/// Wall-clock bound for the filtered-only skip loop inside `poll_work`.
const MAX_FILTERED_POLL_SPIN: Duration = Duration::from_millis(50);

/// Minimum time between execute-snapshot signal table polls (avoids querying on
/// every filtered-yield outer cycle).
const SIGNAL_POLL_MIN_INTERVAL: Duration = Duration::from_millis(200);

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
    run_replication_tail_with_checkpoints::<S, surreal_sync_core::NullStore>(
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

/// Replication tail via [`BinlogSourceDriver`] + [`surreal_sync_runtime::run_source_runtime_with`] (shared apply window).
///
/// Sink success still gates
/// [`SourceDriver::advance_watermark`](surreal_sync_runtime::SourceDriver::advance_watermark)
/// (binlog client `commit`). CatchUpProgress uses
/// [`CheckpointPolicy::IntervalWhenDrained`]: sunk watermarks persist promptly
/// once the apply window drains; when fully drained with no unsunk work, the
/// same interval may advance the store to the **current** binlog position
/// through filtered/unrelated GTID noise (never while transform/apply still
/// holds buffered or in-flight batches).
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

    let pool = new_mysql_pool_with_ssl(&from_opts.connection_string, &from_opts.ssl).await?;
    let database = resolve_database(&pool, &from_opts).await?;
    let mut conn = get_pool_conn(&pool, &from_opts.connection_string).await?;
    use_database(&mut conn, &database).await?;
    conn.query_drop(create_signal_table_sql()).await?;

    let schema = collect_mysql_database_schema(&mut conn).await?;
    let json_columns = crate::json_columns::get_json_columns(&mut conn, &database).await?;

    let catch_up = if from_opts.tables.is_empty() {
        None
    } else if let Some(manager) = checkpoint_manager {
        read_catch_up_progress(manager).await?
    } else {
        None
    };
    let effective_tables = effective_sync_tables(&from_opts.tables, catch_up.as_ref());
    let table_filter = effective_tables.clone();

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

    let transforms = SnapshotTransforms::from_refs(pipeline, apply_opts);
    let mut driver = BinlogSourceDriver {
        surreal,
        pool: pool.clone(),
        database,
        from_opts,
        options: &options,
        checkpoint_manager,
        transforms,
        conn,
        client: Some(client),
        schema,
        json_columns,
        column_names_cache: HashMap::new(),
        table_maps: HashMap::new(),
        table_filter,
        last_sunk_checkpoint: None,
        last_persisted_checkpoint: None,
        total_changes: 0,
        pending_adhoc_signals: Vec::new(),
        last_signal_check: None,
        until_reached: false,
        cancel_seen: false,
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = surreal_sync_runtime::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Cancelled) => {
            info!("Cancellation requested, stopping incremental sync (checkpoint flushed)");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Deadline) => {
            info!("Deadline reached, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Until) => {
            info!("Reached target checkpoint, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Finished) => {
            info!("Binlog source finished");
        }
    }

    driver.finalize_store_checkpoint().await?;
    info!(
        "MySQL binlog incremental sync completed: {} changes sunk",
        driver.total_changes
    );
    drop(driver.conn);
    pool.disconnect().await?;
    Ok(())
}

/// Binlog CDC driver for [`surreal_sync_runtime::run_source_runtime_with`].
struct BinlogSourceDriver<'a, S, St: CheckpointStore> {
    surreal: &'a S,
    pool: mysql_async::Pool,
    database: String,
    from_opts: SourceOpts,
    options: &'a ReplicationTailOptions,
    checkpoint_manager: Option<&'a SyncManager<St>>,
    transforms: SnapshotTransforms,
    conn: mysql_async::Conn,
    /// Taken temporarily while an ad-hoc watermark snapshot borrows the session.
    client: Option<crate::binlog_protocol::BinlogClient>,
    schema: DatabaseSchema,
    json_columns: HashMap<String, Vec<String>>,
    column_names_cache: HashMap<String, Vec<String>>,
    table_maps: HashMap<u64, TableMapEvent>,
    table_filter: Option<Vec<String>>,
    last_sunk_checkpoint: Option<BinlogCheckpoint>,
    last_persisted_checkpoint: Option<BinlogCheckpoint>,
    /// Events successfully sunk (not merely buffered).
    total_changes: u64,
    pending_adhoc_signals: Vec<SnapshotSignal>,
    /// Last successful execute-snapshot signal table poll (`None` = never).
    last_signal_check: Option<std::time::Instant>,
    until_reached: bool,
    cancel_seen: bool,
}

impl<'a, S, St> BinlogSourceDriver<'a, S, St>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    fn client_mut(&mut self) -> Result<&mut crate::binlog_protocol::BinlogClient> {
        self.client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("binlog client unavailable during ad-hoc snapshot"))
    }

    fn client_ref(&self) -> Result<&crate::binlog_protocol::BinlogClient> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("binlog client unavailable during ad-hoc snapshot"))
    }

    async fn finalize_store_checkpoint(&mut self) -> Result<()> {
        let client = self.client_ref()?;
        let current = crate::from_binlog::checkpoint::get_current_checkpoint(client)?;
        let Some(cp) =
            store_checkpoint_to_emit(self.last_sunk_checkpoint.as_ref(), &current, false)
        else {
            return Ok(());
        };
        persist_checkpoint_at(
            self.checkpoint_manager,
            Some(&cp),
            &mut self.last_persisted_checkpoint,
            true,
        )
        .await
    }

    async fn refresh_schema_metadata(&mut self) -> Result<()> {
        self.schema = collect_mysql_database_schema(&mut self.conn).await?;
        self.json_columns =
            crate::json_columns::get_json_columns(&mut self.conn, &self.database).await?;
        self.column_names_cache.clear();
        self.table_maps.clear();
        Ok(())
    }

    /// Convert raw binlog events into positioned changes. Table-affecting DDL
    /// refreshes schema metadata inline (same as the pre-SourceDriver loop) so
    /// subsequent row events in the same batch see the new catalog.
    async fn consume_events(
        &mut self,
        events: Vec<RawEvent>,
    ) -> Result<Vec<PositionedEvent<BinlogPosition>>> {
        let mut out = Vec::new();
        for event in events {
            match event.body {
                EventBody::Query(query) => {
                    if crate::from_binlog::ddl::is_table_affecting_ddl(&query, &self.database) {
                        info!("Refreshing MySQL schema metadata after DDL: {}", query.sql);
                        apply_renames_to_filter(
                            &mut self.table_filter,
                            &crate::from_binlog::ddl::parse_table_renames(&query.sql),
                        );
                        self.refresh_schema_metadata().await?;
                    }
                }
                EventBody::TableMap(tm) => {
                    self.table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows) => {
                    let table_map =
                        self.table_maps
                            .get(&rows.table_id)
                            .cloned()
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "binlog row event for table_id {} has no preceding TableMap; \
                             refusing to silently drop the change",
                                    rows.table_id
                                )
                            })?;
                    if table_map.database != self.database {
                        continue;
                    }
                    if let Some(ref tables) = self.table_filter {
                        if !tables.contains(&table_map.table) {
                            continue;
                        }
                    }

                    for row_change in rows.rows {
                        let position = {
                            let client = self.client_mut()?;
                            client.current_position()
                        };
                        if let Some(target) = self.options.until.as_ref() {
                            if position >= target.position {
                                self.until_reached = true;
                                // Do not apply the event at/past the until bound.
                                return Ok(out);
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

                        let column_names =
                            if let Some(names) = self.column_names_cache.get(&change.table) {
                                names.clone()
                            } else {
                                let names =
                                    get_table_column_names_ordinal(&mut self.conn, &change.table)
                                        .await?;
                                self.column_names_cache
                                    .insert(change.table.clone(), names.clone());
                                names
                            };

                        let universal = cdc_to_change(
                            &change,
                            &table_map,
                            &column_names,
                            &self.schema,
                            &self.json_columns,
                        )?;
                        out.push(PositionedEvent::change(universal, position));
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl<'a, S, St> SourceDriver for BinlogSourceDriver<'a, S, St>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    type Position = BinlogPosition;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() {
            return Ok(Vec::new());
        }

        // Keep reading while events are available but filtered out (other schemas,
        // non-row events). Returning empty every batch would re-enter
        // between_events (signal-table query) between every binlog event and
        // stall catch-up through large unrelated transactions. Bound the spin
        // so the runtime can still drain, interval-persist, and poll ad-hoc.
        let mut out = Vec::new();
        let spin_started = std::time::Instant::now();
        let mut filtered_batches = 0usize;
        loop {
            if self.stop_reason().is_some() {
                return Ok(out);
            }

            let event_batch_size = self.options.event_batch_size;
            let cancel = self.options.cancel.clone();
            let events = {
                let client = self.client_mut()?;
                tokio::select! {
                    result = client.next_events(event_batch_size) => {
                        result.map_err(|e| anyhow::anyhow!("binlog read failed: {e}"))?
                    }
                    _ = cancel.cancelled() => {
                        self.cancel_seen = true;
                        return Ok(out);
                    }
                }
            };

            if events.is_empty() {
                if out.is_empty() {
                    tokio::time::sleep(self.options.idle_sleep).await;
                }
                return Ok(out);
            }

            out.extend(self.consume_events(events).await?);
            if !out.is_empty() {
                return Ok(out);
            }

            filtered_batches = filtered_batches.saturating_add(1);
            if filtered_batches >= MAX_FILTERED_POLL_BATCHES
                || spin_started.elapsed() >= MAX_FILTERED_POLL_SPIN
            {
                return Ok(out);
            }
        }
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("binlog client unavailable during ad-hoc snapshot"))?;
        client.commit(position.clone());
        self.last_sunk_checkpoint = Some(BinlogCheckpoint {
            flavor: client.flavor(),
            position,
            timestamp: Utc::now(),
        });
        Ok(())
    }

    async fn between_events(&mut self) -> Result<Vec<ControlSignal>> {
        let mut signals = Vec::new();
        let signal_poll_due = self
            .last_signal_check
            .map(|t| t.elapsed() >= SIGNAL_POLL_MIN_INTERVAL)
            .unwrap_or(true);
        if self.pending_adhoc_signals.is_empty() && signal_poll_due {
            let pending = read_pending_execute_snapshot_signals(&self.pool, &self.database).await?;
            self.pending_adhoc_signals = pending;
            self.last_signal_check = Some(std::time::Instant::now());
        }
        if !self.pending_adhoc_signals.is_empty() {
            let tables = self
                .pending_adhoc_signals
                .iter()
                .flat_map(|s| s.tables.iter().cloned())
                .collect();
            signals.push(ControlSignal::AdHocSnapshot { tables });
        }
        Ok(signals)
    }

    async fn on_schema_refresh(&mut self) -> Result<()> {
        // DDL is refreshed inline in poll_work; this hook remains for control-plane
        // callers that emit SchemaRefresh explicitly.
        self.refresh_schema_metadata().await
    }

    async fn on_adhoc_snapshot(
        &mut self,
        _tables: &[String],
        _apply: &dyn AdhocApply,
    ) -> Result<()> {
        let signals = std::mem::take(&mut self.pending_adhoc_signals);
        if signals.is_empty() {
            return Ok(());
        }

        let client = self
            .client
            .take()
            .ok_or_else(|| anyhow::anyhow!("binlog client missing for ad-hoc snapshot"))?;

        let mut wm = BinlogWatermarkSource::wrap_active_binlog_client(
            self.pool.clone(),
            self.database.clone(),
            &self.from_opts,
            client,
            self.options.cancel.clone(),
        )
        .await?;

        for signal in signals {
            info!(
                "ad-hoc snapshot signal {} requesting tables {:?}",
                signal.id, signal.tables
            );
            if let Some(ref mut tables) = self.table_filter {
                for name in &signal.tables {
                    if !tables.contains(name) {
                        tables.push(name.clone());
                    }
                }
            }
            run_adhoc_snapshots_for_tables(
                &mut wm,
                self.surreal,
                &signal.tables,
                self.options.chunk_size,
                self.checkpoint_manager,
                &self.transforms,
            )
            .await?;

            acknowledge_execute_snapshot_signal(&self.pool, &self.database, &signal.id).await?;

            if let Some(manager) = self.checkpoint_manager {
                // Runtime flushes before ad-hoc, so the apply window is drained.
                let current = wm.current_checkpoint()?;
                let existing = read_catch_up_progress(manager).await?;
                if let Some(checkpoint) = adhoc_catch_up_checkpoint(
                    self.last_sunk_checkpoint.as_ref(),
                    &current,
                    false,
                    existing.as_ref(),
                ) {
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
        }

        self.client = Some(wm.into_binlog_client());
        Ok(())
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if self.cancel_seen || self.options.cancel.is_cancelled() {
            return Some(StopReason::Cancelled);
        }
        if let Some(deadline) = self.options.deadline {
            if Utc::now() >= deadline {
                return Some(StopReason::Deadline);
            }
        }
        if self.until_reached {
            return Some(StopReason::Until);
        }
        if let (Some(target), Some(client)) = (self.options.until.as_ref(), self.client.as_ref()) {
            if client.current_position() >= target.position {
                return Some(StopReason::Until);
            }
        }
        None
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::IntervalWhenDrained {
            interval: self.options.checkpoint_interval,
        }
    }

    async fn persist_checkpoint(&mut self, position: Self::Position) -> Result<()> {
        // IntervalWhenDrained only calls this when the apply window is empty, so
        // `current` is safe (may include filtered-only advances past last sunk).
        let client = self.client_ref()?;
        let current = crate::from_binlog::checkpoint::get_current_checkpoint(client)?;
        let sunk = self.last_sunk_checkpoint.clone().or_else(|| {
            Some(BinlogCheckpoint {
                flavor: client.flavor(),
                position,
                timestamp: Utc::now(),
            })
        });
        let Some(cp) = store_checkpoint_to_emit(sunk.as_ref(), &current, false) else {
            return Ok(());
        };
        persist_checkpoint_at(
            self.checkpoint_manager,
            Some(&cp),
            &mut self.last_persisted_checkpoint,
            false,
        )
        .await
    }

    async fn read_progress_for_persist(&mut self) -> Result<Option<Self::Position>> {
        // Fully drained: current read position is safe to persist (filtered noise).
        let client = self.client_ref()?;
        Ok(Some(client.current_position()))
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.total_changes = self.total_changes.saturating_add(count);
        if self.total_changes > 0 && self.total_changes.is_multiple_of(100) {
            debug!("Sunk {} binlog changes", self.total_changes);
        }
    }
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
/// Never persists read-ahead `current` while apply still has unsunk work —
/// same policy as the interval path (`store_checkpoint_to_emit`):
/// - unsunk + `last_sunk` → use `last_sunk`
/// - unsunk + no `last_sunk` + existing → keep existing position (coverage only)
/// - unsunk + no `last_sunk` + no existing → `None` (skip write; do not invent `current`)
/// - fully drained → `current` is safe
fn adhoc_catch_up_checkpoint(
    last_sunk: Option<&BinlogCheckpoint>,
    current: &BinlogCheckpoint,
    has_unsunk_work: bool,
    existing: Option<&CatchUpProgress>,
) -> Option<BinlogCheckpoint> {
    store_checkpoint_to_emit(last_sunk, current, has_unsunk_work)
        .or_else(|| existing.map(|p| p.position.clone()))
}

/// Update the synced-table filter so a `RENAME TABLE old TO new` keeps tracking
/// the renamed table under its new name. If the filter is `None` (all tables)
/// there is nothing to adjust. When a synced `old` is renamed, `new` is added
/// and `old` removed (no more events will ever arrive for `old`).
fn apply_renames_to_filter(
    filter: &mut Option<Vec<String>>,
    renames: &[crate::from_binlog::ddl::TableRename],
) {
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
    use crate::binlog_protocol::BinlogPosition;
    use crate::from_binlog::ddl::TableRename;
    use crate::from_binlog::flavor::Flavor;

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
        let cp = adhoc_catch_up_checkpoint(Some(&sunk), &current, true, Some(&existing)).unwrap();
        assert_eq!(cp.position, sunk.position);

        // Unsung work, no last_sunk → keep existing progress position (coverage only).
        let cp = adhoc_catch_up_checkpoint(None, &current, true, Some(&existing)).unwrap();
        assert_eq!(cp, existing.position);

        // Unsung + no last_sunk + no existing → skip (never invent read-ahead current).
        assert!(adhoc_catch_up_checkpoint(None, &current, true, None).is_none());

        // Fully drained → current is safe (same as interval path).
        let cp = adhoc_catch_up_checkpoint(Some(&sunk), &current, false, Some(&existing)).unwrap();
        assert_eq!(cp.position, current.position);

        // Fully drained, no existing → current is still safe.
        let cp = adhoc_catch_up_checkpoint(None, &current, false, None).unwrap();
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
