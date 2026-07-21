//! PostgreSQL pgoutput replication tail (post-handoff steady CDC).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::{DateTime, Utc};
use pgoutput_protocol::{Lsn, PgWalClient, RelationMeta, StreamEvent};
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::{SnapshotSignal, SnapshotTransforms};
use sync_core::DatabaseSchema;
use sync_transform::{
    AdhocApply, ApplyOpts, CheckpointPolicy, ControlSignal, Pipeline, PositionedEvent,
    SourceDriver, SourceRuntimeOpts, StopReason,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::catch_up::{
    effective_sync_tables, emit_catch_up_progress, read_catch_up_progress, CatchUpProgress,
    CoverageKind,
};
use crate::change::cdc_change_to_universal;
use crate::checkpoint::{get_current_checkpoint, PgoutputCheckpoint};
use crate::client::{
    connect_wal_client, ensure_publication_for_source, new_sql_client, resolve_schema,
    start_wal_from_checkpoint,
};
use crate::ddl::{
    detect_relation_rename_from_filter, detect_relation_renames, truncate_affects_synced,
};
use crate::schema::{collect_postgresql_database_schema, get_table_column_names_ordinal};
use crate::signal::{acknowledge_execute_snapshot_signal, read_pending_execute_snapshot_signals};
use crate::watermark_source::{
    run_adhoc_snapshots_for_tables, write_catch_up_for_tables, PgoutputWatermarkSource,
};
use crate::SourceOpts;

/// Default chunk size for ad-hoc snapshots during steady-state streaming.
pub const DEFAULT_STREAM_CHUNK_SIZE: usize = 1024;

/// Default `next_events` batch size in the replication tail loop.
pub const DEFAULT_WAL_EVENT_BATCH_SIZE: usize = 32;

/// Default sleep when a replication tail poll returns no events.
pub const DEFAULT_REPLICATION_TAIL_IDLE_SLEEP: Duration = Duration::from_millis(100);

/// Max filtered-only `next_events` batches before `poll_work` yields to the
/// runtime (drain / interval persist / ad-hoc), so catch-up through unrelated
/// WAL noise cannot block control-plane work forever.
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
    /// Optional exact WAL stop bound for the stream phase.
    pub until: Option<PgoutputCheckpoint>,
    pub checkpoint_interval: Duration,
    /// Rows read per keyset chunk when handling ad-hoc snapshot signals.
    pub chunk_size: usize,
    /// Max events requested per `next_events` call in the replication tail loop.
    pub event_batch_size: usize,
    /// Sleep when a poll returns no events before retrying.
    pub idle_sleep: Duration,
    /// Cooperative cancellation signal. When cancelled, the sync loop flushes a
    /// final resumable checkpoint and returns cleanly.
    pub cancel: tokio_util::sync::CancellationToken,
}

impl ReplicationTailOptions {
    pub fn stream(deadline: Option<DateTime<Utc>>, until: Option<PgoutputCheckpoint>) -> Self {
        Self {
            deadline,
            until,
            checkpoint_interval: Duration::from_secs(10),
            chunk_size: DEFAULT_STREAM_CHUNK_SIZE,
            event_batch_size: DEFAULT_WAL_EVENT_BATCH_SIZE,
            idle_sleep: DEFAULT_REPLICATION_TAIL_IDLE_SLEEP,
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }

    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
}

pub async fn run_replication_tail<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: PgoutputCheckpoint,
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
    from_checkpoint: PgoutputCheckpoint,
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
/// Incremental work runs via [`PgoutputSourceDriver`] +
/// [`sync_transform::run_source_runtime_with`]. Sink success still gates
/// [`SourceDriver::advance_watermark`](sync_transform::SourceDriver::advance_watermark)
/// (WAL client `commit`). CatchUpProgress uses
/// [`CheckpointPolicy::IntervalWhenDrained`]: sunk watermarks persist promptly
/// once the apply window drains; when fully drained with no unsunk work, the
/// same interval may advance the store to the **current** WAL LSN through
/// filtered/unrelated noise (never while transform/apply still holds buffered
/// or in-flight batches).
pub async fn run_replication_tail_with_transforms<S, St>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: PgoutputCheckpoint,
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
        "Starting PostgreSQL WAL incremental sync from checkpoint: {}",
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

    let sql = new_sql_client(&from_opts.connection_string).await?;
    let schema = resolve_schema(&from_opts).await;
    {
        let client = sql.lock().await;
        ensure_publication_for_source(&client, &from_opts, &schema).await?;
    }

    let db_schema = {
        let client = sql.lock().await;
        collect_postgresql_database_schema(&client).await?
    };

    let catch_up = if from_opts.tables.is_empty() {
        None
    } else if let Some(manager) = checkpoint_manager {
        read_catch_up_progress(manager).await?
    } else {
        None
    };
    let effective_tables = effective_sync_tables(&from_opts.tables, catch_up.as_ref());
    let table_filter = effective_tables.clone();

    let mut client = connect_wal_client(&from_opts)
        .await?
        .with_cancel(options.cancel.clone());
    client.ensure_replication_slot().await?;
    start_wal_from_checkpoint(&mut client, &from_checkpoint).await?;

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
    let mut driver = PgoutputSourceDriver {
        surreal,
        sql: sql.clone(),
        schema,
        from_opts,
        options: &options,
        checkpoint_manager,
        transforms,
        client: Some(client),
        db_schema,
        column_names_cache: HashMap::new(),
        prev_relations: HashMap::new(),
        table_filter,
        last_sunk_checkpoint: None,
        last_persisted_checkpoint: None,
        total_changes: 0,
        pending_adhoc_signals: Vec::new(),
        last_signal_check: None,
        ddl_refresh_pending: false,
        until_reached: false,
        cancel_seen: false,
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = sync_transform::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        sync_transform::RuntimeExit::Stopped(StopReason::Cancelled) => {
            info!("Cancellation requested, stopping incremental sync (checkpoint flushed)");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Deadline) => {
            info!("Deadline reached, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Until) => {
            info!("Reached target checkpoint, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Finished) => {
            info!("PostgreSQL WAL source finished");
        }
    }

    driver.finalize_store_checkpoint().await?;
    info!(
        "PostgreSQL WAL incremental sync completed: {} changes sunk",
        driver.total_changes
    );
    Ok(())
}

/// pgoutput CDC driver for [`sync_transform::run_source_runtime_with`].
struct PgoutputSourceDriver<'a, S, St: CheckpointStore> {
    surreal: &'a S,
    sql: std::sync::Arc<Mutex<tokio_postgres::Client>>,
    schema: String,
    from_opts: SourceOpts,
    options: &'a ReplicationTailOptions,
    checkpoint_manager: Option<&'a SyncManager<St>>,
    transforms: SnapshotTransforms,
    /// Taken temporarily while an ad-hoc watermark snapshot borrows the session.
    client: Option<PgWalClient>,
    db_schema: DatabaseSchema,
    column_names_cache: HashMap<String, Vec<String>>,
    prev_relations: HashMap<u32, RelationMeta>,
    table_filter: Option<Vec<String>>,
    last_sunk_checkpoint: Option<PgoutputCheckpoint>,
    last_persisted_checkpoint: Option<PgoutputCheckpoint>,
    /// Events successfully sunk (not merely buffered).
    total_changes: u64,
    pending_adhoc_signals: Vec<SnapshotSignal>,
    /// Last successful execute-snapshot signal table poll (`None` = never).
    last_signal_check: Option<std::time::Instant>,
    ddl_refresh_pending: bool,
    until_reached: bool,
    cancel_seen: bool,
}

impl<'a, S, St> PgoutputSourceDriver<'a, S, St>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    fn client_mut(&mut self) -> Result<&mut PgWalClient> {
        self.client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("WAL client unavailable during ad-hoc snapshot"))
    }

    fn client_ref(&self) -> Result<&PgWalClient> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("WAL client unavailable during ad-hoc snapshot"))
    }

    async fn finalize_store_checkpoint(&mut self) -> Result<()> {
        let client = self.client_ref()?;
        let current = get_current_checkpoint(client)?;
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
        let sql_client = self.sql.lock().await;
        self.db_schema = collect_postgresql_database_schema(&sql_client).await?;
        self.column_names_cache.clear();
        Ok(())
    }

    /// Convert WAL stream events into positioned changes. Relation renames and
    /// deferred DDL refresh (on Commit) update schema metadata inline so
    /// subsequent row events in the same batch see the new catalog.
    async fn consume_events(
        &mut self,
        events: Vec<StreamEvent>,
    ) -> Result<Vec<PositionedEvent<Lsn>>> {
        let mut out = Vec::new();
        for event in events {
            match event {
                StreamEvent::Relation(meta) => {
                    let mut renames = detect_relation_renames(&self.prev_relations, &meta);
                    if renames.is_empty() {
                        if let Some(rename) =
                            detect_relation_rename_from_filter(&self.table_filter, &meta)
                        {
                            renames.push(rename);
                        }
                    }
                    self.prev_relations.insert(meta.relation_oid, meta.clone());
                    if !renames.is_empty() {
                        apply_renames_to_filter(&mut self.table_filter, &renames);
                        info!(
                            "Refreshing PostgreSQL schema metadata after Relation rename for '{}'",
                            meta.table
                        );
                        self.refresh_schema_metadata().await?;
                    } else {
                        self.ddl_refresh_pending = true;
                    }
                }
                StreamEvent::Type {
                    schema: type_schema,
                    name,
                    ..
                } => {
                    if type_schema == self.schema {
                        self.ddl_refresh_pending = true;
                        info!(
                            "PostgreSQL Type event for '{}.{}'; schema refresh deferred to commit",
                            type_schema, name
                        );
                    }
                }
                StreamEvent::Truncate { tables } => {
                    if truncate_affects_filtered(&tables, &self.table_filter) {
                        warn!(
                            "TRUNCATE on synced table(s) {:?}; target rows were removed at source",
                            tables
                        );
                    }
                }
                StreamEvent::Change(change) => {
                    let rename_from_filter = self.table_filter.as_ref().and_then(|tables| {
                        if !tables.contains(&change.table) && tables.len() == 1 {
                            Some(crate::ddl::TableRename {
                                old: tables[0].clone(),
                                new: change.table.clone(),
                            })
                        } else {
                            None
                        }
                    });
                    if let Some(rename) = rename_from_filter {
                        apply_renames_to_filter(&mut self.table_filter, &[rename]);
                    }
                    let relation = {
                        let client = self.client_ref()?;
                        client
                            .relation_cache()
                            .get(&change.relation_oid)
                            .cloned()
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "pgoutput change for relation_oid {} has no preceding Relation; \
                                     refusing to silently drop the change",
                                    change.relation_oid
                                )
                            })?
                    };
                    if relation.schema != self.schema {
                        continue;
                    }
                    if let Some(ref tables) = self.table_filter {
                        if !tables.contains(&change.table) {
                            continue;
                        }
                    }

                    let position = {
                        let client = self.client_ref()?;
                        client.current_position()
                    };
                    if let Some(target) = self.options.until.as_ref() {
                        if position >= target.lsn {
                            self.until_reached = true;
                            // Do not apply the event at/past the until bound.
                            return Ok(out);
                        }
                    }

                    let column_names =
                        if let Some(names) = self.column_names_cache.get(&change.table) {
                            names.clone()
                        } else {
                            let sql_client = self.sql.lock().await;
                            let names = get_table_column_names_ordinal(
                                &sql_client,
                                &self.schema,
                                &change.table,
                            )
                            .await?;
                            self.column_names_cache
                                .insert(change.table.clone(), names.clone());
                            names
                        };

                    let universal = cdc_change_to_universal(
                        &change,
                        &relation,
                        &column_names,
                        &self.db_schema,
                    )?;
                    out.push(PositionedEvent::change(universal, position));
                }
                StreamEvent::Control => {}
                StreamEvent::Commit => {
                    if self.ddl_refresh_pending {
                        info!("Refreshing PostgreSQL schema metadata after DDL commit");
                        self.refresh_schema_metadata().await?;
                        self.ddl_refresh_pending = false;
                    }
                }
            }
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl<'a, S, St> SourceDriver for PgoutputSourceDriver<'a, S, St>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    type Position = Lsn;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() {
            return Ok(Vec::new());
        }

        // Keep reading while events are available but filtered out (other schemas,
        // non-row events). Returning empty every batch would re-enter
        // between_events (signal-table query) between every WAL event and
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
                        result.map_err(|e| anyhow::anyhow!("wal read failed: {e}"))?
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
            .ok_or_else(|| anyhow::anyhow!("WAL client unavailable during ad-hoc snapshot"))?;
        client.commit(position);
        self.last_sunk_checkpoint = Some(PgoutputCheckpoint {
            lsn: position,
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
            let pending = {
                let sql_client = self.sql.lock().await;
                read_pending_execute_snapshot_signals(&sql_client).await?
            };
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
            .ok_or_else(|| anyhow::anyhow!("WAL client missing for ad-hoc snapshot"))?;

        let mut wm = PgoutputWatermarkSource::wrap_active_wal_client(
            self.sql.clone(),
            self.schema.clone(),
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

            {
                let sql_client = self.sql.lock().await;
                acknowledge_execute_snapshot_signal(&sql_client, &signal.id).await?;
            }

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

        self.client = Some(wm.into_wal_client());
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
            if client.current_position() >= target.lsn {
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
        let current = get_current_checkpoint(client)?;
        let sunk = self.last_sunk_checkpoint.clone().or_else(|| {
            Some(PgoutputCheckpoint {
                lsn: position,
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
            debug!("Sunk {} WAL changes", self.total_changes);
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
    last_sunk: Option<&PgoutputCheckpoint>,
    current: &PgoutputCheckpoint,
    has_unsunk_work: bool,
) -> Option<PgoutputCheckpoint> {
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
    last_sunk: Option<&PgoutputCheckpoint>,
    current: &PgoutputCheckpoint,
    has_unsunk_work: bool,
    existing: Option<&CatchUpProgress>,
) -> Option<PgoutputCheckpoint> {
    store_checkpoint_to_emit(last_sunk, current, has_unsunk_work)
        .or_else(|| existing.map(|p| p.position.clone()))
}

fn truncate_affects_filtered(tables: &[String], filter: &Option<Vec<String>>) -> bool {
    match filter {
        None => !tables.is_empty(),
        Some(synced) => truncate_affects_synced(tables, synced),
    }
}

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
                "Relation rename '{}' -> '{}': tracking the renamed table under its new name",
                rename.old,
                rename.new
            );
        }
    }
}

async fn persist_checkpoint_at<St: CheckpointStore>(
    manager: Option<&SyncManager<St>>,
    checkpoint: Option<&PgoutputCheckpoint>,
    last: &mut Option<PgoutputCheckpoint>,
    force: bool,
) -> Result<()> {
    let Some(manager) = manager else {
        return Ok(());
    };
    let Some(checkpoint) = checkpoint else {
        return Ok(());
    };
    if !force && last.as_ref().is_some_and(|prev| prev.lsn == checkpoint.lsn) {
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

    fn ckpt(lsn: &str) -> PgoutputCheckpoint {
        PgoutputCheckpoint {
            lsn: Lsn::parse(lsn).unwrap(),
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn store_checkpoint_refuses_to_advance_past_unsunk_buffer() {
        let sunk = ckpt("0/100");
        let current = ckpt("0/200");

        // Buffered / in-flight / completed-waiting: persist last sunk only.
        let emitted = store_checkpoint_to_emit(Some(&sunk), &current, true).unwrap();
        assert_eq!(emitted.lsn, sunk.lsn);

        // No prior sink while buffered: skip persist entirely.
        assert!(store_checkpoint_to_emit(None, &current, true).is_none());

        // Nothing unsunk: current is safe (may include filtered-only advances).
        let emitted = store_checkpoint_to_emit(Some(&sunk), &current, false).unwrap();
        assert_eq!(emitted.lsn, current.lsn);
    }

    #[test]
    fn adhoc_catch_up_uses_last_sunk_when_unsunk_work_remains() {
        let sunk = ckpt("0/100");
        let current = ckpt("0/200");
        let existing = CatchUpProgress::new(ckpt("0/50"));

        // Unsung work + last_sunk → never write read-ahead current.
        let cp = adhoc_catch_up_checkpoint(Some(&sunk), &current, true, Some(&existing)).unwrap();
        assert_eq!(cp.lsn, sunk.lsn);

        // Unsung work, no last_sunk → keep existing progress position (coverage only).
        let cp = adhoc_catch_up_checkpoint(None, &current, true, Some(&existing)).unwrap();
        assert_eq!(cp, existing.position);

        // Unsung + no last_sunk + no existing → skip (never invent read-ahead current).
        assert!(adhoc_catch_up_checkpoint(None, &current, true, None).is_none());

        // Fully drained → current is safe (same as interval path).
        let cp = adhoc_catch_up_checkpoint(Some(&sunk), &current, false, Some(&existing)).unwrap();
        assert_eq!(cp.lsn, current.lsn);

        // Fully drained, no existing → current is still safe.
        let cp = adhoc_catch_up_checkpoint(None, &current, false, None).unwrap();
        assert_eq!(cp.lsn, current.lsn);
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
