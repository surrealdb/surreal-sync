//! SourceDriver control plane: poll/commit + schema/ad-hoc/cancel/checkpoint hooks.
//!
//! [`SourceDriver`] + [`run_source_runtime`] are the **general** incremental API.
//! [`crate::ChangeFeed`] / [`crate::run_change_feed`] are a convenience for
//! simple row-CDC sources (defaults cover the rest as no-ops).

use crate::apply::event::PositionedEvent;
use crate::apply::feed::ChangeFeed;
use crate::apply::opts::ApplyOpts;
use crate::apply::runtime::{
    apply_changes_with, apply_relation_changes_with, write_relations_with, write_rows_with,
    ApplyContext,
};
use crate::apply::transform::BatchTransformer;
use crate::pipeline::Pipeline;
use anyhow::{Context, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use surreal_sink::SurrealSink;
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow};
use tracing::debug;

/// Control-plane signal returned from [`SourceDriver::between_events`].
///
/// Handled by [`run_source_runtime`] via the corresponding `on_*` hooks before
/// the next poll. Defaults are no-ops so simple CDC drivers stay thin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlSignal {
    /// Refresh source schema / catalog (DDL observed, etc.).
    SchemaRefresh,
    /// Run an ad-hoc snapshot for the named tables (empty = driver-defined).
    AdHocSnapshot {
        /// Table names to snapshot; empty means “driver default set”.
        tables: Vec<String>,
    },
}

/// Why [`run_source_runtime`] should stop (checked each loop iteration).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// External cancellation (token, signal, operator stop).
    Cancelled,
    /// Wall-clock deadline reached.
    Deadline,
    /// Source-defined “until” bound reached (position / LSN / etc.).
    Until,
    /// Feed exhausted (`is_finished` after drain).
    Finished,
}

/// When to call [`SourceDriver::persist_checkpoint`].
///
/// Persistence is always **sink-safe only**: the runtime never asks a driver to
/// persist a position past unsunk transform/apply work. Drivers that map the
/// persist argument to a read-ahead cursor must only do so when the apply window
/// is drained (see [`IntervalWhenDrained`](Self::IntervalWhenDrained)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointPolicy {
    /// After each successful sink + `commit` watermark advance, also call
    /// [`SourceDriver::persist_checkpoint`] with that position.
    #[default]
    PersistAfterCommit,
    /// Do not call `persist_checkpoint` (driver folds durability into `commit`).
    CommitOnly,
    /// Persist sink-safe positions when the apply window is **fully drained**
    /// (no buffer / in-flight / completed-waiting):
    ///
    /// - After a `commit`, if the window is already drained, persist that
    ///   sink-safe watermark promptly (same durability cadence as persisting
    ///   last-sunk on sink success).
    /// - Otherwise arm a pending watermark and flush once the window drains
    ///   and at least `interval` has elapsed since the last persist.
    /// - When drained with **no** pending commit (filtered-only / idle read
    ///   progress), call [`SourceDriver::read_progress_for_persist`] on the
    ///   same interval so drivers can advance a store cursor through noise
    ///   without reintroducing unsunk read-ahead.
    ///
    /// `interval = Duration::ZERO` persists whenever the window is drained
    /// (still never advances past unsunk work). On stop flush, any pending
    /// sink-safe position is force-persisted.
    IntervalWhenDrained {
        /// Minimum time between deferred / filtered-only `persist_checkpoint` calls.
        interval: Duration,
    },
}

/// Why the runtime exited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeExit {
    /// Clean stop with the given reason.
    Stopped(StopReason),
}

/// Framework-injected apply helpers for [`SourceDriver::on_adhoc_snapshot`].
///
/// Drivers must use these (or the same sink + transformer + [`ApplyOpts`]) so
/// ad-hoc / snapshot writes honor transform and sink invariants — no private
/// durability bypass of the pipeline.
#[async_trait::async_trait]
pub trait AdhocApply: Send + Sync {
    /// Transform + `write_universal_rows` via the runtime’s pipeline and opts.
    async fn write_rows(&self, rows: Vec<UniversalRow>) -> Result<()>;

    /// Transform + `write_universal_relations`.
    async fn write_relations(&self, relations: Vec<UniversalRelation>) -> Result<()>;

    /// Transform + apply each row change.
    async fn apply_changes(&self, changes: Vec<UniversalChange>) -> Result<()>;

    /// Transform + apply each relation change.
    async fn apply_relation_changes(&self, changes: Vec<UniversalRelationChange>) -> Result<()>;

    /// Borrow the apply options used by the incremental loop.
    fn apply_opts(&self) -> &ApplyOpts;
}

struct AdhocApplyImpl<'a, S, T> {
    sink: &'a S,
    transformer: Arc<T>,
    apply_opts: &'a ApplyOpts,
}

#[async_trait::async_trait]
impl<'a, S, T> AdhocApply for AdhocApplyImpl<'a, S, T>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    async fn write_rows(&self, rows: Vec<UniversalRow>) -> Result<()> {
        write_rows_with(self.sink, Arc::clone(&self.transformer), rows, self.apply_opts).await
    }

    async fn write_relations(&self, relations: Vec<UniversalRelation>) -> Result<()> {
        write_relations_with(
            self.sink,
            Arc::clone(&self.transformer),
            relations,
            self.apply_opts,
        )
        .await
    }

    async fn apply_changes(&self, changes: Vec<UniversalChange>) -> Result<()> {
        apply_changes_with(
            self.sink,
            Arc::clone(&self.transformer),
            changes,
            self.apply_opts,
        )
        .await
    }

    async fn apply_relation_changes(&self, changes: Vec<UniversalRelationChange>) -> Result<()> {
        apply_relation_changes_with(
            self.sink,
            Arc::clone(&self.transformer),
            changes,
            self.apply_opts,
        )
        .await
    }

    fn apply_opts(&self) -> &ApplyOpts {
        self.apply_opts
    }
}

/// Source-facing incremental driver: work items + optional control-plane hooks.
///
/// Mirrors the spirit of [`interleaved_snapshot::WatermarkSource`]: the framework
/// owns the loop; the driver supplies poll/commit and optional extension points.
///
/// # Defaults
///
/// All hooks default to no-op / empty so a minimal CDC source only implements
/// [`poll_work`](Self::poll_work), [`commit`](Self::commit), and optionally
/// [`is_finished`](Self::is_finished).
#[async_trait::async_trait]
pub trait SourceDriver: Send {
    /// Checkpoint / resume position type.
    type Position: Clone + Send + Sync + 'static;

    /// Next work items (row and/or relation changes). May be empty on idle.
    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>>;

    /// Advance the source cursor after docs for this position were sunk.
    async fn commit(&mut self, position: Self::Position) -> Result<()>;

    /// Whether the driver will produce no more work (EOF).
    fn is_finished(&self) -> bool {
        false
    }

    /// Polled between apply cycles; return control signals to handle before the
    /// next [`poll_work`](Self::poll_work).
    async fn between_events(&mut self) -> Result<Vec<ControlSignal>> {
        Ok(Vec::new())
    }

    /// Handle [`ControlSignal::SchemaRefresh`]. Default: no-op.
    async fn on_schema_refresh(&mut self) -> Result<()> {
        Ok(())
    }

    /// Handle [`ControlSignal::AdHocSnapshot`].
    ///
    /// `apply` exposes the runtime’s sink + pipeline/transformer + [`ApplyOpts`]
    /// so drivers can run snapshot writes through the same transform/sink path
    /// as the incremental loop (no private durability bypass).
    ///
    /// Default: no-op.
    async fn on_adhoc_snapshot(
        &mut self,
        _tables: &[String],
        _apply: &dyn AdhocApply,
    ) -> Result<()> {
        Ok(())
    }

    /// If `Some`, the runtime stops after draining in-flight apply work.
    fn stop_reason(&self) -> Option<StopReason> {
        None
    }

    /// Checkpoint persistence policy. Default: [`CheckpointPolicy::PersistAfterCommit`].
    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::PersistAfterCommit
    }

    /// Persist a **sink-safe** checkpoint (called after successful sink +
    /// [`commit`](Self::commit) according to [`CheckpointPolicy`]).
    /// Default: no-op.
    async fn persist_checkpoint(&mut self, _position: Self::Position) -> Result<()> {
        Ok(())
    }

    /// Optional sink-safe position to persist when
    /// [`CheckpointPolicy::IntervalWhenDrained`] finds the apply window empty
    /// but nothing was committed since the last persist (e.g. filtered-only
    /// binlog traffic that advanced the read cursor with no work items).
    ///
    /// The runtime only consults this while fully drained, so returning the
    /// current read position is safe. Default: `None` (no filtered-only persist).
    async fn read_progress_for_persist(&mut self) -> Result<Option<Self::Position>> {
        Ok(None)
    }

    /// Notify the driver that `count` events are accounted for before
    /// [`commit`](Self::commit).
    ///
    /// Called after a successful sink apply, and also under
    /// [`FailurePolicy::Skip`](crate::FailurePolicy::Skip) for batches that are
    /// committed past without writing (so drivers that gate slot/cursor advance
    /// on sunk counts do not stall). Default: no-op.
    fn note_sunk_events(&mut self, _count: u64) {}
}

/// Adapter: any [`ChangeFeed`] is a [`SourceDriver`] with no-op control hooks.
pub struct ChangeFeedDriver<F> {
    /// Inner row-only feed.
    pub inner: F,
}

impl<F> ChangeFeedDriver<F> {
    /// Wrap a [`ChangeFeed`].
    pub fn new(inner: F) -> Self {
        Self { inner }
    }

    /// Borrow the inner feed.
    pub fn inner(&self) -> &F {
        &self.inner
    }

    /// Mutably borrow the inner feed.
    pub fn inner_mut(&mut self) -> &mut F {
        &mut self.inner
    }

    /// Unwrap the inner feed.
    pub fn into_inner(self) -> F {
        self.inner
    }
}

#[async_trait::async_trait]
impl<F> SourceDriver for ChangeFeedDriver<F>
where
    F: ChangeFeed,
{
    type Position = F::Position;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        let changes = self.inner.poll_changes().await?;
        Ok(changes.into_iter().map(PositionedEvent::from).collect())
    }

    async fn commit(&mut self, position: Self::Position) -> Result<()> {
        self.inner.commit(position).await
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

/// Borrowing adapter for [`run_change_feed`] (does not take ownership of the feed).
pub struct ChangeFeedRef<'a, F: ChangeFeed> {
    inner: &'a mut F,
}

impl<'a, F: ChangeFeed> ChangeFeedRef<'a, F> {
    /// Borrow a feed as a [`SourceDriver`].
    pub fn new(inner: &'a mut F) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<'a, F> SourceDriver for ChangeFeedRef<'a, F>
where
    F: ChangeFeed,
{
    type Position = F::Position;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        let changes = self.inner.poll_changes().await?;
        Ok(changes.into_iter().map(PositionedEvent::from).collect())
    }

    async fn commit(&mut self, position: Self::Position) -> Result<()> {
        self.inner.commit(position).await
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

/// Optional wall-clock / cancel bounds for [`run_source_runtime`].
///
/// Driver [`SourceDriver::stop_reason`] is also honored each iteration.
#[derive(Debug, Clone, Default)]
pub struct SourceRuntimeOpts {
    /// Stop with [`StopReason::Deadline`] once `Instant::now() >= deadline`.
    pub deadline: Option<Instant>,
    /// When true, stop with [`StopReason::Cancelled`] at the next check.
    pub cancelled: bool,
}

impl SourceRuntimeOpts {
    /// No extra bounds (driver `stop_reason` / `is_finished` only).
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: wall-clock deadline.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Builder: mark cancelled.
    pub fn with_cancelled(mut self, cancelled: bool) -> Self {
        self.cancelled = cancelled;
        self
    }
}

/// Framework-owned incremental loop over a [`SourceDriver`].
///
/// Order per batch: buffer → transform → ordered sink → `commit` → optional
/// `persist_checkpoint` (sink-safe only). Between cycles: `between_events` →
/// control hooks (ad-hoc receives [`AdhocApply`]). Stops on `is_finished`
/// (after drain), driver `stop_reason`, or [`SourceRuntimeOpts`] cancel/deadline.
pub async fn run_source_runtime<D, S>(
    driver: &mut D,
    sink: &S,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
    runtime_opts: &SourceRuntimeOpts,
) -> Result<RuntimeExit>
where
    D: SourceDriver,
    S: SurrealSink,
{
    run_source_runtime_with(
        driver,
        sink,
        Arc::new(pipeline.clone()),
        apply_opts,
        runtime_opts,
    )
    .await
}

/// Like [`run_source_runtime`] but accepts any [`BatchTransformer`] behind [`Arc`].
///
/// Pipeline overlap model (identity and non-identity share this path):
/// - **Reads** continue while prior batches transform or sink.
/// - **Transforms** overlap up to `max_in_flight` window occupancy (transforming +
///   awaiting/in-flight ordered sink).
/// - **Writes** stay ordered; the fill loop does not await sink before the next
///   poll/transform start. Source `commit` runs only after that batch’s sink
///   succeeds.
pub async fn run_source_runtime_with<D, S, T>(
    driver: &mut D,
    sink: &S,
    transformer: Arc<T>,
    apply_opts: &ApplyOpts,
    runtime_opts: &SourceRuntimeOpts,
) -> Result<RuntimeExit>
where
    D: SourceDriver,
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let mut ctx = ApplyContext::new(sink, Arc::clone(&transformer), apply_opts);
    // In-flight ordered sink work (overlaps with poll/transform). The drive
    // future is created once per batch so select! cancelling an await never
    // restarts apply from scratch (duplicate writes).
    let mut sinking: Option<PendingSink<'_, D::Position>> = None;

    loop {
        if let Some(reason) = effective_stop_reason(driver, runtime_opts) {
            finish_pending_sink(&mut ctx, driver, &mut sinking).await?;
            ctx.flush_for_driver(driver).await?;
            return Ok(RuntimeExit::Stopped(reason));
        }

        // Only drain an in-flight sink before control-plane work when there are
        // signals. Spare window capacity must keep polling while sink runs.
        let signals = driver.between_events().await.context("between_events")?;
        if !signals.is_empty() {
            finish_pending_sink(&mut ctx, driver, &mut sinking).await?;
            handle_control_signals(
                driver,
                &mut ctx,
                sink,
                &transformer,
                apply_opts,
                signals,
            )
            .await?;
        }

        // Fill transform window; opportunistically start ordered sink without
        // awaiting it so poll can continue while a slow sink is in flight.
        loop {
            if let Some(reason) = effective_stop_reason(driver, runtime_opts) {
                finish_pending_sink(&mut ctx, driver, &mut sinking).await?;
                ctx.flush_for_driver(driver).await?;
                return Ok(RuntimeExit::Stopped(reason));
            }

            ctx.poll_join_ready_public().await?;
            try_launch_sink(sink, &mut ctx, &mut sinking)?;

            if ctx.window_occupancy() >= apply_opts.max_in_flight {
                break;
            }

            // Pull work into the buffer. A single poll_work may return more than
            // batch_size events — that overshoot is intentional: we keep every
            // event in the buffer (never drop) and form transform batches from
            // it. Memory may exceed batch_size until the excess is drained.
            while ctx.buffer_len() < apply_opts.batch_size && !driver.is_finished() {
                let polled = driver.poll_work().await.context("poll_work")?;
                if polled.is_empty() {
                    break;
                }
                for pe in polled {
                    ctx.push_buffered_event(pe);
                }
            }

            let started = if ctx.buffer_len() >= apply_opts.batch_size {
                ctx.try_start_full_batch()
            } else if ctx.buffer_len() > 0
                && (driver.is_finished() || ctx.should_flush_partial_public())
            {
                ctx.try_start_partial_batch()
            } else {
                false
            };

            if !started {
                break;
            }
            // Collect instant (identity) completions and start sink if possible
            // without awaiting sink completion.
            ctx.poll_join_ready_public().await?;
            try_launch_sink(sink, &mut ctx, &mut sinking)?;
        }

        // Idle / progress wait: poll transforms and sink concurrently.
        let has_transform = ctx.in_flight_count() > 0;
        let has_sink = sinking.is_some();
        let has_buffer = ctx.buffer_len() > 0;
        let finished = driver.is_finished();

        if !has_transform && !has_sink && !has_buffer {
            if finished {
                ctx.flush_for_driver(driver).await?;
                return Ok(RuntimeExit::Stopped(StopReason::Finished));
            }
            if let Some(reason) = effective_stop_reason(driver, runtime_opts) {
                ctx.flush_for_driver(driver).await?;
                return Ok(RuntimeExit::Stopped(reason));
            }
            ctx.try_interval_persist_public(driver).await?;
            tokio::time::sleep(apply_opts.batch_max_wait.min(Duration::from_millis(10))).await;
            continue;
        }

        if !has_transform && !has_sink {
            // Buffered but waiting on batch_max_wait / occupancy.
            tokio::time::sleep(apply_opts.batch_max_wait.min(Duration::from_millis(10))).await;
            continue;
        }

        // Spare window capacity while sink is in flight: return to fill/poll
        // instead of parking solely on the sink future (reads must keep rising).
        if has_sink
            && ctx.window_occupancy() < apply_opts.max_in_flight
            && !finished
        {
            let idle = apply_opts.batch_max_wait.min(Duration::from_millis(10));
            if has_transform {
                // Concurrently progress transforms and sink, then refill.
                tokio::select! {
                    biased;
                    outcome = ctx.wait_one_completion_public() => {
                        outcome?;
                        try_launch_sink(sink, &mut ctx, &mut sinking)?;
                    }
                    result = poll_pending_sink(&mut sinking) => {
                        complete_pending_sink(&mut ctx, driver, &mut sinking, result).await?;
                        ctx.try_interval_persist_public(driver).await?;
                        try_launch_sink(sink, &mut ctx, &mut sinking)?;
                    }
                    _ = tokio::time::sleep(idle) => {}
                }
            } else {
                // No transform to wait on: race sink against a short idle so we
                // reopen the fill loop under spare capacity without busy-spinning
                // when poll_work is temporarily empty.
                tokio::select! {
                    result = poll_pending_sink(&mut sinking) => {
                        complete_pending_sink(&mut ctx, driver, &mut sinking, result).await?;
                        ctx.try_interval_persist_public(driver).await?;
                        try_launch_sink(sink, &mut ctx, &mut sinking)?;
                    }
                    _ = tokio::time::sleep(idle) => {}
                }
            }
            continue;
        }

        tokio::select! {
            biased;
            outcome = ctx.wait_one_completion_public(), if has_transform => {
                outcome?;
                try_launch_sink(sink, &mut ctx, &mut sinking)?;
            }
            // Poll the same once-created drive future in place. Dropping this
            // await (transform arm wins) must not recreate apply from scratch.
            result = poll_pending_sink(&mut sinking), if has_sink => {
                complete_pending_sink(&mut ctx, driver, &mut sinking, result).await?;
                ctx.try_interval_persist_public(driver).await?;
                try_launch_sink(sink, &mut ctx, &mut sinking)?;
            }
        }
    }
}

struct PendingSinkMeta<P> {
    batch_id: u64,
    last_position: P,
    event_count: u64,
    sunk: u64,
}

/// In-flight ordered sink batch. `drive` is started exactly once at launch.
struct PendingSink<'s, P> {
    meta: PendingSinkMeta<P>,
    drive: Pin<Box<dyn Future<Output = Result<SinkDrive>> + Send + 's>>,
}

fn try_launch_sink<'s, S, T, P>(
    sink: &'s S,
    ctx: &mut ApplyContext<'_, S, T, P>,
    sinking: &mut Option<PendingSink<'s, P>>,
) -> Result<()>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
    P: Clone + Send + Sync + 'static,
{
    if sinking.is_some() {
        return Ok(());
    }
    let Some(batch) = ctx.prepare_ordered_sink() else {
        return Ok(());
    };
    match batch.result {
        Ok(events) => {
            let sunk = events.len() as u64;
            let event_count = batch.event_count.max(sunk);
            let drive = Box::pin(async move {
                apply_sink_events_ref(sink, &events).await?;
                Ok(SinkDrive::Applied)
            });
            *sinking = Some(PendingSink {
                meta: PendingSinkMeta {
                    batch_id: batch.batch_id,
                    last_position: batch.last_position,
                    event_count,
                    sunk,
                },
                drive,
            });
        }
        Err(e) => {
            let drive = Box::pin(async move { Ok(SinkDrive::TransformFailed(e)) });
            *sinking = Some(PendingSink {
                meta: PendingSinkMeta {
                    batch_id: batch.batch_id,
                    last_position: batch.last_position,
                    event_count: batch.event_count,
                    sunk: 0,
                },
                drive,
            });
        }
    }
    Ok(())
}

enum SinkDrive {
    Applied,
    TransformFailed(anyhow::Error),
}

/// Await the in-flight drive future without taking ownership (select-safe).
async fn poll_pending_sink<'s, P>(
    sinking: &mut Option<PendingSink<'s, P>>,
) -> Result<SinkDrive> {
    match sinking.as_mut() {
        Some(pending) => pending.drive.as_mut().await,
        None => std::future::pending().await,
    }
}

async fn complete_pending_sink<D, S, T>(
    ctx: &mut ApplyContext<'_, S, T, D::Position>,
    driver: &mut D,
    sinking: &mut Option<PendingSink<'_, D::Position>>,
    result: Result<SinkDrive>,
) -> Result<()>
where
    D: SourceDriver,
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let pending = sinking.take().expect("sink slot");
    match result {
        Ok(SinkDrive::Applied) => {
            ctx.finish_sink_ok_driver(driver, pending.meta.last_position, pending.meta.sunk)
                .await
        }
        Ok(SinkDrive::TransformFailed(e)) => {
            ctx.finish_sink_err_driver(
                driver,
                pending.meta.batch_id,
                pending.meta.last_position,
                pending.meta.event_count,
                e,
            )
            .await
        }
        Err(e) => {
            ctx.finish_sink_err_driver(
                driver,
                pending.meta.batch_id,
                pending.meta.last_position,
                pending.meta.event_count,
                e,
            )
            .await
        }
    }
}

async fn finish_pending_sink<D, S, T>(
    ctx: &mut ApplyContext<'_, S, T, D::Position>,
    driver: &mut D,
    sinking: &mut Option<PendingSink<'_, D::Position>>,
) -> Result<()>
where
    D: SourceDriver,
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    if sinking.is_none() {
        return Ok(());
    }
    let result = poll_pending_sink(sinking).await;
    complete_pending_sink(ctx, driver, sinking, result).await
}

async fn apply_sink_events_ref<S: SurrealSink>(
    sink: &S,
    events: &[crate::ApplyEvent],
) -> Result<()> {
    for event in events {
        match event {
            crate::ApplyEvent::Change(change) => {
                sink.apply_universal_change(change)
                    .await
                    .context("sink apply_universal_change")?;
            }
            crate::ApplyEvent::RelationChange(change) => {
                sink.apply_universal_relation_change(change)
                    .await
                    .context("sink apply_universal_relation_change")?;
            }
        }
    }
    Ok(())
}

fn effective_stop_reason<D: SourceDriver>(
    driver: &D,
    runtime_opts: &SourceRuntimeOpts,
) -> Option<StopReason> {
    if runtime_opts.cancelled {
        return Some(StopReason::Cancelled);
    }
    if let Some(deadline) = runtime_opts.deadline {
        if Instant::now() >= deadline {
            return Some(StopReason::Deadline);
        }
    }
    driver.stop_reason()
}

async fn handle_control_signals<D, S, T>(
    driver: &mut D,
    ctx: &mut ApplyContext<'_, S, T, D::Position>,
    sink: &S,
    transformer: &Arc<T>,
    apply_opts: &ApplyOpts,
    signals: Vec<ControlSignal>,
) -> Result<()>
where
    D: SourceDriver,
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    // Drain the apply window before control-plane side work so CatchUpProgress /
    // ad-hoc snapshots never observe read-ahead positions past unsunk batches.
    ctx.flush_for_driver(driver).await?;
    for signal in signals {
        match signal {
            ControlSignal::SchemaRefresh => {
                debug!("SourceDriver schema refresh");
                driver
                    .on_schema_refresh()
                    .await
                    .context("on_schema_refresh")?;
            }
            ControlSignal::AdHocSnapshot { tables } => {
                debug!(?tables, "SourceDriver ad-hoc snapshot");
                let apply = AdhocApplyImpl {
                    sink,
                    transformer: Arc::clone(transformer),
                    apply_opts,
                };
                driver
                    .on_adhoc_snapshot(&tables, &apply)
                    .await
                    .context("on_adhoc_snapshot")?;
            }
        }
    }
    Ok(())
}
