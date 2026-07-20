//! SourceDriver control plane: poll/commit + schema/ad-hoc/cancel/checkpoint hooks.
//!
//! [`SourceDriver`] + [`run_source_runtime`] are the **general** incremental API.
//! [`crate::ChangeFeed`] / [`crate::run_change_feed`] are a convenience for
//! simple row-CDC sources (defaults cover the rest as no-ops).

use crate::apply::event::PositionedEvent;
use crate::apply::feed::ChangeFeed;
use crate::apply::opts::ApplyOpts;
use crate::apply::runtime::ApplyContext;
use crate::apply::transform::BatchTransformer;
use crate::pipeline::Pipeline;
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use surreal_sink::SurrealSink;
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
/// Persistence is always **sink-safe only**: the runtime never passes a position
/// that has not been successfully sunk (and committed via [`SourceDriver::commit`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointPolicy {
    /// After each successful sink + `commit` watermark advance, also call
    /// [`SourceDriver::persist_checkpoint`] with that position.
    #[default]
    PersistAfterCommit,
    /// Do not call `persist_checkpoint` (driver folds durability into `commit`).
    CommitOnly,
}

/// Why the runtime exited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeExit {
    /// Clean stop with the given reason.
    Stopped(StopReason),
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

    /// Handle [`ControlSignal::AdHocSnapshot`]. Default: no-op.
    async fn on_adhoc_snapshot(&mut self, _tables: &[String]) -> Result<()> {
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

    /// Persist a **sink-safe** checkpoint (called only after successful sink +
    /// [`commit`](Self::commit) when policy is [`CheckpointPolicy::PersistAfterCommit`]).
    /// Default: no-op.
    async fn persist_checkpoint(&mut self, _position: Self::Position) -> Result<()> {
        Ok(())
    }
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
/// control hooks. Stops on `is_finished` (after drain), driver `stop_reason`,
/// or [`SourceRuntimeOpts`] cancel/deadline.
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
    let mut ctx = ApplyContext::new(sink, transformer, apply_opts);
    loop {
        if let Some(reason) = effective_stop_reason(driver, runtime_opts) {
            ctx.flush_for_driver(driver).await?;
            return Ok(RuntimeExit::Stopped(reason));
        }

        handle_control_signals(driver).await?;

        // Fill the transform window to capacity before waiting on completions.
        while ctx.in_flight_count() < apply_opts.max_in_flight {
            if let Some(reason) = effective_stop_reason(driver, runtime_opts) {
                ctx.flush_for_driver(driver).await?;
                return Ok(RuntimeExit::Stopped(reason));
            }

            while ctx.buffer_len() < apply_opts.batch_size && !driver.is_finished() {
                let polled = driver.poll_work().await.context("poll_work")?;
                if polled.is_empty() {
                    break;
                }
                for pe in polled {
                    ctx.push_buffered_event(pe);
                }
            }

            if ctx.buffer_len() >= apply_opts.batch_size {
                if !ctx.try_start_full_batch() {
                    break;
                }
                ctx.drain_ordered_driver(driver).await?;
                continue;
            }

            if ctx.buffer_len() > 0 && (driver.is_finished() || ctx.should_flush_partial_public()) {
                if !ctx.try_start_partial_batch() {
                    break;
                }
                ctx.drain_ordered_driver(driver).await?;
                continue;
            }

            break;
        }

        ctx.drain_ordered_driver(driver).await?;

        if ctx.in_flight_count() == 0 && ctx.buffer_len() == 0 {
            if driver.is_finished() {
                return Ok(RuntimeExit::Stopped(StopReason::Finished));
            }
            if let Some(reason) = effective_stop_reason(driver, runtime_opts) {
                return Ok(RuntimeExit::Stopped(reason));
            }
            tokio::time::sleep(apply_opts.batch_max_wait.min(Duration::from_millis(10))).await;
            continue;
        }

        if ctx.in_flight_count() == 0 {
            tokio::time::sleep(apply_opts.batch_max_wait).await;
            continue;
        }

        ctx.wait_one_completion().await?;
        ctx.drain_ordered_driver(driver).await?;
    }
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

async fn handle_control_signals<D: SourceDriver>(driver: &mut D) -> Result<()> {
    let signals = driver.between_events().await.context("between_events")?;
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
                driver
                    .on_adhoc_snapshot(&tables)
                    .await
                    .context("on_adhoc_snapshot")?;
            }
        }
    }
    Ok(())
}
