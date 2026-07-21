//! Unified in-flight window runtime: transform → ordered sink → contiguous commit.
//!
//! Handles row changes **and** relation changes through the same window,
//! ordered sink, sink-safe positions, and flush/discard/poison invariants.
//!
//! The general incremental driver API is [`crate::SourceDriver`] /
//! [`crate::run_source_runtime`]. [`run_change_feed`] remains a convenience for
//! row-only feeds.

use crate::apply::{
    ApplyEvent, ChangeFeed, ChangeFeedRef, CheckpointPolicy, FailurePolicy, PositionedChange,
    PositionedEvent, RuntimeExit, SourceDriver, SourceRuntimeOpts,
};
use crate::apply::opts::ApplyOpts;
use crate::apply::transform::BatchTransformer;
use crate::pipeline::Pipeline;
use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use surreal_sink::SurrealSink;
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow};
use tokio::task::JoinSet;
use tracing::warn;

/// Transform then `write_universal_rows`. Shared by full sync and snapshot flushes.
///
/// Gates on [`BatchTransformer::is_identity`] so an empty pipeline is a pure
/// move into the sink with no transform dispatch or timeout wrapper.
pub async fn write_rows<S: SurrealSink>(
    sink: &S,
    pipeline: &Pipeline,
    rows: Vec<UniversalRow>,
    opts: &ApplyOpts,
) -> Result<()> {
    write_rows_with(sink, Arc::new(pipeline.clone()), rows, opts).await
}

/// Transform then `write_universal_relations`.
pub async fn write_relations<S: SurrealSink>(
    sink: &S,
    pipeline: &Pipeline,
    relations: Vec<UniversalRelation>,
    opts: &ApplyOpts,
) -> Result<()> {
    write_relations_with(sink, Arc::new(pipeline.clone()), relations, opts).await
}

/// Transform then apply each change via [`SurrealSink::apply_universal_change`].
pub async fn apply_changes<S: SurrealSink>(
    sink: &S,
    pipeline: &Pipeline,
    changes: Vec<UniversalChange>,
    opts: &ApplyOpts,
) -> Result<()> {
    apply_changes_with(sink, Arc::new(pipeline.clone()), changes, opts).await
}

/// Transform then apply each relation change via
/// [`SurrealSink::apply_universal_relation_change`].
pub async fn apply_relation_changes<S: SurrealSink>(
    sink: &S,
    pipeline: &Pipeline,
    changes: Vec<UniversalRelationChange>,
    opts: &ApplyOpts,
) -> Result<()> {
    apply_relation_changes_with(sink, Arc::new(pipeline.clone()), changes, opts).await
}

/// Like [`apply_changes`] but accepts any [`BatchTransformer`] behind [`Arc`].
pub async fn apply_changes_with<S, T>(
    sink: &S,
    transformer: Arc<T>,
    changes: Vec<UniversalChange>,
    opts: &ApplyOpts,
) -> Result<()>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let changes = if transformer.is_identity() {
        changes
    } else {
        tokio::time::timeout(opts.timeout, transformer.transform_changes(0, changes))
            .await
            .map_err(|_| anyhow!("transform timeout after {:?}", opts.timeout))?
            .context("transform changes")?
    };
    for change in &changes {
        sink.apply_universal_change(change)
            .await
            .context("sink apply_universal_change")?;
    }
    Ok(())
}

/// Like [`apply_relation_changes`] but accepts any [`BatchTransformer`] behind [`Arc`].
pub async fn apply_relation_changes_with<S, T>(
    sink: &S,
    transformer: Arc<T>,
    changes: Vec<UniversalRelationChange>,
    opts: &ApplyOpts,
) -> Result<()>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let changes = if transformer.is_identity() {
        changes
    } else {
        tokio::time::timeout(
            opts.timeout,
            transformer.transform_relation_changes(0, changes),
        )
        .await
        .map_err(|_| anyhow!("transform timeout after {:?}", opts.timeout))?
        .context("transform relation changes")?
    };
    for change in &changes {
        sink.apply_universal_relation_change(change)
            .await
            .context("sink apply_universal_relation_change")?;
    }
    Ok(())
}

/// Like [`write_rows`] but accepts any [`BatchTransformer`] behind [`Arc`].
pub async fn write_rows_with<S, T>(
    sink: &S,
    transformer: Arc<T>,
    rows: Vec<UniversalRow>,
    opts: &ApplyOpts,
) -> Result<()>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let rows = if transformer.is_identity() {
        rows
    } else {
        tokio::time::timeout(opts.timeout, transformer.transform_rows(0, rows))
            .await
            .map_err(|_| anyhow!("transform timeout after {:?}", opts.timeout))?
            .context("transform rows")?
    };
    sink.write_universal_rows(&rows)
        .await
        .context("sink write_universal_rows")?;
    Ok(())
}

/// Like [`write_relations`] but accepts any [`BatchTransformer`] behind [`Arc`].
pub async fn write_relations_with<S, T>(
    sink: &S,
    transformer: Arc<T>,
    relations: Vec<UniversalRelation>,
    opts: &ApplyOpts,
) -> Result<()>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let relations = if transformer.is_identity() {
        relations
    } else {
        tokio::time::timeout(opts.timeout, transformer.transform_relations(0, relations))
            .await
            .map_err(|_| anyhow!("transform timeout after {:?}", opts.timeout))?
            .context("transform relations")?
    };
    sink.write_universal_relations(&relations)
        .await
        .context("sink write_universal_relations")?;
    Ok(())
}

/// Convenience loop for row-only [`ChangeFeed`] sources.
///
/// Equivalent to [`crate::run_source_runtime`] over a [`ChangeFeedDriver`].
/// Prefer [`crate::SourceDriver`] when you need relation events or control-plane
/// hooks (schema refresh, ad-hoc snapshot, cancel/deadline, persist_checkpoint).
pub async fn run_change_feed<F, S>(
    feed: &mut F,
    sink: &S,
    pipeline: &Pipeline,
    opts: &ApplyOpts,
) -> Result<()>
where
    F: ChangeFeed,
    S: SurrealSink,
{
    run_change_feed_with(feed, sink, Arc::new(pipeline.clone()), opts).await
}

/// Like [`run_change_feed`] but accepts any [`BatchTransformer`] behind [`Arc`].
pub async fn run_change_feed_with<F, S, T>(
    feed: &mut F,
    sink: &S,
    transformer: Arc<T>,
    opts: &ApplyOpts,
) -> Result<()>
where
    F: ChangeFeed,
    S: SurrealSink,
    T: BatchTransformer + 'static,
{
    let mut driver = ChangeFeedRef::new(feed);
    let exit = crate::apply::source_driver::run_source_runtime_with(
        &mut driver,
        sink,
        transformer,
        opts,
        &SourceRuntimeOpts::default(),
    )
    .await?;
    match exit {
        RuntimeExit::Stopped(_) => Ok(()),
    }
}

struct InFlightMeta<P> {
    #[allow(dead_code)]
    batch_id: u64,
    #[allow(dead_code)]
    last_position: P,
}

struct CompletedBatch<P> {
    #[allow(dead_code)]
    batch_id: u64,
    last_position: P,
    /// Events in this batch (input count; for `Ok` prefer `events.len()`).
    event_count: u64,
    result: Result<Vec<ApplyEvent>>,
}

struct TransformOutcome<P> {
    epoch: u64,
    seq: u64,
    batch_id: u64,
    last_position: P,
    event_count: u64,
    result: Result<Vec<ApplyEvent>>,
}

/// Batch ready for ordered sink apply (transform done; sink slot reserved).
pub(crate) struct PreparedSinkBatch<P> {
    pub(crate) batch_id: u64,
    pub(crate) last_position: P,
    pub(crate) event_count: u64,
    pub(crate) result: Result<Vec<ApplyEvent>>,
}

/// Library / custom-loop driver sharing the same ordered apply path as
/// [`crate::run_source_runtime`].
///
/// Accepts row [`UniversalChange`] and [`UniversalRelationChange`] into one
/// buffer / window. Identity and non-identity pipelines share one path: every
/// batch is spawned onto the transform [`JoinSet`] (identity is an async
/// no-op). Sink apply is ordered and may overlap with polling / transforming
/// later batches; `push_*` / `flush` still return `Some(position)` only after
/// that batch’s sink succeeds. They do **not** call
/// [`SourceDriver::commit`] / [`ChangeFeed::commit`].
///
/// # Poisoning after [`FailurePolicy::Fail`]
///
/// After a batch fails under [`FailurePolicy::Fail`], this context is
/// **poisoned**: successors are discarded, and further push/flush return `Err`
/// immediately. Do not reuse — create a new context and replay from the last
/// successful commit watermark.
pub struct ApplyContext<'a, S, T, P = ()> {
    sink: &'a S,
    transformer: Arc<T>,
    opts: &'a ApplyOpts,
    buffer: Vec<PositionedEvent<P>>,
    next_batch_id: u64,
    next_seq: u64,
    next_to_apply: u64,
    in_flight: HashMap<u64, InFlightMeta<P>>,
    completed: HashMap<u64, CompletedBatch<P>>,
    join_set: JoinSet<TransformOutcome<P>>,
    /// True while a prepared sink batch is being applied (slot reserved).
    sink_in_flight: bool,
    buffer_started: Option<tokio::time::Instant>,
    /// Bumped on fail-discard so stale JoinSet tasks are ignored.
    epoch: u64,
    /// Set after [`FailurePolicy::Fail`]; context must not be reused.
    poisoned: bool,
    /// Events successfully sunk since the last [`Self::take_sunk_change_count`].
    sunk_since_take: u64,
    /// Deferred sink-safe position for [`CheckpointPolicy::IntervalWhenDrained`].
    pending_checkpoint: Option<P>,
    /// Last wall-clock time a deferred checkpoint was persisted.
    last_checkpoint_persist: Instant,
}

impl<'a, S, T, P> ApplyContext<'a, S, T, P>
where
    S: SurrealSink,
    T: BatchTransformer + 'static,
    P: Clone + Send + Sync + 'static,
{
    /// Create an apply context bound to a sink, transformer, and options.
    pub fn new(sink: &'a S, transformer: Arc<T>, opts: &'a ApplyOpts) -> Self {
        Self {
            sink,
            transformer,
            opts,
            buffer: Vec::new(),
            next_batch_id: 1,
            next_seq: 0,
            next_to_apply: 0,
            in_flight: HashMap::new(),
            completed: HashMap::new(),
            join_set: JoinSet::new(),
            sink_in_flight: false,
            buffer_started: None,
            epoch: 0,
            poisoned: false,
            sunk_since_take: 0,
            pending_checkpoint: None,
            last_checkpoint_persist: Instant::now(),
        }
    }

    /// Whether this context was poisoned by a [`FailurePolicy::Fail`] error.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Number of events waiting to form a batch.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Number of batches currently transforming (JoinSet path).
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Whether an ordered sink apply is in progress (slot reserved).
    pub fn sink_in_flight(&self) -> bool {
        self.sink_in_flight
    }

    /// Batches occupying the apply window: transforming + awaiting/in sink.
    ///
    /// [`ApplyOpts::max_in_flight`] bounds this total so a slow sink back-pressures
    /// new transforms while still allowing overlap (reads / transforms / writes).
    pub fn window_occupancy(&self) -> usize {
        self.in_flight.len()
            + self.completed.len()
            + usize::from(self.sink_in_flight)
    }

    /// Take and reset the count of events sunk since the previous take.
    pub fn take_sunk_change_count(&mut self) -> u64 {
        std::mem::take(&mut self.sunk_since_take)
    }

    /// Number of transform results waiting for ordered sink apply.
    pub fn completed_waiting_count(&self) -> usize {
        self.completed.len()
    }

    /// Whether any buffered, in-flight, completed-waiting, or sinking work remains.
    ///
    /// Used by checkpoint policies that must not persist a read-ahead position
    /// while transform/apply still has unsunk work.
    pub fn has_unsunk_work(&self) -> bool {
        self.buffer_len() > 0
            || self.in_flight_count() > 0
            || self.completed_waiting_count() > 0
            || self.sink_in_flight
    }

    /// Whether the apply window is fully drained (no unsunk work).
    pub fn is_fully_drained(&self) -> bool {
        !self.has_unsunk_work()
    }

    fn ensure_not_poisoned(&self) -> Result<()> {
        if self.poisoned {
            bail!(
                "ApplyContext is poisoned after FailurePolicy::Fail; \
                 create a new context and replay from the last successful commit"
            );
        }
        Ok(())
    }

    pub(crate) fn push_buffered_event(&mut self, pe: PositionedEvent<P>) {
        if self.buffer.is_empty() {
            self.buffer_started = Some(tokio::time::Instant::now());
        }
        self.buffer.push(pe);
    }

    pub(crate) fn should_flush_partial_public(&self) -> bool {
        self.should_flush_partial()
    }

    fn should_flush_partial(&self) -> bool {
        match self.buffer_started {
            Some(started) => started.elapsed() >= self.opts.batch_max_wait,
            None => false,
        }
    }

    /// Push one row change; may start transforms and drain ordered sink.
    ///
    /// Returns the last position successfully sunk (caller should `commit`).
    pub async fn push_change(
        &mut self,
        change: UniversalChange,
        position: P,
    ) -> Result<Option<P>> {
        self.ensure_not_poisoned()?;
        self.push_buffered_event(PositionedEvent::change(change, position));
        while self.try_start_full_batch() {}
        self.collect_ready_transforms().await?;
        self.drain_ordered_no_commit().await
    }

    /// Push one relation change into the same window as row changes.
    pub async fn push_relation_change(
        &mut self,
        change: UniversalRelationChange,
        position: P,
    ) -> Result<Option<P>> {
        self.ensure_not_poisoned()?;
        self.push_buffered_event(PositionedEvent::relation_change(change, position));
        while self.try_start_full_batch() {}
        self.collect_ready_transforms().await?;
        self.drain_ordered_no_commit().await
    }

    /// Push a unified positioned event.
    pub async fn push_event(&mut self, event: ApplyEvent, position: P) -> Result<Option<P>> {
        self.ensure_not_poisoned()?;
        self.push_buffered_event(PositionedEvent::new(event, position));
        while self.try_start_full_batch() {}
        self.collect_ready_transforms().await?;
        self.drain_ordered_no_commit().await
    }

    /// Collect JoinSet results that are ready. Identity batches are async no-ops
    /// and usually complete after a yield; slow transforms stay in-flight.
    async fn collect_ready_transforms(&mut self) -> Result<()> {
        if self.transformer.is_identity() {
            while self.in_flight_count() > 0 {
                self.wait_one_completion().await?;
            }
            return Ok(());
        }
        self.poll_join_ready().await
    }

    /// Flush remaining buffered events and wait for in-flight work.
    pub async fn flush(&mut self) -> Result<Option<P>> {
        self.ensure_not_poisoned()?;
        let mut last = None;
        loop {
            while self.try_start_partial_batch() {}

            if let Some(p) = self.drain_ordered_no_commit().await? {
                last = Some(p);
            }

            if self.buffer.is_empty()
                && self.in_flight.is_empty()
                && self.completed.is_empty()
                && !self.sink_in_flight
            {
                break;
            }

            if self.in_flight_count() > 0 {
                self.wait_one_completion().await?;
                continue;
            }

            if !self.buffer.is_empty() {
                if !self.try_start_partial_batch() {
                    bail!(
                        "flush: {} buffered event(s) but window would not accept a batch",
                        self.buffer.len()
                    );
                }
                continue;
            }

            bail!(
                "flush: {} completed batch(es) waiting but none is next_to_apply={}",
                self.completed.len(),
                self.next_to_apply
            );
        }
        Ok(last)
    }

    /// Transform then sink rows (same as [`write_rows_with`]).
    pub async fn write_rows(&self, rows: Vec<UniversalRow>) -> Result<()> {
        self.ensure_not_poisoned()?;
        write_rows_with(self.sink, Arc::clone(&self.transformer), rows, self.opts).await
    }

    /// Transform then sink relations (same as [`write_relations_with`]).
    pub async fn write_relations(&self, relations: Vec<UniversalRelation>) -> Result<()> {
        self.ensure_not_poisoned()?;
        write_relations_with(
            self.sink,
            Arc::clone(&self.transformer),
            relations,
            self.opts,
        )
        .await
    }

    pub(crate) fn try_start_full_batch(&mut self) -> bool {
        if self.buffer.len() < self.opts.batch_size {
            return false;
        }
        self.start_batch_from_buffer(self.opts.batch_size)
    }

    pub(crate) fn try_start_partial_batch(&mut self) -> bool {
        if self.buffer.is_empty() {
            return false;
        }
        let n = self.buffer.len();
        self.start_batch_from_buffer(n)
    }

    fn start_batch_from_buffer(&mut self, n: usize) -> bool {
        if n == 0 {
            return false;
        }
        // One window for identity and transforms: occupancy includes transforming,
        // completed-waiting, and the in-flight ordered sink slot.
        if self.window_occupancy() >= self.opts.max_in_flight {
            return false;
        }

        let batch: Vec<PositionedEvent<P>> = self.buffer.drain(..n).collect();
        if self.buffer.is_empty() {
            self.buffer_started = None;
        } else {
            self.buffer_started = Some(tokio::time::Instant::now());
        }

        let last_position = batch.last().expect("n > 0").position.clone();
        let events: Vec<ApplyEvent> = batch.into_iter().map(|pe| pe.event).collect();
        let event_count = events.len() as u64;

        let batch_id = self.next_batch_id;
        self.next_batch_id = self.next_batch_id.saturating_add(1);
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        self.in_flight.insert(
            seq,
            InFlightMeta {
                batch_id,
                last_position: last_position.clone(),
            },
        );

        let transformer = Arc::clone(&self.transformer);
        let timeout = self.opts.timeout;
        let epoch = self.epoch;
        // Identity uses the same JoinSet path (async no-op) so poll/transform/sink
        // can overlap; do not sync-insert into `completed` (that serialized the window).
        let identity = transformer.is_identity();
        self.join_set.spawn(async move {
            let result = if identity {
                Ok(events)
            } else {
                match tokio::time::timeout(
                    timeout,
                    transformer.transform_events(batch_id, events),
                )
                .await
                {
                    Ok(inner) => inner,
                    Err(_) => Err(anyhow!(
                        "transform timeout after {:?} for batch_id={batch_id}",
                        timeout
                    )),
                }
            };
            TransformOutcome {
                epoch,
                seq,
                batch_id,
                last_position,
                event_count,
                result,
            }
        });
        true
    }

    pub(crate) async fn poll_join_ready_public(&mut self) -> Result<()> {
        self.poll_join_ready().await
    }

    pub(crate) async fn wait_one_completion_public(&mut self) -> Result<()> {
        self.wait_one_completion().await
    }

    pub(crate) async fn try_interval_persist_public(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
    ) -> Result<()> {
        self.try_interval_persist(driver).await
    }

    fn try_poll_join(&mut self) -> Option<Result<TransformOutcome<P>>> {
        let handle = self.join_set.try_join_next()?;
        Some(match handle {
            Ok(outcome) => Ok(outcome),
            Err(e) => Err(anyhow!("transform task join error: {e}")),
        })
    }

    async fn poll_join_ready(&mut self) -> Result<()> {
        while let Some(outcome) = self.try_poll_join() {
            self.handle_outcome(outcome?)?;
        }
        if self.in_flight_count() > 0 {
            tokio::task::yield_now().await;
            while let Some(outcome) = self.try_poll_join() {
                self.handle_outcome(outcome?)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn wait_one_completion(&mut self) -> Result<()> {
        let outcome = self
            .join_set
            .join_next()
            .await
            .ok_or_else(|| anyhow!("no in-flight transform tasks"))?
            .map_err(|e| anyhow!("transform task join error: {e}"))?;
        self.handle_outcome(outcome)
    }

    fn handle_outcome(&mut self, outcome: TransformOutcome<P>) -> Result<()> {
        if outcome.epoch != self.epoch {
            return Ok(());
        }
        if self.in_flight.remove(&outcome.seq).is_none() {
            return Ok(());
        }
        self.completed.insert(
            outcome.seq,
            CompletedBatch {
                batch_id: outcome.batch_id,
                last_position: outcome.last_position,
                event_count: outcome.event_count,
                result: outcome.result,
            },
        );
        Ok(())
    }

    /// Reserve the next ordered batch for sink apply (non-blocking).
    ///
    /// Returns `None` when the sink slot is busy or the next seq is not ready.
    /// Caller must apply (or skip) then call [`Self::finish_sink_driver`] /
    /// [`Self::finish_sink_no_commit`] (or [`Self::release_sink_slot`] on abandon).
    pub(crate) fn prepare_ordered_sink(&mut self) -> Option<PreparedSinkBatch<P>> {
        if self.sink_in_flight {
            return None;
        }
        let batch = self.completed.remove(&self.next_to_apply)?;
        self.sink_in_flight = true;
        Some(PreparedSinkBatch {
            batch_id: batch.batch_id,
            last_position: batch.last_position,
            event_count: batch.event_count,
            result: batch.result,
        })
    }

    /// After a successful sink apply: account, commit, persist, advance.
    pub(crate) async fn finish_sink_ok_driver(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
        last_position: P,
        sunk: u64,
    ) -> Result<()> {
        self.sunk_since_take = self.sunk_since_take.saturating_add(sunk);
        driver.note_sunk_events(sunk);
        driver
            .commit(last_position.clone())
            .await
            .context("commit")?;
        // Free the sink slot before persist checks so IntervalWhenDrained sees a
        // drained window when nothing else is outstanding.
        self.next_to_apply += 1;
        self.sink_in_flight = false;
        self.after_commit_persist(driver, last_position).await?;
        Ok(())
    }

    /// After sink/transform failure under driver path.
    pub(crate) async fn finish_sink_err_driver(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
        batch_id: u64,
        last_position: P,
        event_count: u64,
        e: anyhow::Error,
    ) -> Result<()> {
        self.sink_in_flight = false;
        self.fail_or_skip_driver(driver, batch_id, last_position, event_count, e)
            .await
    }

    /// After successful sink for push/flush (no driver commit).
    pub(crate) fn finish_sink_ok_no_commit(&mut self, last_position: P, sunk: u64) -> P {
        self.sunk_since_take = self.sunk_since_take.saturating_add(sunk);
        self.next_to_apply += 1;
        self.sink_in_flight = false;
        last_position
    }

    /// After sink/transform failure for push/flush.
    pub(crate) fn finish_sink_err_no_commit(
        &mut self,
        batch_id: u64,
        last_position: P,
        e: anyhow::Error,
    ) -> Result<Option<P>> {
        self.sink_in_flight = false;
        self.fail_or_skip_no_feed(batch_id, last_position, e)
    }

    /// Drain ordered sink + driver commit (+ optional persist_checkpoint).
    ///
    /// Awaits each sink apply to completion (used by flush / control-plane).
    pub(crate) async fn drain_ordered_driver(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
    ) -> Result<()> {
        loop {
            // Collect ready transform results without blocking.
            self.poll_join_ready().await?;
            let Some(batch) = self.prepare_ordered_sink() else {
                break;
            };
            match batch.result {
                Ok(events) => match self.apply_sink_events(&events).await {
                    Ok(()) => {
                        self.finish_sink_ok_driver(
                            driver,
                            batch.last_position,
                            events.len() as u64,
                        )
                        .await?;
                    }
                    Err(e) => {
                        self.finish_sink_err_driver(
                            driver,
                            batch.batch_id,
                            batch.last_position,
                            batch.event_count.max(events.len() as u64),
                            e,
                        )
                        .await?;
                    }
                },
                Err(e) => {
                    self.finish_sink_err_driver(
                        driver,
                        batch.batch_id,
                        batch.last_position,
                        batch.event_count,
                        e,
                    )
                    .await?;
                }
            }
        }
        self.try_interval_persist(driver).await
    }

    async fn after_commit_persist(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
        position: P,
    ) -> Result<()> {
        match driver.checkpoint_policy() {
            CheckpointPolicy::PersistAfterCommit => {
                driver
                    .persist_checkpoint(position)
                    .await
                    .context("persist_checkpoint")?;
            }
            CheckpointPolicy::CommitOnly => {}
            CheckpointPolicy::IntervalWhenDrained { .. } => {
                self.pending_checkpoint = Some(position);
                // Persist sunk watermarks promptly once the window is empty
                // (matches pre-SourceDriver last-sunk-on-sink cadence). Interval
                // still gates filtered-only read_progress persists.
                if self.is_fully_drained() {
                    self.flush_pending_checkpoint(driver).await?;
                }
            }
        }
        Ok(())
    }

    async fn try_interval_persist(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
    ) -> Result<()> {
        let CheckpointPolicy::IntervalWhenDrained { interval } = driver.checkpoint_policy() else {
            return Ok(());
        };
        if !self.is_fully_drained() {
            return Ok(());
        }
        if self.last_checkpoint_persist.elapsed() < interval {
            return Ok(());
        }
        if self.pending_checkpoint.is_some() {
            return self.flush_pending_checkpoint(driver).await;
        }
        // No commit armed a pending watermark (filtered-only / idle catch-up).
        // Ask the driver for a drained-safe read position to persist.
        if let Some(position) = driver
            .read_progress_for_persist()
            .await
            .context("read_progress_for_persist")?
        {
            driver
                .persist_checkpoint(position)
                .await
                .context("persist_checkpoint")?;
            self.last_checkpoint_persist = Instant::now();
        }
        Ok(())
    }

    async fn flush_pending_checkpoint(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
    ) -> Result<()> {
        if let Some(position) = self.pending_checkpoint.take() {
            driver
                .persist_checkpoint(position)
                .await
                .context("persist_checkpoint")?;
            self.last_checkpoint_persist = Instant::now();
        }
        Ok(())
    }

    /// Flush remaining work and commit via driver (used on cancel/deadline stop).
    pub(crate) async fn flush_for_driver(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
    ) -> Result<()> {
        if self.poisoned {
            return Ok(());
        }
        loop {
            while self.try_start_partial_batch() {}
            self.drain_ordered_driver(driver).await?;
            if self.buffer.is_empty()
                && self.in_flight.is_empty()
                && self.completed.is_empty()
                && !self.sink_in_flight
            {
                // Force-persist any deferred IntervalWhenDrained position.
                self.flush_pending_checkpoint(driver).await?;
                return Ok(());
            }
            if self.in_flight_count() > 0 {
                self.wait_one_completion().await?;
                continue;
            }
            if !self.buffer.is_empty() {
                if !self.try_start_partial_batch() {
                    bail!(
                        "flush_for_driver: {} buffered but window would not accept a batch",
                        self.buffer.len()
                    );
                }
                continue;
            }
            bail!(
                "flush_for_driver: {} completed waiting but none is next_to_apply={}",
                self.completed.len(),
                self.next_to_apply
            );
        }
    }

    async fn drain_ordered_no_commit(&mut self) -> Result<Option<P>> {
        let mut last = None;
        loop {
            self.poll_join_ready().await?;
            let Some(batch) = self.prepare_ordered_sink() else {
                break;
            };
            match batch.result {
                Ok(events) => match self.apply_sink_events(&events).await {
                    Ok(()) => {
                        last = Some(self.finish_sink_ok_no_commit(
                            batch.last_position,
                            events.len() as u64,
                        ));
                    }
                    Err(e) => {
                        last = self.finish_sink_err_no_commit(
                            batch.batch_id,
                            batch.last_position,
                            e,
                        )?;
                    }
                },
                Err(e) => {
                    last = self.finish_sink_err_no_commit(
                        batch.batch_id,
                        batch.last_position,
                        e,
                    )?;
                }
            }
        }
        Ok(last)
    }

    async fn fail_or_skip_driver(
        &mut self,
        driver: &mut impl SourceDriver<Position = P>,
        batch_id: u64,
        last_position: P,
        event_count: u64,
        e: anyhow::Error,
    ) -> Result<()> {
        match self.opts.failure_policy {
            FailurePolicy::Fail => {
                self.discard_successors();
                Err(e).with_context(|| format!("batch {batch_id} failed"))
            }
            FailurePolicy::Skip => {
                warn!(
                    batch_id,
                    error = %e,
                    "skipping failed batch (failure_policy=skip); committing past it"
                );
                // Account for skipped events so drivers that gate advance on
                // note_sunk_events (e.g. wal2json slot) do not stall.
                driver.note_sunk_events(event_count);
                driver
                    .commit(last_position.clone())
                    .await
                    .context("commit after skip")?;
                self.after_commit_persist(driver, last_position).await?;
                self.next_to_apply += 1;
                Ok(())
            }
        }
    }

    fn fail_or_skip_no_feed(
        &mut self,
        batch_id: u64,
        last_position: P,
        e: anyhow::Error,
    ) -> Result<Option<P>> {
        match self.opts.failure_policy {
            FailurePolicy::Fail => {
                self.discard_successors();
                Err(e).with_context(|| format!("batch {batch_id} failed"))
            }
            FailurePolicy::Skip => {
                warn!(
                    batch_id,
                    error = %e,
                    "skipping failed batch (failure_policy=skip)"
                );
                self.next_to_apply += 1;
                Ok(Some(last_position))
            }
        }
    }

    fn discard_successors(&mut self) {
        self.epoch = self.epoch.saturating_add(1);
        self.in_flight.clear();
        self.completed.clear();
        self.buffer.clear();
        self.buffer_started = None;
        self.sink_in_flight = false;
        self.join_set.abort_all();
        self.poisoned = true;
    }

    async fn apply_sink_events(&self, events: &[ApplyEvent]) -> Result<()> {
        for event in events {
            match event {
                ApplyEvent::Change(change) => {
                    self.sink
                        .apply_universal_change(change)
                        .await
                        .context("sink apply_universal_change")?;
                }
                ApplyEvent::RelationChange(change) => {
                    self.sink
                        .apply_universal_relation_change(change)
                        .await
                        .context("sink apply_universal_relation_change")?;
                }
            }
        }
        Ok(())
    }
}

// Re-export helper for tests that still construct PositionedChange.
#[allow(dead_code)]
fn _positioned_change_to_event<P>(pc: PositionedChange<P>) -> PositionedEvent<P> {
    pc.into()
}
