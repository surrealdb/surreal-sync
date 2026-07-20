//! Unified in-flight window runtime: transform → ordered sink → contiguous commit.

use crate::apply::feed::{ChangeFeed, PositionedChange};
use crate::apply::opts::{ApplyOpts, FailurePolicy};
use crate::apply::transform::BatchTransformer;
use crate::pipeline::Pipeline;
use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use surreal_sink::SurrealSink;
use sync_core::{UniversalChange, UniversalRow};
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

/// Transform then apply each change via [`SurrealSink::apply_universal_change`].
///
/// Used by interleaved snapshot reconciliation (one event at a time) and other
/// paths that already hold a change batch. Identity pipelines skip transform
/// dispatch entirely.
pub async fn apply_changes<S: SurrealSink>(
    sink: &S,
    pipeline: &Pipeline,
    changes: Vec<UniversalChange>,
    opts: &ApplyOpts,
) -> Result<()> {
    apply_changes_with(sink, Arc::new(pipeline.clone()), changes, opts).await
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

/// Framework-owned incremental loop: poll → batch → transform window → ordered
/// sink apply → contiguous `commit`.
///
/// Exits when the feed reports [`ChangeFeed::is_finished`] and all buffered /
/// in-flight work has been drained.
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
    let mut ctx = ApplyContext::new(sink, transformer, opts);
    loop {
        // Fill the transform window to capacity before waiting on completions.
        // This is what makes W≥2 overlap real (out-of-order transform completion).
        while ctx.in_flight_count() < opts.max_in_flight {
            while ctx.buffer_len() < opts.batch_size && !feed.is_finished() {
                let polled = feed.poll_changes().await.context("poll_changes")?;
                if polled.is_empty() {
                    break;
                }
                for pc in polled {
                    ctx.push_buffered(pc);
                }
            }

            if ctx.buffer_len() >= opts.batch_size {
                if !ctx.try_start_full_batch() {
                    // Async window full (in-flight transforms at capacity).
                    break;
                }
                // Identity completes synchronously into `completed` — drain now so
                // the fill loop (which keys off in_flight) can keep going.
                ctx.drain_ordered(feed).await?;
                continue;
            }

            if ctx.buffer_len() > 0 && (feed.is_finished() || ctx.should_flush_partial()) {
                if !ctx.try_start_partial_batch() {
                    break;
                }
                ctx.drain_ordered(feed).await?;
                continue;
            }

            // Cannot start another batch right now (need more input or wait).
            break;
        }

        // Drain any completed results (including identity leftovers) before
        // deciding whether to wait on JoinSet.
        ctx.drain_ordered(feed).await?;

        if ctx.in_flight_count() == 0 && ctx.buffer_len() == 0 {
            if feed.is_finished() {
                return Ok(());
            }
            // Endless source idle: brief sleep then re-poll.
            tokio::time::sleep(opts.batch_max_wait.min(Duration::from_millis(10))).await;
            continue;
        }

        if ctx.in_flight_count() == 0 {
            // Buffered but waiting on batch_max_wait (feed not finished).
            tokio::time::sleep(opts.batch_max_wait).await;
            continue;
        }

        // Window full or cannot start more — wait for a transform completion.
        ctx.wait_one_completion().await?;
        ctx.drain_ordered(feed).await?;
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
    result: Result<Vec<UniversalChange>>,
}

struct TransformOutcome<P> {
    epoch: u64,
    seq: u64,
    batch_id: u64,
    last_position: P,
    result: Result<Vec<UniversalChange>>,
}

/// Library / custom-loop driver sharing the same ordered apply path as
/// [`run_change_feed`].
///
/// `push_change` / `flush` return `Some(position)` when a batch was transformed,
/// sunk, and is safe to `commit`. They do **not** call [`ChangeFeed::commit`].
///
/// # Poisoning after [`FailurePolicy::Fail`]
///
/// After a batch fails under [`FailurePolicy::Fail`], this context is
/// **poisoned**: successors are discarded, and further [`Self::push_change`] /
/// [`Self::flush`] return `Err` immediately. Do not reuse the context — create
/// a new one and replay from the last successful commit watermark.
pub struct ApplyContext<'a, S, T, P = ()> {
    sink: &'a S,
    transformer: Arc<T>,
    opts: &'a ApplyOpts,
    buffer: Vec<PositionedChange<P>>,
    next_batch_id: u64,
    next_seq: u64,
    next_to_apply: u64,
    in_flight: HashMap<u64, InFlightMeta<P>>,
    completed: HashMap<u64, CompletedBatch<P>>,
    join_set: JoinSet<TransformOutcome<P>>,
    buffer_started: Option<tokio::time::Instant>,
    /// Bumped on fail-discard so stale JoinSet tasks are ignored.
    epoch: u64,
    /// Set after [`FailurePolicy::Fail`]; context must not be reused.
    poisoned: bool,
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
            buffer_started: None,
            epoch: 0,
            poisoned: false,
        }
    }

    /// Whether this context was poisoned by a [`FailurePolicy::Fail`] error.
    ///
    /// A poisoned context must not be reused; create a new one instead.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Number of changes waiting to form a batch.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Number of batches currently transforming (JoinSet path only).
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Number of transform results waiting for ordered sink apply.
    ///
    /// Useful in tests to assert successor discard cleared the window.
    pub fn completed_waiting_count(&self) -> usize {
        self.completed.len()
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

    fn push_buffered(&mut self, pc: PositionedChange<P>) {
        if self.buffer.is_empty() {
            self.buffer_started = Some(tokio::time::Instant::now());
        }
        self.buffer.push(pc);
    }

    fn should_flush_partial(&self) -> bool {
        match self.buffer_started {
            Some(started) => started.elapsed() >= self.opts.batch_max_wait,
            None => false,
        }
    }

    /// Push one change; may start transforms and drain ordered sink/commit.
    ///
    /// Returns the last position that was successfully sunk during this call
    /// (caller should `commit` it). Does not call [`ChangeFeed::commit`] itself.
    ///
    /// Returns `Err` immediately if this context is [`Self::is_poisoned`].
    pub async fn push_change(
        &mut self,
        change: UniversalChange,
        position: P,
    ) -> Result<Option<P>> {
        self.ensure_not_poisoned()?;
        self.push_buffered(PositionedChange::new(change, position));
        while self.try_start_full_batch() {}
        // Collect any already-finished transforms without blocking.
        // Yield once so Duration::ZERO scripted tasks can complete in-process.
        self.poll_join_ready().await?;
        self.drain_ordered_no_commit().await
    }

    /// Flush remaining buffered changes and wait for in-flight work.
    ///
    /// Returns the last position successfully sunk (caller should `commit`).
    ///
    /// Returns `Err` immediately if this context is [`Self::is_poisoned`].
    pub async fn flush(&mut self) -> Result<Option<P>> {
        self.ensure_not_poisoned()?;
        while self.try_start_partial_batch() {}
        let mut last = None;
        while self.in_flight_count() > 0 {
            self.wait_one_completion().await?;
            if let Some(p) = self.drain_ordered_no_commit().await? {
                last = Some(p);
            }
        }
        if let Some(p) = self.drain_ordered_no_commit().await? {
            last = Some(p);
        }
        Ok(last)
    }

    /// Transform then sink rows (same as [`write_rows_with`]).
    pub async fn write_rows(&self, rows: Vec<UniversalRow>) -> Result<()> {
        self.ensure_not_poisoned()?;
        write_rows_with(self.sink, Arc::clone(&self.transformer), rows, self.opts).await
    }

    fn try_start_full_batch(&mut self) -> bool {
        if self.buffer.len() < self.opts.batch_size {
            return false;
        }
        self.start_batch_from_buffer(self.opts.batch_size)
    }

    fn try_start_partial_batch(&mut self) -> bool {
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
        // Window occupancy: JoinSet path uses `in_flight`; identity completes
        // synchronously into `completed`, so count those too before drain.
        let occupying = if self.transformer.is_identity() {
            self.completed.len() + self.in_flight.len()
        } else {
            self.in_flight.len()
        };
        if occupying >= self.opts.max_in_flight {
            return false;
        }

        let batch: Vec<PositionedChange<P>> = self.buffer.drain(..n).collect();
        if self.buffer.is_empty() {
            self.buffer_started = None;
        } else {
            self.buffer_started = Some(tokio::time::Instant::now());
        }

        let last_position = batch.last().expect("n > 0").position.clone();
        let changes: Vec<UniversalChange> = batch.into_iter().map(|pc| pc.change).collect();

        let batch_id = self.next_batch_id;
        self.next_batch_id = self.next_batch_id.saturating_add(1);
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        // Zero-overhead identity: pure move into completed, no JoinSet/timeout.
        if self.transformer.is_identity() {
            self.completed.insert(
                seq,
                CompletedBatch {
                    batch_id,
                    last_position,
                    result: Ok(changes),
                },
            );
            return true;
        }

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
        self.join_set.spawn(async move {
            let result = match tokio::time::timeout(
                timeout,
                transformer.transform_changes(batch_id, changes),
            )
            .await
            {
                Ok(inner) => inner,
                Err(_) => Err(anyhow!(
                    "transform timeout after {:?} for batch_id={batch_id}",
                    timeout
                )),
            };
            TransformOutcome {
                epoch,
                seq,
                batch_id,
                last_position,
                result,
            }
        });
        true
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

    async fn wait_one_completion(&mut self) -> Result<()> {
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
            // Discarded after a prior failure; ignore.
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
                result: outcome.result,
            },
        );
        Ok(())
    }

    async fn drain_ordered(&mut self, feed: &mut impl ChangeFeed<Position = P>) -> Result<()> {
        loop {
            let Some(batch) = self.completed.remove(&self.next_to_apply) else {
                return Ok(());
            };
            match batch.result {
                Ok(changes) => match self.apply_sink(&changes).await {
                    Ok(()) => {
                        feed.commit(batch.last_position)
                            .await
                            .context("commit")?;
                        self.next_to_apply += 1;
                    }
                    Err(e) => {
                        self.fail_or_skip(feed, batch.batch_id, batch.last_position, e)
                            .await?;
                    }
                },
                Err(e) => {
                    self.fail_or_skip(feed, batch.batch_id, batch.last_position, e)
                        .await?;
                }
            }
        }
    }

    /// Drain ordered sink apply without committing (for [`ApplyContext::push_change`]).
    async fn drain_ordered_no_commit(&mut self) -> Result<Option<P>> {
        let mut last = None;
        loop {
            let Some(batch) = self.completed.remove(&self.next_to_apply) else {
                break;
            };
            match batch.result {
                Ok(changes) => match self.apply_sink(&changes).await {
                    Ok(()) => {
                        last = Some(batch.last_position);
                        self.next_to_apply += 1;
                    }
                    Err(e) => {
                        last = self.fail_or_skip_no_feed(batch.batch_id, batch.last_position, e)?;
                    }
                },
                Err(e) => {
                    last = self.fail_or_skip_no_feed(batch.batch_id, batch.last_position, e)?;
                }
            }
        }
        Ok(last)
    }

    async fn fail_or_skip(
        &mut self,
        feed: &mut impl ChangeFeed<Position = P>,
        batch_id: u64,
        last_position: P,
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
                feed.commit(last_position)
                    .await
                    .context("commit after skip")?;
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
        // Also drop any not-yet-started buffered changes — docs claim successors
        // are discarded; leaving the buffer would re-process them after Fail
        // if the poisoned check were ever bypassed, and contradicts the
        // "discard in-flight K+1…" reliability story for unstarted work.
        self.buffer.clear();
        self.buffer_started = None;
        // Abort remaining tasks; late JoinSet results will have a stale epoch
        // (or we abort them). JoinSet::abort_all is available on recent tokio.
        self.join_set.abort_all();
        // Poison so reuse cannot hang on a missing next_to_apply seq.
        self.poisoned = true;
    }

    async fn apply_sink(&self, changes: &[UniversalChange]) -> Result<()> {
        for change in changes {
            self.sink
                .apply_universal_change(change)
                .await
                .context("sink apply_universal_change")?;
        }
        Ok(())
    }
}
