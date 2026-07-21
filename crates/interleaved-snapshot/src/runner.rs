//! The generic DBLog-style watermark snapshot loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use checkpoint::{InterleavedSnapshotCheckpoint, SnapshotTableProgress};
use surreal_sink::SurrealSink;
use sync_core::{UniversalChange, UniversalRow};
use sync_transform::{
    run_source_runtime_with, ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver,
    SourceRuntimeOpts,
};
use tracing::info;
use uuid::Uuid;

use crate::checkpointer::SnapshotCheckpointer;
use crate::source::WatermarkSource;
use crate::types::{PkTuple, ReconciliationPos, TableSpec, WatermarkKind};

/// Default chunk size (matches Debezium's incremental snapshot default).
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Configuration for a watermark snapshot run.
#[derive(Debug, Clone)]
pub struct InterleavedSnapshotConfig {
    /// Maximum number of rows read per chunk (the `LIMIT` of each keyset read).
    /// This structurally bounds the loop's buffered memory.
    pub chunk_size: usize,
}

impl Default for InterleavedSnapshotConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

/// Transform pipeline + apply options for interleaved snapshot sink writes.
///
/// Default is an identity pipeline with `batch_size = 1` (no transform stage
/// dispatch; matches pre-transform behavior).
#[derive(Debug, Clone)]
pub struct SnapshotTransforms {
    pub pipeline: Pipeline,
    pub apply_opts: ApplyOpts,
}

impl Default for SnapshotTransforms {
    fn default() -> Self {
        Self::identity()
    }
}

impl SnapshotTransforms {
    /// Empty pipeline, per-row apply cadence.
    pub fn identity() -> Self {
        Self {
            pipeline: Pipeline::new(),
            apply_opts: ApplyOpts::identity(),
        }
    }

    /// Borrowed view used by the runner.
    pub fn from_refs(pipeline: &Pipeline, apply_opts: &ApplyOpts) -> Self {
        Self {
            pipeline: pipeline.clone(),
            apply_opts: apply_opts.clone(),
        }
    }
}

/// Outcome of a completed watermark snapshot.
#[derive(Debug, Clone)]
pub struct InterleavedSnapshotResult<P: ReconciliationPos> {
    /// The final stream position reached; downstream live processing resumes
    /// from here.
    pub final_position: P,
    /// The peak number of rows held in the chunk buffer at any instant during
    /// the run, recorded inline at every buffer insertion (an exact maximum,
    /// not a sample). For full chunks this equals `chunk_size`, independent of
    /// table size.
    pub peak_buffered_rows: usize,
}

/// Per-table copy progress accumulated across chunks.
struct TableState {
    name: String,
    last_pk: Option<PkTuple>,
    done: bool,
}

fn build_checkpoint<P: ReconciliationPos>(
    position: &P,
    progress: &[TableState],
) -> Result<InterleavedSnapshotCheckpoint> {
    let tables = progress
        .iter()
        .map(|t| {
            Ok(SnapshotTableProgress {
                name: t.name.clone(),
                last_pk: t.last_pk.as_ref().map(serde_json::to_value).transpose()?,
                done: t.done,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(InterleavedSnapshotCheckpoint {
        reconciliation_pos: serde_json::to_value(position)?,
        tables,
    })
}

fn table_state_from_progress(t: &SnapshotTableProgress) -> Result<TableState> {
    let last_pk = match &t.last_pk {
        None => None,
        Some(v) => Some(serde_json::from_value(v.clone())?),
    };
    Ok(TableState {
        name: t.name.clone(),
        last_pk,
        done: t.done,
    })
}

fn init_snapshot_state(
    all_tables: Vec<TableSpec>,
    resume: Option<&InterleavedSnapshotCheckpoint>,
) -> Result<(VecDeque<TableSpec>, HashSet<String>, Vec<TableState>)> {
    let progress_by_name: HashMap<String, &SnapshotTableProgress> = resume
        .map(|cp| cp.tables.iter().map(|t| (t.name.clone(), t)).collect())
        .unwrap_or_default();

    let progress: Vec<TableState> = all_tables
        .iter()
        .map(|spec| {
            if let Some(saved) = progress_by_name.get(&spec.table) {
                table_state_from_progress(saved)
            } else {
                Ok(TableState {
                    name: spec.table.clone(),
                    last_pk: None,
                    done: false,
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let queue: VecDeque<TableSpec> = all_tables
        .into_iter()
        .filter(|spec| {
            progress_by_name
                .get(&spec.table)
                .map(|t| !t.done)
                .unwrap_or(true)
        })
        .collect();

    let seen: HashSet<String> = progress.iter().map(|t| t.name.clone()).collect();
    Ok((queue, seen, progress))
}

/// Run a watermark snapshot, copying every table reported by `source` in
/// primary-key-ordered chunks while concurrently consuming and applying the
/// source's change stream to `sink`.
///
/// Uses an identity transform pipeline. Prefer
/// [`run_interleaved_snapshot_with_transforms`] when a [`Pipeline`] is configured.
pub async fn run_interleaved_snapshot<S, K, C>(
    source: &mut S,
    sink: &K,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
) -> Result<InterleavedSnapshotResult<S::Position>>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let transforms = SnapshotTransforms::identity();
    run_interleaved_snapshot_with_resume_and_transforms(
        source,
        sink,
        config,
        checkpointer,
        None,
        &transforms,
    )
    .await
}

/// Like [`run_interleaved_snapshot`], but resumes from a saved per-chunk
/// checkpoint when `resume` is set. Identity transforms.
pub async fn run_interleaved_snapshot_with_resume<S, K, C>(
    source: &mut S,
    sink: &K,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
    resume: Option<&InterleavedSnapshotCheckpoint>,
) -> Result<InterleavedSnapshotResult<S::Position>>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let transforms = SnapshotTransforms::identity();
    run_interleaved_snapshot_with_resume_and_transforms(
        source,
        sink,
        config,
        checkpointer,
        resume,
        &transforms,
    )
    .await
}

/// [`run_interleaved_snapshot`] with an explicit transform pipeline.
pub async fn run_interleaved_snapshot_with_transforms<S, K, C>(
    source: &mut S,
    sink: &K,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
    transforms: &SnapshotTransforms,
) -> Result<InterleavedSnapshotResult<S::Position>>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    run_interleaved_snapshot_with_resume_and_transforms(
        source,
        sink,
        config,
        checkpointer,
        None,
        transforms,
    )
    .await
}

/// Resume-capable watermark snapshot with transform → sink apply.
///
/// Checkpoint / `commit_reconciled` still run only after successful sink writes
/// (transform ack is in-memory only).
pub async fn run_interleaved_snapshot_with_resume_and_transforms<S, K, C>(
    source: &mut S,
    sink: &K,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
    resume: Option<&InterleavedSnapshotCheckpoint>,
    transforms: &SnapshotTransforms,
) -> Result<InterleavedSnapshotResult<S::Position>>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let all_tables = source.snapshot_tables().await?;
    let (mut queue, mut seen, mut progress) = init_snapshot_state(all_tables, resume)?;

    let mut peak_buffered_rows = 0usize;

    loop {
        // Poll for ad-hoc snapshot signals and enqueue any newly requested
        // tables, so the stream keeps running while additional tables are
        // snapshotted with the same watermark-window machinery.
        for signal in source.read_signals().await? {
            let specs = source.resolve_tables(&signal.tables).await?;
            for spec in specs {
                if seen.insert(spec.table.clone()) {
                    info!(
                        "ad-hoc snapshot requested for table '{}' (signal {})",
                        spec.table, signal.id
                    );
                    progress.push(TableState {
                        name: spec.table.clone(),
                        last_pk: None,
                        done: false,
                    });
                    queue.push_back(spec);
                }
            }
        }

        let Some(spec) = queue.pop_front() else {
            break;
        };
        let table_index = progress
            .iter()
            .position(|t| t.name == spec.table)
            .expect("progress entry for queued table");

        snapshot_one_table(
            source,
            sink,
            config,
            checkpointer,
            &spec,
            table_index,
            &mut progress,
            &mut peak_buffered_rows,
            transforms,
        )
        .await?;
    }

    let final_position = source.current_position().await?;
    Ok(InterleavedSnapshotResult {
        final_position,
        peak_buffered_rows,
    })
}

/// Snapshot a fixed set of tables (for example after an ad-hoc
/// `execute-snapshot` signal during steady-state streaming) without re-running
/// the initial [`WatermarkSource::snapshot_tables`] enumeration.
pub async fn run_adhoc_snapshot_tables<S, K, C>(
    source: &mut S,
    sink: &K,
    tables: Vec<TableSpec>,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
) -> Result<()>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let transforms = SnapshotTransforms::identity();
    run_adhoc_snapshot_tables_with_transforms(
        source,
        sink,
        tables,
        config,
        checkpointer,
        &transforms,
    )
    .await
}

/// [`run_adhoc_snapshot_tables`] with an explicit transform pipeline.
pub async fn run_adhoc_snapshot_tables_with_transforms<S, K, C>(
    source: &mut S,
    sink: &K,
    tables: Vec<TableSpec>,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
    transforms: &SnapshotTransforms,
) -> Result<()>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let mut progress: Vec<TableState> = tables
        .iter()
        .map(|t| TableState {
            name: t.table.clone(),
            last_pk: None,
            done: false,
        })
        .collect();
    let mut peak_buffered_rows = 0usize;
    for (table_index, spec) in tables.iter().enumerate() {
        snapshot_one_table(
            source,
            sink,
            config,
            checkpointer,
            spec,
            table_index,
            &mut progress,
            &mut peak_buffered_rows,
            transforms,
        )
        .await?;
    }
    Ok(())
}

/// Snapshot a single table in primary-key-ordered chunks, applying the same
/// low/high watermark dedup window per chunk and checkpointing after each one.
///
/// One long-lived [`run_source_runtime_with`] spans every chunk so reconciliation
/// polls continue under spare `max_in_flight` while ordered sink applies run
/// (R∩W), and surviving buffer rows share that same apply window (M1).
/// `commit_reconciled` / checkpointer run only after each chunk's events sink.
#[allow(clippy::too_many_arguments)]
async fn snapshot_one_table<S, K, C>(
    source: &mut S,
    sink: &K,
    config: &InterleavedSnapshotConfig,
    checkpointer: &mut C,
    spec: &TableSpec,
    table_index: usize,
    progress: &mut [TableState],
    peak_buffered_rows: &mut usize,
    transforms: &SnapshotTransforms,
) -> Result<()>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let after = progress[table_index].last_pk.clone();
    let mut driver = SnapshotTableDriver {
        source,
        checkpointer,
        spec,
        table_index,
        progress,
        peak_buffered_rows,
        chunk_size: config.chunk_size,
        after,
        phase: SnapshotPhase::NeedChunk,
        low_id: Uuid::nil(),
        high_id: Uuid::nil(),
        in_window: false,
        buffer: HashMap::new(),
        last_pk: None,
        chunk_len: 0,
        pending: VecDeque::new(),
        next_pos: 0,
        events_emitted: 0,
        events_sunk: 0,
        chunk_poll_complete: false,
        finished: false,
    };

    let transformer = Arc::new(transforms.pipeline.clone());
    let runtime_opts = SourceRuntimeOpts::new();
    run_source_runtime_with(
        &mut driver,
        sink,
        transformer,
        &transforms.apply_opts,
        &runtime_opts,
    )
    .await?;
    Ok(())
}

/// Phases of the per-table interleaved snapshot state machine.
enum SnapshotPhase {
    /// Open watermarks and read the next keyset chunk.
    NeedChunk,
    /// Consume reconciliation events until the high watermark; survivors are
    /// flushed into the apply queue as soon as high is observed (before any
    /// later events in the same poll batch).
    Reconciling,
    /// Wait until sunk count catches emitted count, then checkpoint.
    AwaitingChunkSink,
}

/// Long-lived driver: chunk reads + reconciliation + buffer flush share one
/// `run_source_runtime` apply window across the whole table.
struct SnapshotTableDriver<'a, S, C>
where
    S: WatermarkSource,
    C: SnapshotCheckpointer,
{
    source: &'a mut S,
    checkpointer: &'a mut C,
    spec: &'a TableSpec,
    table_index: usize,
    progress: &'a mut [TableState],
    peak_buffered_rows: &'a mut usize,
    chunk_size: usize,
    after: Option<PkTuple>,
    phase: SnapshotPhase,
    low_id: Uuid,
    high_id: Uuid,
    in_window: bool,
    buffer: HashMap<String, UniversalRow>,
    last_pk: Option<PkTuple>,
    chunk_len: usize,
    pending: VecDeque<PositionedEvent<u64>>,
    next_pos: u64,
    events_emitted: u64,
    events_sunk: u64,
    chunk_poll_complete: bool,
    finished: bool,
}

impl<'a, S, C> SnapshotTableDriver<'a, S, C>
where
    S: WatermarkSource,
    C: SnapshotCheckpointer,
{
    fn push_change(&mut self, change: UniversalChange) {
        let pos = self.next_pos;
        self.next_pos = self.next_pos.saturating_add(1);
        self.events_emitted = self.events_emitted.saturating_add(1);
        self.pending
            .push_back(PositionedEvent::change(change, pos));
    }

    async fn open_chunk(&mut self) -> Result<()> {
        let low_id = Uuid::new_v4();
        self.source
            .write_watermark(WatermarkKind::Low, low_id)
            .await?;

        let rows = self
            .source
            .read_chunk(self.spec, self.after.as_ref(), self.chunk_size)
            .await?;

        if rows.is_empty() {
            self.progress[self.table_index].done = true;
            self.source
                .on_table_snapshot_complete(&self.spec.table)
                .await?;
            let position = self.source.current_position().await?;
            self.checkpointer
                .save_progress(&build_checkpoint(&position, self.progress)?)
                .await?;
            self.source.commit_reconciled(position).await?;
            self.finished = true;
            return Ok(());
        }

        self.chunk_len = rows.len();
        self.buffer = HashMap::with_capacity(self.chunk_len);
        self.last_pk = None;
        for row in rows {
            let pk = PkTuple::from_row(&row, &self.spec.pk_columns)?;
            self.last_pk = Some(pk.clone());
            self.buffer.insert(pk.key(), row);
            if self.buffer.len() > *self.peak_buffered_rows {
                *self.peak_buffered_rows = self.buffer.len();
            }
        }

        let high_id = Uuid::new_v4();
        self.source
            .write_watermark(WatermarkKind::High, high_id)
            .await?;

        self.low_id = low_id;
        self.high_id = high_id;
        self.in_window = false;
        self.events_emitted = 0;
        self.events_sunk = 0;
        self.chunk_poll_complete = false;
        self.phase = SnapshotPhase::Reconciling;
        Ok(())
    }

    fn queue_buffer_rows(&mut self) {
        let rows: Vec<UniversalRow> = self.buffer.drain().map(|(_, row)| row).collect();
        for row in rows {
            let change = UniversalChange::update(row.table, row.id, row.fields);
            self.push_change(change);
        }
        self.chunk_poll_complete = true;
        self.phase = SnapshotPhase::AwaitingChunkSink;
    }

    async fn try_finish_chunk_after_sink(&mut self) -> Result<()> {
        if !self.chunk_poll_complete || self.events_sunk < self.events_emitted {
            return Ok(());
        }

        self.after = self.last_pk.clone();
        self.progress[self.table_index].last_pk = self.last_pk.clone();

        let table_done = self.chunk_len < self.chunk_size;
        if table_done {
            self.progress[self.table_index].done = true;
            self.source
                .on_table_snapshot_complete(&self.spec.table)
                .await?;
        }

        let position = self.source.current_position().await?;
        self.checkpointer
            .save_progress(&build_checkpoint(&position, self.progress)?)
            .await?;
        self.source.commit_reconciled(position).await?;

        if table_done {
            self.finished = true;
        } else {
            // Next chunk may open while prior-chunk transforms already drained;
            // the same ApplyContext stays alive across chunks (M1).
            self.phase = SnapshotPhase::NeedChunk;
            self.chunk_poll_complete = false;
        }
        Ok(())
    }
}

#[async_trait]
impl<S, C> SourceDriver for SnapshotTableDriver<'_, S, C>
where
    S: WatermarkSource,
    C: SnapshotCheckpointer,
{
    type Position = u64;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        loop {
            if self.finished {
                return Ok(Vec::new());
            }

            // Drain already-queued events first so the runtime can fill the window.
            if !self.pending.is_empty() {
                let mut out = Vec::with_capacity(self.pending.len().min(64));
                while out.len() < 64 {
                    match self.pending.pop_front() {
                        Some(pe) => out.push(pe),
                        None => break,
                    }
                }
                return Ok(out);
            }

            match self.phase {
                SnapshotPhase::NeedChunk => {
                    self.open_chunk().await?;
                    if self.finished {
                        return Ok(Vec::new());
                    }
                    // Same poll: start reconciling the chunk we just opened.
                    continue;
                }
                SnapshotPhase::Reconciling => {
                    let events = self.source.next_reconciliation_events().await?;
                    let mut saw_high = false;
                    for event in events {
                        if let Some(watermark_id) = event.pk.single_uuid() {
                            if watermark_id == self.low_id {
                                self.in_window = true;
                                continue;
                            }
                            if watermark_id == self.high_id {
                                self.in_window = false;
                                // Flush survivors *before* any post-high events
                                // that share this poll batch. Otherwise a Delete
                                // that commits after high would be queued first
                                // and then overwritten by a stale buffer upsert.
                                self.queue_buffer_rows();
                                saw_high = true;
                                continue;
                            }
                        }

                        if self.in_window {
                            self.buffer.remove(&event.pk.key());
                        }
                        self.push_change(event.change);
                    }

                    if saw_high {
                        if self.events_emitted == 0 {
                            // Empty window (no CDC, no survivors): checkpoint now.
                            self.try_finish_chunk_after_sink().await?;
                            continue;
                        }
                        let mut out = Vec::with_capacity(self.pending.len().min(64));
                        while out.len() < 64 {
                            match self.pending.pop_front() {
                                Some(pe) => out.push(pe),
                                None => break,
                            }
                        }
                        return Ok(out);
                    }

                    let mut out = Vec::with_capacity(self.pending.len().min(64));
                    while out.len() < 64 {
                        match self.pending.pop_front() {
                            Some(pe) => out.push(pe),
                            None => break,
                        }
                    }
                    return Ok(out);
                }
                SnapshotPhase::AwaitingChunkSink => {
                    // Sink-gated: do not open the next chunk until watermark advance catches up.
                    return Ok(Vec::new());
                }
            }
        }
    }

    async fn advance_watermark(&mut self, _position: Self::Position) -> Result<()> {
        // Watermark advance is sink-ordered; chunk retention advances in
        // note_sunk_events once emitted count is fully sunk (see
        // try_finish_chunk_after_sink).
        self.try_finish_chunk_after_sink().await
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        // Durability is the interleaved checkpointer + commit_reconciled, not
        // persist_checkpoint on this u64 apply index.
        CheckpointPolicy::AdvanceOnly
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.events_sunk = self.events_sunk.saturating_add(count);
    }
}
