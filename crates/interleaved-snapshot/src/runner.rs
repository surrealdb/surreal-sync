//! The generic DBLog-style watermark snapshot loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use anyhow::Result;
use checkpoint::{InterleavedSnapshotCheckpoint, SnapshotTableProgress};
use surreal_sink::SurrealSink;
use sync_core::UniversalRow;
use sync_transform::{write_rows, ApplyContext, ApplyOpts, Pipeline};
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
    // Resume from the last copied primary key if progress already recorded one.
    let mut after: Option<PkTuple> = progress[table_index].last_pk.clone();

    loop {
        // Open the dedup window before reading the chunk.
        let low_id = Uuid::new_v4();
        source.write_watermark(WatermarkKind::Low, low_id).await?;

        let rows = source
            .read_chunk(spec, after.as_ref(), config.chunk_size)
            .await?;

        if rows.is_empty() {
            progress[table_index].done = true;
            source.on_table_snapshot_complete(&spec.table).await?;
            let position = source.current_position().await?;
            checkpointer
                .save_progress(&build_checkpoint(&position, progress)?)
                .await?;
            source.commit_reconciled(position).await?;
            break;
        }

        let chunk_len = rows.len();

        // Load the chunk into the primary-key-keyed buffer, tracking the
        // peak inline at every insertion (event-based, exact maximum).
        let mut buffer: HashMap<String, UniversalRow> = HashMap::with_capacity(chunk_len);
        let mut last_pk: Option<PkTuple> = None;
        for row in rows {
            let pk = PkTuple::from_row(&row, &spec.pk_columns)?;
            last_pk = Some(pk.clone());
            buffer.insert(pk.key(), row);
            if buffer.len() > *peak_buffered_rows {
                *peak_buffered_rows = buffer.len();
            }
        }

        // Close the dedup window after the chunk is read.
        let high_id = Uuid::new_v4();
        source.write_watermark(WatermarkKind::High, high_id).await?;

        // Consume the stream until the high watermark passes by. Every
        // data change is applied to the sink; while in-window, any buffered
        // row touched by an event is dropped so the log event wins.
        //
        // One long-lived ApplyContext spans the whole reconciliation window so
        // `max_in_flight > 1` can overlap transforms across events (and the
        // final buffer flush). Checkpoint / commit_reconciled run only after
        // this context fully drains.
        let transformer = Arc::new(transforms.pipeline.clone());
        let mut ctx = ApplyContext::<_, _, ()>::new(
            sink,
            Arc::clone(&transformer),
            &transforms.apply_opts,
        );
        let mut in_window = false;
        let mut window_closed = false;
        while !window_closed {
            let events = source.next_reconciliation_events().await?;
            for event in events {
                if let Some(watermark_id) = event.pk.single_uuid() {
                    if watermark_id == low_id {
                        in_window = true;
                        continue;
                    }
                    if watermark_id == high_id {
                        in_window = false;
                        window_closed = true;
                        continue;
                    }
                }

                // Never commit_reconciled before sink succeeds (flush below).
                ctx.push_change(event.change, ()).await?;
                if in_window {
                    buffer.remove(&event.pk.key());
                }
            }
        }
        // Drain reconciliation applies first (ordered sink), then flush surviving
        // snapshot rows via write_rows (same upsert path as before).
        ctx.flush().await?;
        flush_buffer(sink, &mut buffer, transforms).await?;

        after = last_pk.clone();
        progress[table_index].last_pk = last_pk;

        let table_done = chunk_len < config.chunk_size;
        if table_done {
            progress[table_index].done = true;
            source.on_table_snapshot_complete(&spec.table).await?;
        }

        let position = source.current_position().await?;
        checkpointer
            .save_progress(&build_checkpoint(&position, progress)?)
            .await?;
        source.commit_reconciled(position).await?;

        if table_done {
            break;
        }
    }

    Ok(())
}

/// Flush the surviving buffered rows as upserts and clear the buffer.
async fn flush_buffer<K: SurrealSink>(
    sink: &K,
    buffer: &mut HashMap<String, UniversalRow>,
    transforms: &SnapshotTransforms,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let rows: Vec<UniversalRow> = buffer.drain().map(|(_, row)| row).collect();
    write_rows(sink, &transforms.pipeline, rows, &transforms.apply_opts).await?;
    Ok(())
}
