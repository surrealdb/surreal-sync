//! The generic DBLog-style watermark snapshot loop.

use std::collections::HashMap;

use anyhow::Result;
use checkpoint::{SnapshotStreamCheckpoint, SnapshotTableProgress};
use surreal_sink::SurrealSink;
use sync_core::UniversalRow;
use uuid::Uuid;

use crate::checkpointer::SnapshotCheckpointer;
use crate::source::WatermarkSource;
use crate::types::{PkTuple, StreamPosition, WatermarkKind};

/// Default chunk size (matches Debezium's incremental snapshot default).
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Configuration for a watermark snapshot run.
#[derive(Debug, Clone)]
pub struct SnapshotStreamConfig {
    /// Maximum number of rows read per chunk (the `LIMIT` of each keyset read).
    /// This structurally bounds the loop's buffered memory.
    pub chunk_size: usize,
}

impl Default for SnapshotStreamConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

/// Outcome of a completed watermark snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotStreamResult<P: StreamPosition> {
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

fn build_checkpoint<P: StreamPosition>(
    position: &P,
    progress: &[TableState],
) -> Result<SnapshotStreamCheckpoint> {
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
    Ok(SnapshotStreamCheckpoint {
        stream_pos: serde_json::to_value(position)?,
        tables,
    })
}

/// Run a watermark snapshot, copying every table reported by `source` in
/// primary-key-ordered chunks while concurrently consuming and applying the
/// source's change stream to `sink`.
///
/// For each chunk the loop:
/// 1. writes a low watermark, reads the chunk into a primary-key-keyed buffer,
///    and writes a high watermark;
/// 2. consumes change-stream events, applying every data change to the sink,
///    and while inside the open window drops any buffered row whose primary
///    key also appears as an event (the log event wins);
/// 3. on window close, flushes the surviving buffered rows as upserts;
/// 4. checkpoints `{stream_pos, per-table last_pk}` and calls
///    [`WatermarkSource::commit_consumed`] so the source can free applied
///    change-log data.
///
/// On completion it returns the final stream position and the exact peak
/// buffered-row count.
pub async fn run_snapshot_stream<S, K, C>(
    source: &mut S,
    sink: &K,
    config: &SnapshotStreamConfig,
    checkpointer: &mut C,
) -> Result<SnapshotStreamResult<S::Position>>
where
    S: WatermarkSource,
    K: SurrealSink,
    C: SnapshotCheckpointer,
{
    let tables = source.snapshot_tables().await?;
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
        let mut after: Option<PkTuple> = None;

        loop {
            // Open the dedup window before reading the chunk.
            let low_id = Uuid::new_v4();
            source.write_watermark(WatermarkKind::Low, low_id).await?;

            let rows = source
                .read_chunk(spec, after.as_ref(), config.chunk_size)
                .await?;

            if rows.is_empty() {
                progress[table_index].done = true;
                let position = source.current_position().await?;
                checkpointer
                    .save_progress(&build_checkpoint(&position, &progress)?)
                    .await?;
                source.commit_consumed(position).await?;
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
                if buffer.len() > peak_buffered_rows {
                    peak_buffered_rows = buffer.len();
                }
            }

            // Close the dedup window after the chunk is read.
            let high_id = Uuid::new_v4();
            source.write_watermark(WatermarkKind::High, high_id).await?;

            // Consume the stream until the high watermark passes by. Every
            // data change is applied to the sink; while in-window, any buffered
            // row touched by an event is dropped so the log event wins.
            let mut in_window = false;
            let mut window_closed = false;
            while !window_closed {
                let events = source.next_stream_events().await?;
                for event in events {
                    if let Some(watermark_id) = event.pk.single_uuid() {
                        if watermark_id == low_id {
                            in_window = true;
                            continue;
                        }
                        if watermark_id == high_id {
                            flush_buffer(sink, &mut buffer).await?;
                            in_window = false;
                            window_closed = true;
                            continue;
                        }
                    }

                    sink.apply_universal_change(&event.change).await?;
                    if in_window {
                        buffer.remove(&event.pk.key());
                    }
                }
            }

            after = last_pk.clone();
            progress[table_index].last_pk = last_pk;

            let table_done = chunk_len < config.chunk_size;
            if table_done {
                progress[table_index].done = true;
            }

            let position = source.current_position().await?;
            checkpointer
                .save_progress(&build_checkpoint(&position, &progress)?)
                .await?;
            source.commit_consumed(position).await?;

            if table_done {
                break;
            }
        }
    }

    let final_position = source.current_position().await?;
    Ok(SnapshotStreamResult {
        final_position,
        peak_buffered_rows,
    })
}

/// Flush the surviving buffered rows as upserts and clear the buffer.
async fn flush_buffer<K: SurrealSink>(
    sink: &K,
    buffer: &mut HashMap<String, UniversalRow>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let rows: Vec<UniversalRow> = buffer.drain().map(|(_, row)| row).collect();
    sink.write_universal_rows(&rows).await?;
    Ok(())
}
