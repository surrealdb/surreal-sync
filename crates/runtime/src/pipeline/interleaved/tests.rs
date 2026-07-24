//! Unit tests for the interleaved snapshot loop using an in-memory mock source.
//!
//! These tests need no database: the mock source generates rows lazily and
//! scripts change-stream events directly, so the conflict (dedup) cases and
//! the bounded-memory guarantee run in microseconds.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, ChangeOp, Relation, RelationChange, Row, Value};
use uuid::Uuid;

use crate::pipeline::{
    run_interleaved_snapshot, InterleavedSnapshotConfig, PkTuple, ReconciliationEvent,
    SnapshotSignal, TableSpec, WatermarkKind, WatermarkSource,
};

// ---------------------------------------------------------------------------
// Mock sink
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockSink {
    state: Mutex<MockSinkState>,
}

#[derive(Default)]
struct MockSinkState {
    /// (op, table, id) of every sunk change-stream / buffer event.
    /// Surviving snapshot rows are emitted as [`ChangeOp::Update`]
    /// through the shared `run_source_runtime` window. Homogeneous Update
    /// batches coalesce to `write_rows`; those are mirrored here as
    /// Updates so observations match per-event apply.
    changes: Vec<(ChangeOp, String, Value)>,
}

impl MockSink {
    fn changes(&self) -> Vec<(ChangeOp, String, Value)> {
        self.state.lock().unwrap().changes.clone()
    }
}

#[async_trait]
impl SurrealSink for MockSink {
    async fn write_rows(&self, rows: &[Row]) -> Result<()> {
        // Homogeneous Update upserts coalesce here; mirror into `changes`.
        let mut state = self.state.lock().unwrap();
        for row in rows {
            state
                .changes
                .push((ChangeOp::Update, row.table.clone(), row.id.clone()));
        }
        Ok(())
    }

    async fn write_relations(&self, _relations: &[Relation]) -> Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .changes
            .push((change.operation, change.table.clone(), change.id.clone()));
        Ok(())
    }

    async fn apply_relation_change(&self, _change: &RelationChange) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Mock source
// ---------------------------------------------------------------------------

/// A scripted data change to emit within a window.
#[derive(Clone)]
struct DataEvent {
    table: String,
    pk: PkTuple,
    change: Change,
}

struct MockState {
    window_index: usize,
    pending_low: Option<Uuid>,
    pending_high: Option<Uuid>,
    position: i64,
    consumed: Vec<i64>,
}

/// In-memory [`WatermarkSource`] with an `i64` sequence position.
///
/// Rows are generated lazily on `read_chunk` (ids `1..=total_rows`), so a large
/// *logical* table costs nothing until read. `scripted[w]` holds the data
/// events emitted inside window `w`.
struct MockSource {
    spec: TableSpec,
    total_rows: i64,
    scripted: Vec<Vec<DataEvent>>,
    /// Events emitted in the same poll batch *after* the high watermark.
    /// Used to regress post-high CDC ordering against buffer flush.
    trailing: Vec<Vec<DataEvent>>,
    state: Mutex<MockState>,
}

impl MockSource {
    fn new(spec: TableSpec, total_rows: i64, scripted: Vec<Vec<DataEvent>>) -> Self {
        Self {
            spec,
            total_rows,
            scripted,
            trailing: Vec::new(),
            state: Mutex::new(MockState {
                window_index: 0,
                pending_low: None,
                pending_high: None,
                position: 0,
                consumed: Vec::new(),
            }),
        }
    }

    fn with_trailing(mut self, trailing: Vec<Vec<DataEvent>>) -> Self {
        self.trailing = trailing;
        self
    }

    /// A bulk table with no concurrent changes (every window is empty).
    fn bulk(spec: TableSpec, total_rows: i64) -> Self {
        Self::new(spec, total_rows, Vec::new())
    }

    fn watermark_event(state: &mut MockState, id: Uuid) -> ReconciliationEvent<i64> {
        state.position += 1;
        ReconciliationEvent {
            position: state.position,
            table: "surreal_sync_signal".to_string(),
            pk: PkTuple::new(vec![Value::Uuid(id)]),
            change: Change::create("surreal_sync_signal", Value::Uuid(id), HashMap::new()),
        }
    }
}

fn user_row(i: i64) -> Row {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), Value::Int64(i));
    fields.insert("val".to_string(), Value::Text(format!("v{i}")));
    Row::new("users", i as u64, Value::Int64(i), fields)
}

#[async_trait]
impl WatermarkSource for MockSource {
    type Position = i64;

    async fn snapshot_tables(&self) -> Result<Vec<TableSpec>> {
        Ok(vec![self.spec.clone()])
    }

    async fn read_chunk(
        &self,
        _table: &TableSpec,
        after: Option<&PkTuple>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        let start = after
            .and_then(|pk| pk.0.first().and_then(|v| v.as_i64()))
            .unwrap_or(0);
        let end = (start + limit as i64).min(self.total_rows);
        // Lazily generate only the requested window of rows.
        Ok(((start + 1)..=end).map(user_row).collect())
    }

    async fn write_watermark(&self, kind: WatermarkKind, id: Uuid) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        match kind {
            WatermarkKind::Low => state.pending_low = Some(id),
            WatermarkKind::High => state.pending_high = Some(id),
        }
        Ok(())
    }

    async fn next_reconciliation_events(&mut self) -> Result<Vec<ReconciliationEvent<i64>>> {
        let mut state = self.state.lock().unwrap();
        let mut out = Vec::new();

        if let Some(low) = state.pending_low.take() {
            out.push(Self::watermark_event(&mut state, low));
        }

        let window = if state.window_index < self.scripted.len() {
            self.scripted[state.window_index].clone()
        } else {
            Vec::new()
        };
        for event in window {
            state.position += 1;
            out.push(ReconciliationEvent {
                position: state.position,
                table: event.table,
                pk: event.pk,
                change: event.change,
            });
        }

        if let Some(high) = state.pending_high.take() {
            out.push(Self::watermark_event(&mut state, high));
            let trailing = if state.window_index < self.trailing.len() {
                self.trailing[state.window_index].clone()
            } else {
                Vec::new()
            };
            for event in trailing {
                state.position += 1;
                out.push(ReconciliationEvent {
                    position: state.position,
                    table: event.table,
                    pk: event.pk,
                    change: event.change,
                });
            }
            state.window_index += 1;
        }

        Ok(out)
    }

    async fn current_position(&self) -> Result<i64> {
        Ok(self.state.lock().unwrap().position)
    }

    async fn commit_reconciled(&mut self, position: i64) -> Result<()> {
        self.state.lock().unwrap().consumed.push(position);
        Ok(())
    }

    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>> {
        Ok(Vec::new())
    }

    async fn resolve_tables(&self, names: &[String]) -> Result<Vec<TableSpec>> {
        Ok(self
            .snapshot_tables()
            .await?
            .into_iter()
            .filter(|spec| names.iter().any(|n| n == &spec.table))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Conflict / dedup tests (the blog example)
// ---------------------------------------------------------------------------

fn update_event(i: i64) -> DataEvent {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), Value::Int64(i));
    fields.insert("val".to_string(), Value::Text(format!("v{i}-updated")));
    DataEvent {
        table: "users".to_string(),
        pk: PkTuple::new(vec![Value::Int64(i)]),
        change: Change::update("users", Value::Int64(i), fields),
    }
}

fn delete_event(i: i64) -> DataEvent {
    DataEvent {
        table: "users".to_string(),
        pk: PkTuple::new(vec![Value::Int64(i)]),
        change: Change::delete("users", Value::Int64(i)),
    }
}

fn create_event(i: i64) -> DataEvent {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), Value::Int64(i));
    fields.insert("val".to_string(), Value::Text(format!("v{i}")));
    DataEvent {
        table: "users".to_string(),
        pk: PkTuple::new(vec![Value::Int64(i)]),
        change: Change::create("users", Value::Int64(i), fields),
    }
}

#[tokio::test]
async fn window_dedup_lets_log_event_win() {
    // Rows 1,2,3 read in a single window. During the window: row 2 updated,
    // row 3 deleted, row 4 (new) created. Rows 2 and 3 must be dropped from the
    // snapshot buffer (the log event wins); row 1 (unchanged) must be flushed.
    let spec = TableSpec::new("users", vec!["id".to_string()]);
    let scripted = vec![vec![update_event(2), delete_event(3), create_event(4)]];
    let mut source = MockSource::new(spec, 3, scripted);
    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 16 };
    let mut checkpointer = crate::pipeline::NoopCheckpointer;

    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    // Surviving buffer row 1 is sunk as Update (coalesced write_rows);
    // CDC events for 2/3/4 are applied as-is. Watermark rows are never applied.
    let changes = sink.changes();
    assert_eq!(changes.len(), 4, "unexpected changes: {changes:?}");
    assert!(changes.iter().all(|(_, table, _)| table == "users"));
    assert!(changes
        .iter()
        .any(|(op, _, id)| *op == ChangeOp::Update && id.as_i64() == Some(1)));
    assert!(changes
        .iter()
        .any(|(op, _, id)| *op == ChangeOp::Update && id.as_i64() == Some(2)));
    assert!(changes
        .iter()
        .any(|(op, _, id)| *op == ChangeOp::Delete && id.as_i64() == Some(3)));
    assert!(changes
        .iter()
        .any(|(op, _, id)| *op == ChangeOp::Create && id.as_i64() == Some(4)));

    // The chunk held exactly the three read rows at its peak.
    assert_eq!(result.peak_buffered_rows, 3);
}

#[tokio::test]
async fn post_high_delete_wins_over_buffer_flush() {
    // Delete commits after the high watermark but arrives in the same poll
    // batch. Survivors must be queued before that Delete so the log wins and
    // the row is not resurrected by a stale buffer upsert.
    let spec = TableSpec::new("users", vec!["id".to_string()]);
    let mut source =
        MockSource::new(spec, 2, vec![vec![]]).with_trailing(vec![vec![delete_event(2)]]);
    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 16 };
    let mut checkpointer = crate::pipeline::NoopCheckpointer;

    run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    let changes = sink.changes();
    // Order: Update(1), Update(2) from buffer, then Delete(2).
    assert!(
        changes
            .iter()
            .any(|(op, _, id)| *op == ChangeOp::Update && id.as_i64() == Some(1)),
        "unexpected changes: {changes:?}"
    );
    let delete_pos = changes
        .iter()
        .position(|(op, _, id)| *op == ChangeOp::Delete && id.as_i64() == Some(2))
        .expect("Delete(2) missing");
    let upsert_2_pos = changes
        .iter()
        .position(|(op, _, id)| *op == ChangeOp::Update && id.as_i64() == Some(2))
        .expect("Update(2) missing");
    assert!(
        upsert_2_pos < delete_pos,
        "buffer Update(2) must precede post-high Delete(2); got {changes:?}"
    );
}

#[tokio::test]
async fn unchanged_keys_are_emitted_from_buffer() {
    // No concurrent changes: every snapshot row should be flushed verbatim.
    let spec = TableSpec::new("users", vec!["id".to_string()]);
    let mut source = MockSource::bulk(spec, 5);
    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 16 };
    let mut checkpointer = crate::pipeline::NoopCheckpointer;

    run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    let mut ids: Vec<i64> = sink
        .changes()
        .into_iter()
        .filter_map(|(op, _, id)| (op == ChangeOp::Update).then(|| id.as_i64()).flatten())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

// ---------------------------------------------------------------------------
// Bounded-memory test
// ---------------------------------------------------------------------------

async fn peak_for_table_size(total_rows: i64) -> (usize, usize) {
    let spec = TableSpec::new("users", vec!["id".to_string()]);
    let mut source = MockSource::bulk(spec, total_rows);
    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 4 };
    let mut checkpointer = crate::pipeline::NoopCheckpointer;

    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();
    let written = sink
        .changes()
        .iter()
        .filter(|(op, _, _)| *op == ChangeOp::Update)
        .count();
    (result.peak_buffered_rows, written)
}

#[tokio::test]
async fn bounded_memory_independent_of_table_size() {
    let (peak_small, written_small) = peak_for_table_size(1000).await;
    let (peak_large, _) = peak_for_table_size(5000).await;

    // Peak buffered rows equals chunk_size for both, proving independence from
    // table size.
    assert_eq!(peak_small, 4);
    assert_eq!(peak_large, 4);
    assert_eq!(peak_small, peak_large);

    // Correctness: every row was copied.
    assert_eq!(written_small, 1000);
}

// ---------------------------------------------------------------------------
// Checkpoint persistence / resumability test
// ---------------------------------------------------------------------------

#[cfg(feature = "checkpoint_fs")]
#[tokio::test]
async fn progress_and_handoff_checkpoints_persist() {
    use crate::checkpoint_fs::FilesystemStore;
    use surreal_sync_core::{InterleavedSnapshotCheckpoint, SyncManager, SyncPhase};
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let manager = SyncManager::new(FilesystemStore::new(tmp.path()));
    let mut checkpointer = crate::pipeline::ManagerCheckpointer::new(manager);

    let spec = TableSpec::new("users", vec!["id".to_string()]);
    let mut source = MockSource::bulk(spec, 10);
    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 4 };

    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    // The latest per-chunk progress checkpoint reports the table fully copied.
    let progress: InterleavedSnapshotCheckpoint = checkpointer
        .manager()
        .read_checkpoint(SyncPhase::SnapshotProgress)
        .await
        .unwrap();
    assert_eq!(progress.tables.len(), 1);
    assert_eq!(progress.tables[0].name, "users");
    assert!(progress.tables[0].done);
    assert!(progress.all_done());

    // A handoff checkpoint records the final stream position.
    let handoff = InterleavedSnapshotCheckpoint::new(
        serde_json::to_value(result.final_position).unwrap(),
        vec![],
    );
    checkpointer.save_handoff(&handoff).await.unwrap();
    let loaded: InterleavedSnapshotCheckpoint = checkpointer
        .manager()
        .read_checkpoint(SyncPhase::SnapshotHandoff)
        .await
        .unwrap();
    assert_eq!(
        loaded.reconciliation_pos,
        serde_json::to_value(result.final_position).unwrap()
    );
}
