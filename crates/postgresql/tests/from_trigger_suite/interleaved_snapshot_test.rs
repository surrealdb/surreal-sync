//! End-to-end tests for the PostgreSQL trigger interleaved snapshot backend.
//!
//! These exercise the real audit-table/trigger machinery against a Docker
//! PostgreSQL instance, but stay small and deterministic (a few hundred rows,
//! a tiny chunk size) so many windows are exercised in seconds. The sink is an
//! in-memory mock so no SurrealDB container is needed.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, ChangeOp, Relation, RelationChange, Row, Value};
use tokio_postgres::{Client, NoTls};

use surreal_sync_postgresql::from_trigger::{PostgresTriggerWatermarkSource, SourceOpts};
use surreal_sync_runtime::{
    run_interleaved_snapshot, InterleavedSnapshotConfig, NoopCheckpointer, WatermarkSource,
};

// ---------------------------------------------------------------------------
// In-memory sink that mirrors final row state (last write / change wins).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockSink {
    rows: Mutex<HashMap<String, HashMap<String, Value>>>,
}

fn id_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap()
}

impl MockSink {
    fn state(&self) -> HashMap<String, HashMap<String, Value>> {
        self.rows.lock().unwrap().clone()
    }

    fn len(&self) -> usize {
        self.rows.lock().unwrap().len()
    }
}

#[async_trait]
impl SurrealSink for MockSink {
    async fn write_rows(&self, rows: &[Row]) -> Result<()> {
        let mut state = self.rows.lock().unwrap();
        for row in rows {
            state.insert(id_key(&row.id), row.fields.clone());
        }
        Ok(())
    }

    async fn write_relations(&self, _relations: &[Relation]) -> Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> Result<()> {
        let mut state = self.rows.lock().unwrap();
        match change.operation {
            ChangeOp::Delete => {
                state.remove(&id_key(&change.id));
            }
            _ => {
                state.insert(
                    id_key(&change.id),
                    change.fields.clone().unwrap_or_default(),
                );
            }
        }
        Ok(())
    }

    async fn apply_relation_change(&self, _change: &RelationChange) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn connect(uri: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(uri, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn reset(admin: &Client, table: &str) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table} CASCADE;
             DROP TABLE IF EXISTS surreal_sync_changes CASCADE;
             DROP TABLE IF EXISTS surreal_sync_signal CASCADE;
             CREATE TABLE {table} (id BIGINT PRIMARY KEY, val TEXT);"
        ))
        .await
        .unwrap();
}

async fn seed(admin: &Client, table: &str, n: i64) {
    for i in 1..=n {
        let val = format!("v{i}");
        admin
            .execute(
                &format!("INSERT INTO {table} (id, val) VALUES ($1, $2)"),
                &[&i, &val],
            )
            .await
            .unwrap();
    }
}

fn opts(uri: &str) -> SourceOpts {
    SourceOpts {
        source_uri: uri.to_string(),
        source_database: Some("public".to_string()),
        tables: Vec::new(),
        relation_tables: Vec::new(),
    }
}

/// Drain any change-stream events the snapshot did not consume and apply them
/// to the sink. This stands in for the handoff to incremental/live processing,
/// making the final comparison deterministic. It also proves the audit pruning
/// never dropped an unapplied change (drained rows survived pruning).
async fn drain_remaining(source: &mut PostgresTriggerWatermarkSource, sink: &MockSink) {
    loop {
        let events = source.next_reconciliation_events().await.unwrap();
        if events.is_empty() {
            break;
        }
        for event in events {
            if event.pk.single_uuid().is_none() {
                sink.apply_change(&event.change).await.unwrap();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn interleaved_snapshot_parity_under_concurrent_writes() {
    let container = crate::shared::postgres().await;
    let uri = crate::shared::create_test_db(container, "ss_parity_db")
        .await
        .expect("create_test_db");
    let table = "ss_parity";

    let admin = connect(&uri).await;
    reset(&admin, table).await;

    let n: i64 = 200;
    seed(&admin, table, n).await;

    // Set up the watermark source AFTER seeding so the seed rows are pure
    // snapshot data (no audit rows) and only the concurrent writes flow through
    // the change stream.
    let mut source = PostgresTriggerWatermarkSource::setup(&opts(&uri))
        .await
        .unwrap();

    // Concurrent writer on its own connection: updates, inserts, deletes.
    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        let w = connect(&writer_uri).await;
        for i in 1..=50i64 {
            let val = format!("u{i}");
            w.execute("UPDATE ss_parity SET val = $2 WHERE id = $1", &[&i, &val])
                .await
                .unwrap();
        }
        for i in (n + 1)..=(n + 30) {
            let val = format!("v{i}");
            w.execute(
                "INSERT INTO ss_parity (id, val) VALUES ($1, $2)",
                &[&i, &val],
            )
            .await
            .unwrap();
        }
        for i in 80..=100i64 {
            w.execute("DELETE FROM ss_parity WHERE id = $1", &[&i])
                .await
                .unwrap();
        }
    });

    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 8 };
    let mut checkpointer = NoopCheckpointer;
    run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    writer.await.unwrap();

    // Apply any trailing changes the snapshot did not reach.
    drain_remaining(&mut source, &sink).await;

    // Expected final state straight from the source.
    let rows = admin
        .query(&format!("SELECT id, val FROM {table} ORDER BY id"), &[])
        .await
        .unwrap();
    let mut expected: HashMap<i64, String> = HashMap::new();
    for row in rows {
        expected.insert(row.get::<_, i64>(0), row.get::<_, String>(1));
    }

    let actual = sink.state();
    assert_eq!(
        actual.len(),
        expected.len(),
        "row count mismatch: sink={}, source={}",
        actual.len(),
        expected.len()
    );

    for (id, val) in expected {
        let key = id_key(&Value::Int64(id));
        let fields = actual
            .get(&key)
            .unwrap_or_else(|| panic!("sink missing id {id}"));
        match fields.get("val") {
            Some(Value::Text(s)) => {
                assert_eq!(s, &val, "value mismatch for id {id}")
            }
            other => panic!("unexpected val for id {id}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn interleaved_snapshot_bounded_audit_retention() {
    let container = crate::shared::postgres().await;
    let uri = crate::shared::create_test_db(container, "ss_retain_db")
        .await
        .expect("create_test_db");
    let table = "ss_retain";

    let admin = connect(&uri).await;
    reset(&admin, table).await;

    let n: i64 = 100;
    seed(&admin, table, n).await;

    let mut source = PostgresTriggerWatermarkSource::setup(&opts(&uri))
        .await
        .unwrap();

    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 8 };
    let mut checkpointer = NoopCheckpointer;
    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    // The final position is the audit table's max sequence_id, i.e. the total
    // number of rows ever inserted into the audit table (two watermark rows per
    // window, over ~13 windows here). Without pruning, the audit table would
    // retain all of them and keep growing one row per change.
    let final_pos = result.final_position;
    assert!(
        final_pos >= 20,
        "expected many watermark inserts to have flowed through the audit table, got {final_pos}"
    );

    // After catch-up, consumed rows have been pruned, so the surviving audit
    // row count stays bounded (and far below the number of rows that flowed).
    let remaining: i64 = admin
        .query_one("SELECT COUNT(*) FROM surreal_sync_changes", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        remaining <= config.chunk_size as i64,
        "audit retention not bounded: {remaining} rows remain (chunk_size={})",
        config.chunk_size
    );
    assert!(
        remaining < final_pos,
        "audit table should be far smaller than total inserts: remaining={remaining}, total={final_pos}"
    );

    // Correctness preserved: every row was copied.
    assert_eq!(sink.len(), n as usize, "all rows should be snapshotted");
}

#[tokio::test]
async fn interleaved_snapshot_peak_buffer_within_chunk_size() {
    let container = crate::shared::postgres().await;
    let uri = crate::shared::create_test_db(container, "ss_peak_db")
        .await
        .expect("create_test_db");
    let table = "ss_peak";

    let admin = connect(&uri).await;
    reset(&admin, table).await;

    let n: i64 = 120;
    seed(&admin, table, n).await;

    let mut source = PostgresTriggerWatermarkSource::setup(&opts(&uri))
        .await
        .unwrap();

    let sink = MockSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 16 };
    let mut checkpointer = NoopCheckpointer;
    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer)
        .await
        .unwrap();

    // The buffer never holds more than one chunk, and full chunks make the peak
    // exactly equal to chunk_size regardless of table size.
    assert!(
        result.peak_buffered_rows <= config.chunk_size,
        "peak {} exceeded chunk_size {}",
        result.peak_buffered_rows,
        config.chunk_size
    );
    assert_eq!(result.peak_buffered_rows, config.chunk_size);
    assert_eq!(sink.len(), n as usize);
}
