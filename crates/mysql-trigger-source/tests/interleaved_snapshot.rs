//! End-to-end tests for the MySQL interleaved snapshot backend.
//!
//! These tests exercise the DBLog-style snapshot under concurrent writes
//! (including a non-`id` single-column primary key and a composite primary key,
//! to cover the trigger primary-key fix), the new bounded audit-table retention
//! (consumed rows pruned per chunk so the audit table never grows one row per
//! change), and the exact bounded-memory peak.
#![allow(clippy::uninlined_format_args)]

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use mysql_async::{prelude::*, Pool};
use mysql_types::RowConversionConfig;
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::{
    run_interleaved_snapshot, InterleavedSnapshotCheckpoint, InterleavedSnapshotConfig,
    NoopCheckpointer, SnapshotCheckpointer, WatermarkSource,
};
use surreal_sync_mysql_trigger_source::testing::MySQLContainer;
use surreal_sync_mysql_trigger_source::{read_table_chunk, MySqlWatermarkSource};
use sync_core::{
    UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow, UniversalValue,
};
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn init_logging() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}

/// In-memory [`SurrealSink`] that records the latest state per record, keyed by
/// the serialized record id, so tests can assert parity against the source.
/// table -> (serialized id -> non-key field map)
type SinkState = BTreeMap<String, BTreeMap<String, HashMap<String, UniversalValue>>>;

#[derive(Default)]
struct MemSink {
    state: Mutex<SinkState>,
}

impl MemSink {
    async fn value_map(&self, table: &str, value_column: &str) -> BTreeMap<String, i64> {
        let state = self.state.lock().await;
        let mut out = BTreeMap::new();
        if let Some(rows) = state.get(table) {
            for (key, fields) in rows {
                let v = fields
                    .get(value_column)
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                out.insert(key.clone(), v);
            }
        }
        out
    }
}

fn id_key(id: &UniversalValue) -> String {
    serde_json::to_string(id).expect("serialize id")
}

#[async_trait::async_trait]
impl SurrealSink for MemSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> Result<()> {
        let mut state = self.state.lock().await;
        for row in rows {
            state
                .entry(row.table.clone())
                .or_default()
                .insert(id_key(&row.id), row.fields.clone());
        }
        Ok(())
    }

    async fn write_universal_relations(&self, _relations: &[UniversalRelation]) -> Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> Result<()> {
        let mut state = self.state.lock().await;
        let key = id_key(&change.id);
        match change.operation {
            sync_core::UniversalChangeOp::Create | sync_core::UniversalChangeOp::Update => {
                state
                    .entry(change.table.clone())
                    .or_default()
                    .insert(key, change.data.clone().unwrap_or_default());
            }
            sync_core::UniversalChangeOp::Delete => {
                if let Some(rows) = state.get_mut(&change.table) {
                    rows.remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        _change: &UniversalRelationChange,
    ) -> Result<()> {
        Ok(())
    }
}

/// Canonicalize a primary-key scalar to the kinds the backend keys records by
/// (`Int64` / `Uuid` / `Text`), matching the source's internal normalization.
fn canon_scalar(value: &UniversalValue) -> UniversalValue {
    match value {
        UniversalValue::Int8 { value, .. } => UniversalValue::Int64(*value as i64),
        UniversalValue::Int16(v) => UniversalValue::Int64(*v as i64),
        UniversalValue::Int32(v) => UniversalValue::Int64(*v as i64),
        UniversalValue::Int64(v) => UniversalValue::Int64(*v),
        UniversalValue::Uuid(u) => UniversalValue::Uuid(*u),
        UniversalValue::Char { value, .. } => UniversalValue::Text(value.clone()),
        UniversalValue::VarChar { value, .. } => UniversalValue::Text(value.clone()),
        UniversalValue::Text(s) => UniversalValue::Text(s.clone()),
        other => other.clone(),
    }
}

fn canon_id_key(row: &UniversalRow, pk_len: usize) -> String {
    if pk_len == 1 {
        return id_key(&canon_scalar(&row.id));
    }
    match &row.id {
        UniversalValue::Array { elements, .. } => {
            let canon = UniversalValue::Array {
                elements: elements.iter().map(canon_scalar).collect(),
                element_type: Box::new(sync_core::UniversalType::Text),
            };
            id_key(&canon)
        }
        other => id_key(&canon_scalar(other)),
    }
}

/// Read every row of a source table directly and project the value column,
/// producing the expected parity map keyed the same way the sink keys records.
async fn source_value_map(
    pool: &Pool,
    table: &str,
    pk_columns: &[String],
    value_column: &str,
) -> Result<BTreeMap<String, i64>> {
    let mut conn = pool.get_conn().await?;
    let config = RowConversionConfig::default();
    let mut out = BTreeMap::new();
    let mut after: Option<Vec<UniversalValue>> = None;
    loop {
        let chunk =
            read_table_chunk(&mut conn, table, pk_columns, after.as_deref(), 64, &config).await?;
        if chunk.rows.is_empty() {
            break;
        }
        for row in &chunk.rows {
            let key = canon_id_key(row, pk_columns.len());
            let v = row
                .fields
                .get(value_column)
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            out.insert(key, v);
        }
        match chunk.last_pk {
            Some(pk) => after = Some(pk),
            None => break,
        }
    }
    Ok(out)
}

/// Drain any change-stream events remaining after the snapshot completes and
/// apply them to the sink (mirroring the handoff to incremental processing),
/// skipping watermark rows. Terminates once the stream is exhausted.
async fn drain_stream(source: &mut MySqlWatermarkSource, sink: &MemSink) -> Result<()> {
    loop {
        let events = source.next_stream_events().await?;
        if events.is_empty() {
            break;
        }
        for event in events {
            if event.pk.single_uuid().is_some() {
                continue;
            }
            sink.apply_universal_change(&event.change).await?;
        }
    }
    Ok(())
}

async fn create_schema(pool: &Pool) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE items (id INT PRIMARY KEY, val INT)")
        .await?;
    conn.query_drop("CREATE TABLE accounts (account_code VARCHAR(32) PRIMARY KEY, balance INT)")
        .await?;
    conn.query_drop("CREATE TABLE ledger (a INT, b INT, amount INT, PRIMARY KEY (a, b))")
        .await?;
    Ok(())
}

async fn insert_baseline(pool: &Pool, rows: i32) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    for i in 1..=rows {
        conn.exec_drop("INSERT INTO items (id, val) VALUES (?, ?)", (i, i * 10))
            .await?;
        conn.exec_drop(
            "INSERT INTO accounts (account_code, balance) VALUES (?, ?)",
            (format!("acct-{:03}", i), i * 100),
        )
        .await?;
    }
    // Composite key ledger: a in 1..=8, b in 1..=5 -> 40 rows.
    for a in 1..=8 {
        for b in 1..=5 {
            conn.exec_drop(
                "INSERT INTO ledger (a, b, amount) VALUES (?, ?, ?)",
                (a, b, a * 1000 + b),
            )
            .await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_mysql_interleaved_snapshot_parity_under_concurrent_writes() -> Result<()> {
    init_logging();

    let mut container = MySQLContainer::new("test-ss-parity");
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = Pool::from_url(&container.connection_string)?;
    let writer_pool = Pool::from_url(&container.connection_string)?;

    create_schema(&pool).await?;
    insert_baseline(&pool, 40).await?;

    // Triggers are installed here; writes after this point are captured.
    let mut source = MySqlWatermarkSource::new(pool.clone(), "testdb".to_string()).await?;

    let writer = tokio::spawn(async move {
        let mut conn = writer_pool.get_conn().await?;
        for i in 1..=10 {
            conn.exec_drop("UPDATE items SET val = val + 1000 WHERE id = ?", (i,))
                .await?;
            conn.exec_drop(
                "UPDATE accounts SET balance = balance + 1000 WHERE account_code = ?",
                (format!("acct-{:03}", i),),
            )
            .await?;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        for b in 1..=5 {
            conn.exec_drop(
                "UPDATE ledger SET amount = amount + 1000 WHERE a = 1 AND b = ?",
                (b,),
            )
            .await?;
        }
        for i in 100..=104 {
            conn.exec_drop("INSERT INTO items (id, val) VALUES (?, ?)", (i, i * 10))
                .await?;
            conn.exec_drop(
                "INSERT INTO accounts (account_code, balance) VALUES (?, ?)",
                (format!("acct-{:03}", i), i * 100),
            )
            .await?;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        for b in 1..=5 {
            conn.exec_drop(
                "INSERT INTO ledger (a, b, amount) VALUES (?, ?, ?)",
                (9, b, 9000 + b),
            )
            .await?;
        }
        for i in 38..=40 {
            conn.exec_drop("DELETE FROM items WHERE id = ?", (i,))
                .await?;
            conn.exec_drop(
                "DELETE FROM accounts WHERE account_code = ?",
                (format!("acct-{:03}", i),),
            )
            .await?;
        }
        conn.exec_drop("DELETE FROM ledger WHERE a = 8 AND b = ?", (5,))
            .await?;
        conn.exec_drop("DELETE FROM ledger WHERE a = 8 AND b = ?", (4,))
            .await?;
        Ok::<(), anyhow::Error>(())
    });

    let sink = MemSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 8 };
    let result =
        run_interleaved_snapshot(&mut source, &sink, &config, &mut NoopCheckpointer).await?;

    writer.await??;

    // Drain post-snapshot changes (handoff) so the sink reflects everything.
    drain_stream(&mut source, &sink).await?;

    // Bounded memory: the buffer never exceeds the chunk size.
    assert!(
        result.peak_buffered_rows <= config.chunk_size,
        "peak buffered rows {} exceeded chunk size {}",
        result.peak_buffered_rows,
        config.chunk_size
    );

    let checks = [
        ("items", vec!["id".to_string()], "val"),
        ("accounts", vec!["account_code".to_string()], "balance"),
        ("ledger", vec!["a".to_string(), "b".to_string()], "amount"),
    ];
    for (table, pk_columns, value_column) in checks {
        let expected = source_value_map(&pool, table, &pk_columns, value_column).await?;
        let actual = sink.value_map(table, value_column).await;
        assert_eq!(
            actual, expected,
            "parity mismatch for table '{}' (value column '{}')",
            table, value_column
        );
        assert!(!expected.is_empty(), "table '{}' produced no rows", table);
    }

    pool.disconnect().await?;
    container.stop()?;
    Ok(())
}

/// Checkpointer that samples the audit-table row count every time progress is
/// persisted (i.e. once per chunk, just before consumed rows are pruned).
struct CountingCheckpointer {
    pool: Pool,
    samples: Vec<i64>,
}

#[async_trait::async_trait]
impl SnapshotCheckpointer for CountingCheckpointer {
    async fn save_progress(&mut self, _checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()> {
        let mut conn = self.pool.get_conn().await?;
        let count: Option<i64> = conn
            .query_first("SELECT COUNT(*) FROM surreal_sync_changes")
            .await?;
        self.samples.push(count.unwrap_or_default());
        Ok(())
    }
}

#[tokio::test]
async fn test_mysql_interleaved_snapshot_bounded_retention() -> Result<()> {
    init_logging();

    let mut container = MySQLContainer::new("test-ss-retention");
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = Pool::from_url(&container.connection_string)?;

    {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, val INT)")
            .await?;
        for i in 1..=120 {
            conn.exec_drop("INSERT INTO widgets (id, val) VALUES (?, ?)", (i, i))
                .await?;
        }
    }

    let mut source = MySqlWatermarkSource::new(pool.clone(), "testdb".to_string()).await?;

    let sink = MemSink::default();
    let chunk_size = 8;
    let config = InterleavedSnapshotConfig { chunk_size };
    let mut checkpointer = CountingCheckpointer {
        pool: pool.clone(),
        samples: Vec::new(),
    };

    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer).await?;

    // Many windows were sampled (120 rows / chunk 8 -> ~16 chunks).
    assert!(
        checkpointer.samples.len() >= 10,
        "expected many chunk samples, got {}",
        checkpointer.samples.len()
    );

    // Retention is bounded: the audit table holds at most a few rows per window
    // (the current low/high watermarks), independent of how many windows ran.
    // Without per-chunk pruning it would grow to ~2 rows per window.
    let max_count = checkpointer
        .samples
        .iter()
        .copied()
        .max()
        .unwrap_or_default();
    let bound = chunk_size as i64 + 4;
    assert!(
        max_count <= bound,
        "audit table retention {} exceeded bound {} across {} windows",
        max_count,
        bound,
        checkpointer.samples.len()
    );
    assert!(
        (checkpointer.samples.len() as i64) * 2 > bound,
        "test workload too small to demonstrate bounded retention"
    );

    // Bounded memory holds here too.
    assert!(result.peak_buffered_rows <= chunk_size);

    // Correctness preserved: every snapshot row reached the sink.
    let expected = source_value_map(&pool, "widgets", &["id".to_string()], "val").await?;
    let actual = sink.value_map("widgets", "val").await;
    assert_eq!(actual, expected, "snapshot parity mismatch for 'widgets'");
    assert_eq!(expected.len(), 120);

    pool.disconnect().await?;
    container.stop()?;
    Ok(())
}

/// Ad-hoc snapshot: an `execute-snapshot` signal requests a table that was not
/// part of the initial snapshot set (it is created after the sync starts). The
/// running snapshot loop must pick the signal up, resolve the table, and copy
/// it with the same watermark-window machinery without interrupting the stream.
#[tokio::test]
async fn test_mysql_adhoc_snapshot_adds_table_mid_stream() -> Result<()> {
    init_logging();

    let mut container = MySQLContainer::new("test-ss-adhoc");
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = Pool::from_url(&container.connection_string)?;

    // Only `items` exists when the sync starts, so it is the sole table in the
    // initial snapshot set.
    {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("CREATE TABLE items (id INT PRIMARY KEY, val INT)")
            .await?;
        for i in 1..=20 {
            conn.exec_drop("INSERT INTO items (id, val) VALUES (?, ?)", (i, i * 10))
                .await?;
        }
    }

    let mut source = MySqlWatermarkSource::new(pool.clone(), "testdb".to_string()).await?;
    let initial: Vec<String> = source
        .snapshot_tables()
        .await?
        .into_iter()
        .map(|t| t.table)
        .collect();
    assert_eq!(
        initial,
        vec!["items".to_string()],
        "ad-hoc table must not be part of the initial snapshot set"
    );

    // An operator introduces a brand-new table after the sync has started and
    // requests an ad-hoc snapshot of it via an execute-snapshot signal.
    {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("CREATE TABLE extra (id INT PRIMARY KEY, score INT)")
            .await?;
        for i in 1..=15 {
            conn.exec_drop("INSERT INTO extra (id, score) VALUES (?, ?)", (i, i * 7))
                .await?;
        }
    }
    surreal_sync_mysql_trigger_source::request_snapshot(
        Pool::from_url(&container.connection_string)?,
        "testdb".to_string(),
        &["extra".to_string()],
    )
    .await?;

    let sink = MemSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 8 };
    let result =
        run_interleaved_snapshot(&mut source, &sink, &config, &mut NoopCheckpointer).await?;

    // Bounded memory holds across both the initial and the ad-hoc table.
    assert!(result.peak_buffered_rows <= config.chunk_size);

    // The initial table is fully snapshotted.
    let items_expected = source_value_map(&pool, "items", &["id".to_string()], "val").await?;
    let items_actual = sink.value_map("items", "val").await;
    assert_eq!(items_actual, items_expected, "parity mismatch for 'items'");
    assert_eq!(items_expected.len(), 20);

    // The ad-hoc requested table was discovered mid-stream and fully copied.
    let extra_expected = source_value_map(&pool, "extra", &["id".to_string()], "score").await?;
    let extra_actual = sink.value_map("extra", "score").await;
    assert_eq!(
        extra_actual, extra_expected,
        "parity mismatch for ad-hoc table 'extra'"
    );
    assert_eq!(extra_expected.len(), 15);

    pool.disconnect().await?;
    container.stop()?;
    Ok(())
}
