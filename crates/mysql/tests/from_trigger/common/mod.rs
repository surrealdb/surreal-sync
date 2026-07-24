//! Shared helpers and test bodies for the interleaved-snapshot e2e tests.
//!
//! MySQL and MariaDB run the identical interleaved-snapshot code path (they
//! differ only by container image), so the test bodies live here parametrized by
//! [`Engine`], and `interleaved_snapshot.rs` / `mariadb_interleaved_snapshot.rs`
//! are thin `#[tokio::test]` wrappers that select the engine.
//!
//! `dead_code` is allowed because each test binary that includes this module via
//! `mod common;` only exercises one [`Engine`] variant, leaving the other
//! (intentionally) unconstructed in that binary.
#![allow(clippy::uninlined_format_args)]
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use mysql_async::{prelude::*, Pool};
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, RelationChange, Row, Value};
use surreal_sync_mysql::from_trigger::testing::MySQLContainer;
use surreal_sync_mysql::from_trigger::{read_table_chunk, MySqlWatermarkSource};
use surreal_sync_mysql::RowConversionConfig;
use surreal_sync_runtime::{
    run_interleaved_snapshot, InterleavedSnapshotCheckpoint, InterleavedSnapshotConfig,
    NoopCheckpointer, SnapshotCheckpointer, WatermarkSource,
};
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Which MySQL-compatible engine a test body runs against.
#[derive(Clone, Copy, Debug)]
pub enum Engine {
    MySql,
    MariaDb,
}

impl Engine {
    /// Construct (but do not start) the appropriate container for this engine.
    fn container(self, name: &str) -> MySQLContainer {
        match self {
            Engine::MySql => MySQLContainer::new(name),
            Engine::MariaDb => MySQLContainer::mariadb(name),
        }
    }
}

pub fn init_logging() {
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
type SinkState = BTreeMap<String, BTreeMap<String, HashMap<String, Value>>>;

#[derive(Default)]
pub struct MemSink {
    state: Mutex<SinkState>,
}

impl MemSink {
    pub async fn value_map(&self, table: &str, value_column: &str) -> BTreeMap<String, i64> {
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

pub fn id_key(id: &Value) -> String {
    serde_json::to_string(id).expect("serialize id")
}

#[async_trait::async_trait]
impl SurrealSink for MemSink {
    async fn write_rows(&self, rows: &[Row]) -> Result<()> {
        let mut state = self.state.lock().await;
        for row in rows {
            state
                .entry(row.table.clone())
                .or_default()
                .insert(id_key(&row.id), row.fields.clone());
        }
        Ok(())
    }

    async fn write_relations(&self, _relations: &[Relation]) -> Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> Result<()> {
        let mut state = self.state.lock().await;
        let key = id_key(&change.id);
        match change.operation {
            surreal_sync_core::ChangeOp::Create | surreal_sync_core::ChangeOp::Update => {
                state
                    .entry(change.table.clone())
                    .or_default()
                    .insert(key, change.fields.clone().unwrap_or_default());
            }
            surreal_sync_core::ChangeOp::Delete => {
                if let Some(rows) = state.get_mut(&change.table) {
                    rows.remove(&key);
                }
            }
        }
        Ok(())
    }

    async fn apply_relation_change(&self, _change: &RelationChange) -> Result<()> {
        Ok(())
    }
}

/// Canonicalize a primary-key scalar to the kinds the backend keys records by
/// (`Int64` / `Uuid` / `Text`), matching the source's internal normalization.
fn canon_scalar(value: &Value) -> Value {
    match value {
        Value::Int8 { value, .. } => Value::Int64(*value as i64),
        Value::Int16(v) => Value::Int64(*v as i64),
        Value::Int32(v) => Value::Int64(*v as i64),
        Value::Int64(v) => Value::Int64(*v),
        Value::Uuid(u) => Value::Uuid(*u),
        Value::Char { value, .. } => Value::Text(value.clone()),
        Value::VarChar { value, .. } => Value::Text(value.clone()),
        Value::Text(s) => Value::Text(s.clone()),
        other => other.clone(),
    }
}

fn canon_id_key(row: &Row, pk_len: usize) -> String {
    if pk_len == 1 {
        return id_key(&canon_scalar(&row.id));
    }
    match &row.id {
        Value::Array { elements, .. } => {
            let canon = Value::Array {
                elements: elements.iter().map(canon_scalar).collect(),
                element_type: Box::new(surreal_sync_core::Type::Text),
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
    let mut after: Option<Vec<Value>> = None;
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
        let events = source.next_reconciliation_events().await?;
        if events.is_empty() {
            break;
        }
        for event in events {
            if event.pk.single_uuid().is_some() {
                continue;
            }
            sink.apply_change(&event.change).await?;
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

/// Snapshot parity under concurrent writes, covering a non-`id` single-column
/// primary key and a composite primary key.
pub async fn run_parity_under_concurrent_writes(
    engine: Engine,
    container_name: &str,
) -> Result<()> {
    init_logging();

    let mut container = engine.container(container_name);
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

/// Bounded audit-table retention: consumed rows are pruned per chunk so the
/// audit table never grows one row per change.
pub async fn run_bounded_retention(engine: Engine, container_name: &str) -> Result<()> {
    init_logging();

    let mut container = engine.container(container_name);
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
pub async fn run_adhoc_snapshot(engine: Engine, container_name: &str) -> Result<()> {
    init_logging();

    let mut container = engine.container(container_name);
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
    surreal_sync_mysql::from_trigger::request_snapshot(
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
