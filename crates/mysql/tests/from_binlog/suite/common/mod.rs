//! Shared helpers for binlog interleaved-snapshot e2e tests.
#![allow(clippy::uninlined_format_args)]
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use mysql_async::{prelude::*, Pool};
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, RelationChange, Row, Value};
use surreal_sync_mysql::from_binlog::{
    request_snapshot, BinlogWatermarkSource, SourceOpts, SIGNAL_TABLE,
};
use surreal_sync_mysql::read_table_chunk;
use surreal_sync_mysql::RowConversionConfig;
use surreal_sync_runtime::{
    run_interleaved_snapshot, InterleavedSnapshotConfig, NoopCheckpointer, WatermarkSource,
};
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::shared::BinlogContainer;

#[derive(Clone, Copy, Debug)]
pub enum Engine {
    MySql,
    MariaDb,
}

impl Engine {
    fn container(self, name: &str) -> BinlogContainer {
        match self {
            Engine::MySql => BinlogContainer::mysql(name),
            Engine::MariaDb => BinlogContainer::mariadb(name),
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
    serde_json::to_string(&canon_scalar(id)).expect("serialize id")
}

#[async_trait::async_trait]
impl SurrealSink for MemSink {
    async fn write_rows(&self, rows: &[Row]) -> Result<()> {
        let mut state = self.state.lock().await;
        for row in rows {
            let pk_len = match &row.id {
                Value::Array { elements, .. } => elements.len(),
                _ => 1,
            };
            state
                .entry(row.table.clone())
                .or_default()
                .insert(canon_id_key(row, pk_len), row.fields.clone());
        }
        Ok(())
    }

    async fn write_relations(&self, _relations: &[Relation]) -> Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> Result<()> {
        let mut state = self.state.lock().await;
        let key = canon_change_id_key(&change.id);
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

fn canon_change_id_key(id: &Value) -> String {
    match id {
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

async fn drain_stream(source: &mut BinlogWatermarkSource, sink: &MemSink) -> Result<()> {
    let mut idle = 0u32;
    loop {
        let events = source.next_reconciliation_events().await?;
        if events.is_empty() {
            idle += 1;
            if idle >= 80 {
                break;
            }
            continue;
        }
        idle = 0;
        for event in events {
            if event.table == SIGNAL_TABLE {
                continue;
            }
            sink.apply_change(&event.change).await?;
        }
    }
    Ok(())
}

fn source_opts(conn: &str) -> SourceOpts {
    SourceOpts {
        connection_string: conn.to_string(),
        database: Some("testdb".to_string()),
        tables: vec![],
        server_id: Some(9_010_001),
        flavor: None,
        ssl: surreal_sync_mysql::from_binlog::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql::from_binlog::MariaDbGtidStrictMode::ServerDefault,
    }
}

fn rand_server_id() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u32;
    (seed % 900_000) + 100_000
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

pub async fn run_parity_under_concurrent_writes(
    engine: Engine,
    container_name: &str,
) -> Result<()> {
    init_logging();

    let mut container = engine.container(container_name);
    container.start()?;
    container.wait_until_ready(60).await?;
    crate::shared::ensure_binlog_repl_user(&container.connection_string, container.flavor())
        .await?;

    let pool = Pool::from_url(&container.connection_string)?;
    let writer_pool = Pool::from_url(&container.connection_string)?;

    create_schema(&pool).await?;
    insert_baseline(&pool, 40).await?;

    let mut source =
        BinlogWatermarkSource::connect(&source_opts(&container.connection_string)).await?;

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
    drain_stream(&mut source, &sink).await?;

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
        assert_eq!(actual, expected, "parity mismatch for table '{table}'");
        assert!(!expected.is_empty(), "table '{table}' produced no rows");
    }

    pool.disconnect().await?;
    container.stop()?;
    Ok(())
}

pub async fn run_bounded_retention(engine: Engine, container_name: &str) -> Result<()> {
    init_logging();

    let mut container = engine.container(container_name);
    container.start()?;
    container.wait_until_ready(60).await?;
    crate::shared::ensure_binlog_repl_user(&container.connection_string, container.flavor())
        .await?;

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

    let mut source =
        BinlogWatermarkSource::connect(&source_opts(&container.connection_string)).await?;
    let start_pos = source.current_position().await?;

    let sink = MemSink::default();
    let chunk_size = 8;
    let config = InterleavedSnapshotConfig { chunk_size };
    let result =
        run_interleaved_snapshot(&mut source, &sink, &config, &mut NoopCheckpointer).await?;

    assert!(result.peak_buffered_rows <= chunk_size);
    let end_pos = source.current_position().await?;
    assert!(
        end_pos > start_pos,
        "binlog position should advance during snapshot ({start_pos:?} -> {end_pos:?})"
    );

    let expected = source_value_map(&pool, "widgets", &["id".to_string()], "val").await?;
    let actual = sink.value_map("widgets", "val").await;
    assert_eq!(actual, expected);
    assert_eq!(expected.len(), 120);

    pool.disconnect().await?;
    container.stop()?;
    Ok(())
}

pub async fn run_adhoc_snapshot(engine: Engine, container_name: &str) -> Result<()> {
    init_logging();

    let mut container = engine.container(container_name);
    container.start()?;
    container.wait_until_ready(60).await?;
    crate::shared::ensure_binlog_repl_user(&container.connection_string, container.flavor())
        .await?;

    let pool = Pool::from_url(&container.connection_string)?;
    {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("CREATE TABLE items (id INT PRIMARY KEY, val INT)")
            .await?;
        for i in 1..=20 {
            conn.exec_drop("INSERT INTO items (id, val) VALUES (?, ?)", (i, i * 10))
                .await?;
        }
    }

    let mut source =
        BinlogWatermarkSource::connect(&source_opts(&container.connection_string)).await?;
    let initial: Vec<String> = source
        .snapshot_tables()
        .await?
        .into_iter()
        .map(|t| t.table)
        .collect();
    assert_eq!(initial, vec!["items".to_string()]);

    {
        let mut conn = pool.get_conn().await?;
        conn.query_drop("CREATE TABLE extra (id INT PRIMARY KEY, score INT)")
            .await?;
        for i in 1..=15 {
            conn.exec_drop("INSERT INTO extra (id, score) VALUES (?, ?)", (i, i * 7))
                .await?;
        }
    }
    request_snapshot(&pool, "testdb", &["extra".to_string()]).await?;

    let sink = MemSink::default();
    let config = InterleavedSnapshotConfig { chunk_size: 8 };
    let result =
        run_interleaved_snapshot(&mut source, &sink, &config, &mut NoopCheckpointer).await?;
    assert!(result.peak_buffered_rows <= config.chunk_size);

    let items_expected = source_value_map(&pool, "items", &["id".to_string()], "val").await?;
    let items_actual = sink.value_map("items", "val").await;
    assert_eq!(items_actual, items_expected);

    let extra_expected = source_value_map(&pool, "extra", &["id".to_string()], "score").await?;
    let extra_actual = sink.value_map("extra", "score").await;
    assert_eq!(extra_actual, extra_expected);
    assert_eq!(extra_expected.len(), 15);

    pool.disconnect().await?;
    container.stop()?;
    Ok(())
}
