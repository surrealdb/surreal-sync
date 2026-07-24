//! Transform pipeline e2e for MySQL trigger source.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use anyhow::Result;
use mysql_async::prelude::*;
use surreal_sink::SurrealSink;
use surreal_sync_mysql_trigger_source::{
    run_incremental_sync_with_transforms, setup_mysql_change_tracking, MySQLCheckpoint,
    ReplicationTailOptions, SourceOpts,
};
use sync_core::{Change, Row, Value};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};

mod common;

struct CaptureSink {
    changes: Mutex<Vec<Change>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_rows(&self, rows: &[Row]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; keep observations in `changes`.
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            changes.push(Change::update(
                row.table.clone(),
                row.id.clone(),
                row.fields.clone(),
            ));
        }
        Ok(())
    }

    async fn write_relations(&self, _relations: &[sync_core::Relation]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_relation_change(
        &self,
        _change: &sync_core::RelationChange,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

fn fixture_worker_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p.push("target/debug/sync-transform-fixture-worker");
    p
}

fn ensure_fixture_worker() -> PathBuf {
    let path = fixture_worker_path();
    if !path.is_file() {
        let status = Command::new("cargo")
            .args([
                "build",
                "-p",
                "sync-transform",
                "--bin",
                "sync-transform-fixture-worker",
            ])
            .current_dir({
                let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                root.pop();
                root.pop();
                root
            })
            .status()
            .expect("spawn cargo build");
        assert!(status.success());
    }
    path
}

fn name_field(change: &Change) -> Option<String> {
    let data = change.fields.as_ref()?;
    match data.get("name")? {
        Value::VarChar { value, .. } | Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

#[tokio::test]
async fn identity_and_external_mutate_incremental() -> Result<()> {
    common::init_logging();
    let worker = ensure_fixture_worker();

    let name = format!("mysql-trigger-xf-{}", std::process::id());
    let mut container = surreal_sync_mysql_trigger_source::testing::MySQLContainer::new(&name);
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = surreal_sync_mysql_trigger_source::new_mysql_pool(&container.connection_string)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("DROP TABLE IF EXISTS people").await?;
    conn.query_drop("DROP TABLE IF EXISTS surreal_sync_changes")
        .await?;
    conn.query_drop("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;
    setup_mysql_change_tracking(&mut conn, "testdb").await?;

    let source_opts = SourceOpts {
        source_uri: container.connection_string.clone(),
        source_database: Some("testdb".to_string()),
        tables: vec!["people".to_string()],
        mysql_boolean_paths: None,
        id_column_overrides: Default::default(),
        ssl: Default::default(),
    };
    let from_checkpoint = MySQLCheckpoint {
        sequence_id: 0,
        timestamp: chrono::Utc::now(),
    };

    conn.query_drop("INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .await?;

    let sink = CaptureSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        &sink,
        source_opts.clone(),
        from_checkpoint.clone(),
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;
    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(changes.len(), 2, "identity: {changes:?}");

    conn.query_drop("INSERT INTO people (id, name) VALUES (3, 'carol'), (4, 'dave')")
        .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![worker.to_string_lossy().into_owned(), "mutate".to_string()],
        FramerKind::Ndjson,
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);
    let sink = CaptureSink::new();
    let from_checkpoint = MySQLCheckpoint {
        sequence_id: 2,
        timestamp: chrono::Utc::now(),
    };
    run_incremental_sync_with_transforms(
        &sink,
        source_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;
    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(changes.len(), 2, "mutate: {changes:?}");
    for change in &changes {
        assert_eq!(name_field(change).as_deref(), Some("mutated"));
    }

    Ok(())
}

/// SurrealDB e2e: non-relation entity table with `PRIMARY KEY (a, b)` writes
/// array record IDs. Extreme `:` / `_` string parts prove Arrays do not collide.
#[tokio::test]
async fn composite_pk_entity_writes_surreal_array_record_ids() -> Result<()> {
    common::init_logging();

    let name = format!("mysql-trigger-cpk-{}", std::process::id());
    let mut container = surreal_sync_mysql_trigger_source::testing::MySQLContainer::new(&name);
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = surreal_sync_mysql_trigger_source::new_mysql_pool(&container.connection_string)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("DROP TABLE IF EXISTS ledger").await?;
    conn.query_drop(
        "CREATE TABLE ledger (
            a VARCHAR(64) NOT NULL,
            b VARCHAR(64) NOT NULL,
            amount INT NOT NULL,
            PRIMARY KEY (a, b)
        )",
    )
    .await?;
    conn.query_drop(
        "INSERT INTO ledger (a, b, amount) VALUES
            ('x:y', 'z', 1),
            ('x', 'y:z', 2),
            ('a_b', 'c', 3),
            ('a', 'b_c', 4),
            ('1', '2', 10)",
    )
    .await?;
    drop(conn);

    let surreal = surrealdb::engine::any::connect("memory").await?;
    surreal.use_ns("test").use_db("test").await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

    let source_opts = SourceOpts {
        source_uri: container.connection_string.clone(),
        source_database: Some("testdb".to_string()),
        tables: vec!["ledger".to_string()],
        mysql_boolean_paths: None,
        id_column_overrides: Default::default(),
        ssl: Default::default(),
    };
    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: 100,
        dry_run: false,
    };

    surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
        &sink,
        &source_opts,
        &sync_opts,
        None,
    )
    .await?;

    use surrealdb::sql::{Id, Thing, Value as SqlValue};

    #[derive(Debug, serde::Deserialize)]
    struct LedgerRow {
        id: Thing,
        amount: i64,
    }

    let mut resp = surreal.query("SELECT id, amount FROM ledger").await?;
    let rows: Vec<LedgerRow> = resp.take(0)?;
    assert_eq!(
        rows.len(),
        5,
        "array record IDs must not collide under :/_ : {rows:?}"
    );

    let mut by_key: Vec<(Vec<String>, i64)> = rows
        .into_iter()
        .map(|row| {
            let key = match row.id.id {
                Id::Array(arr) => arr
                    .0
                    .into_iter()
                    .map(|v| match v {
                        SqlValue::Strand(s) => s.as_string(),
                        SqlValue::Number(n) => n.to_string(),
                        other => panic!("unexpected array id element: {other:?}"),
                    })
                    .collect(),
                other => panic!("expected Id::Array for composite PK, got {other:?}"),
            };
            (key, row.amount)
        })
        .collect();
    by_key.sort_by(|a, b| a.0.cmp(&b.0));

    assert!(by_key
        .iter()
        .any(|(k, a)| k == &vec!["x:y".to_string(), "z".to_string()] && *a == 1));
    assert!(by_key
        .iter()
        .any(|(k, a)| k == &vec!["x".to_string(), "y:z".to_string()] && *a == 2));
    assert!(by_key
        .iter()
        .any(|(k, a)| k == &vec!["a_b".to_string(), "c".to_string()] && *a == 3));
    assert!(by_key
        .iter()
        .any(|(k, a)| k == &vec!["a".to_string(), "b_c".to_string()] && *a == 4));
    assert!(by_key
        .iter()
        .any(|(k, a)| k == &vec!["1".to_string(), "2".to_string()] && *a == 10));

    Ok(())
}

/// `flatten_id` TOML turns composite Array IDs into Text `a:b` before Surreal write.
#[tokio::test]
async fn flatten_id_toml_writes_surreal_text_record_ids() -> Result<()> {
    common::init_logging();

    let name = format!("mysql-trigger-flat-{}", std::process::id());
    let mut container = surreal_sync_mysql_trigger_source::testing::MySQLContainer::new(&name);
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = surreal_sync_mysql_trigger_source::new_mysql_pool(&container.connection_string)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("DROP TABLE IF EXISTS ledger").await?;
    conn.query_drop(
        "CREATE TABLE ledger (
            a INT NOT NULL,
            b INT NOT NULL,
            amount INT NOT NULL,
            PRIMARY KEY (a, b)
        )",
    )
    .await?;
    conn.query_drop("INSERT INTO ledger (a, b, amount) VALUES (1, 2, 99)")
        .await?;
    drop(conn);

    let surreal = surrealdb::engine::any::connect("memory").await?;
    surreal.use_ns("test").use_db("test").await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

    let source_opts = SourceOpts {
        source_uri: container.connection_string.clone(),
        source_database: Some("testdb".to_string()),
        tables: vec!["ledger".to_string()],
        mysql_boolean_paths: None,
        id_column_overrides: Default::default(),
        ssl: Default::default(),
    };
    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: 100,
        dry_run: false,
    };

    let cfg = sync_transform::parse_transforms_toml(
        r#"
[[transforms]]
type = "flatten_id"
"#,
    )?;
    let pipeline = sync_transform::Pipeline::from_config(&cfg)?;
    let apply_opts = sync_transform::ApplyOpts::from_transforms_config(&cfg);

    surreal_sync_mysql_trigger_source::run_full_sync_with_transforms::<_, checkpoint::NullStore>(
        &sink,
        &source_opts,
        &sync_opts,
        None,
        &pipeline,
        &apply_opts,
    )
    .await?;

    use surrealdb::sql::{Id, Thing};

    #[derive(Debug, serde::Deserialize)]
    struct LedgerRow {
        id: Thing,
        amount: i64,
    }

    let mut resp = surreal.query("SELECT id, amount FROM ledger").await?;
    let rows: Vec<LedgerRow> = resp.take(0)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].amount, 99);
    match &rows[0].id.id {
        Id::String(s) => assert_eq!(s, "1:2"),
        other => panic!("expected flattened Text id \"1:2\", got {other:?}"),
    }

    Ok(())
}
