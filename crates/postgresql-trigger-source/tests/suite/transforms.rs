//! Transform pipeline e2e for PostgreSQL trigger source.
//!
//! Each test uses a unique database so parallel `cargo test` does not collide
//! on the shared `surreal_sync_changes` audit table.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql_trigger_source::{
    run_incremental_sync_with_transforms, PostgreSQLCheckpoint, PostgresIncrementalSource,
    ReplicationTailOptions, SourceOpts,
};
use sync_core::{
    UniversalChange, UniversalRelationChange, UniversalRow, UniversalValue,
};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline, FramerKind};
use tokio::sync::Mutex as TokioMutex;

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
    relation_changes: Mutex<Vec<UniversalRelationChange>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
            relation_changes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; keep observations in `changes`.
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            changes.push(UniversalChange::update(
                row.table.clone(),
                row.id.clone(),
                row.fields.clone(),
            ));
        }
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        relations: &[sync_core::UniversalRelation],
    ) -> anyhow::Result<()> {
        let mut relation_changes = self.relation_changes.lock().expect("lock");
        for relation in relations {
            relation_changes.push(UniversalRelationChange::update(relation.clone()));
        }
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        change: &sync_core::UniversalRelationChange,
    ) -> anyhow::Result<()> {
        self.relation_changes
            .lock()
            .expect("lock")
            .push(change.clone());
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

fn name_field(change: &UniversalChange) -> String {
    let data = change
        .data
        .as_ref()
        .unwrap_or_else(|| panic!("change missing data: {change:?}"));
    match data.get("name").unwrap_or_else(|| panic!("change missing name: {change:?}")) {
        UniversalValue::VarChar { value, .. } | UniversalValue::Text(value) => value.clone(),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn unique_suffix(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 1_000_000_000
    )
}

async fn prepare_table(conn_str: &str, table: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let client = Arc::new(TokioMutex::new(client));
    {
        let c = client.lock().await;
        c.execute(&format!("DROP TABLE IF EXISTS {table} CASCADE"), &[])
            .await?;
        c.execute("DROP TABLE IF EXISTS surreal_sync_changes CASCADE", &[])
            .await?;
        c.execute(
            &format!("CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(64))"),
            &[],
        )
        .await?;
    }
    let mut source = PostgresIncrementalSource::new(client, 0);
    source.setup_tracking(vec![table.to_string()]).await?;
    Ok(())
}

#[tokio::test]
async fn identity_pipeline_incremental_sync() -> Result<()> {
    let container = crate::shared::postgres().await;
    let db = unique_suffix("xf_id");
    let conn_str = crate::shared::create_test_db(container, &db).await?;
    let table = "people".to_string();
    prepare_table(&conn_str, &table).await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (1, 'alice'), (2, 'bob')"),
            &[],
        )
        .await?;

    let sink = CaptureSink::new();
    let source_opts = SourceOpts {
        source_uri: conn_str,
        source_database: Some("public".to_string()),
        tables: vec![table],
        relation_tables: vec![],
    };
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        &sink,
        source_opts,
        PostgreSQLCheckpoint {
            sequence_id: 0,
            timestamp: chrono::Utc::now(),
        },
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert!(
        changes.len() >= 2,
        "expected at least two inserts, got {changes:?}"
    );
    let mut names: Vec<_> = changes.iter().map(name_field).collect();
    names.sort();
    names.dedup();
    assert!(
        names.contains(&"alice".to_string()) && names.contains(&"bob".to_string()),
        "expected alice and bob, got {names:?}"
    );
    Ok(())
}

#[tokio::test]
async fn external_mutate_worker_transforms_incremental_changes() -> Result<()> {
    let worker = ensure_fixture_worker();
    let container = crate::shared::postgres().await;
    let db = unique_suffix("xf_ext");
    let conn_str = crate::shared::create_test_db(container, &db).await?;
    let table = "people".to_string();
    prepare_table(&conn_str, &table).await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (1, 'alice'), (2, 'bob')"),
            &[],
        )
        .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
        FramerKind::Ndjson,
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);
    let sink = CaptureSink::new();
    run_incremental_sync_with_transforms(
        &sink,
        SourceOpts {
            source_uri: conn_str,
            source_database: Some("public".to_string()),
            tables: vec![table],
            relation_tables: vec![],
        },
        PostgreSQLCheckpoint {
            sequence_id: 0,
            timestamp: chrono::Utc::now(),
        },
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert!(
        changes.len() >= 2,
        "expected transformed inserts, got {changes:?}"
    );
    for change in &changes {
        assert_eq!(
            name_field(change),
            "mutated",
            "external mutate worker should rewrite name; got {change:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn relation_change_path_records_junction_applies() -> Result<()> {
    let container = crate::shared::postgres().await;
    let db = unique_suffix("xf_rel");
    let conn_str = crate::shared::create_test_db(container, &db).await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let client = Arc::new(TokioMutex::new(client));
    {
        let c = client.lock().await;
        c.batch_execute(
            "
            DROP TABLE IF EXISTS surreal_sync_changes CASCADE;
            CREATE TABLE authors (id INT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE tags (id INT PRIMARY KEY, label TEXT NOT NULL);
            CREATE TABLE books (
                id INT PRIMARY KEY,
                title TEXT NOT NULL,
                author_id INT NOT NULL REFERENCES authors(id)
            );
            CREATE TABLE book_tags (
                book_id INT REFERENCES books(id),
                tag_id INT REFERENCES tags(id),
                PRIMARY KEY (book_id, tag_id)
            );
            ",
        )
        .await?;
    }

    let tables = vec![
        "authors".to_string(),
        "tags".to_string(),
        "books".to_string(),
        "book_tags".to_string(),
    ];
    let mut source = PostgresIncrementalSource::new(client.clone(), 0);
    source.setup_tracking(tables.clone()).await?;

    {
        let c = client.lock().await;
        c.batch_execute(
            "
            INSERT INTO authors (id, name) VALUES (1, 'Ada');
            INSERT INTO tags (id, label) VALUES (7, 'sci');
            INSERT INTO books (id, title, author_id) VALUES (10, 'Notes', 1);
            INSERT INTO book_tags (book_id, tag_id) VALUES (10, 7);
            ",
        )
        .await?;
    }

    let sink = CaptureSink::new();
    run_incremental_sync_with_transforms(
        &sink,
        SourceOpts {
            source_uri: conn_str,
            source_database: Some("public".to_string()),
            tables,
            relation_tables: vec![],
        },
        PostgreSQLCheckpoint {
            sequence_id: 0,
            timestamp: chrono::Utc::now(),
        },
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &Pipeline::new(),
        &ApplyOpts::identity(),
    )
    .await?;

    let relations = sink.relation_changes.lock().expect("lock").clone();
    assert!(
        !relations.is_empty(),
        "expected book_tags RelationChange applies via SourceDriver, got changes={:?} relations={:?}",
        sink.changes.lock().expect("lock"),
        relations
    );
    assert!(
        relations.iter().any(|r| r.relation.relation_type == "book_tags"),
        "expected book_tags relation edge, got {relations:?}"
    );
    Ok(())
}
