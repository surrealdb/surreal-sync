//! Transform pipeline e2e: identity path + external worker against real PostgreSQL (wal2json).

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use anyhow::Result;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql_wal2json_source::{
    run_full_sync_with_transforms, run_incremental_sync_with_transforms, Client,
    PostgreSQLLogicalCheckpoint, ReplicationTailOptions, SourceOpts,
};
use sync_core::{Change, Row, Value};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};

struct CaptureSink {
    changes: Mutex<Vec<Change>>,
    rows: Mutex<Vec<Row>>,
    relation_changes: Mutex<Vec<sync_core::RelationChange>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
            rows: Mutex::new(Vec::new()),
            relation_changes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_rows(&self, rows: &[Row]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; mirror into `changes` for
        // incremental assertions (full sync still reads `rows`).
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
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

    async fn write_relations(&self, relations: &[sync_core::Relation]) -> anyhow::Result<()> {
        let mut relation_changes = self.relation_changes.lock().expect("lock");
        for relation in relations {
            relation_changes.push(sync_core::RelationChange::update(relation.clone()));
        }
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_relation_change(
        &self,
        change: &sync_core::RelationChange,
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
    p.pop(); // crates
    p.pop(); // workspace root
    p.push("target");
    p.push("debug");
    p.push("sync-transform-fixture-worker");
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
            .expect("spawn cargo build fixture worker");
        assert!(
            status.success(),
            "failed to build sync-transform-fixture-worker"
        );
    }
    assert!(
        path.is_file(),
        "fixture worker missing after build: {}",
        path.display()
    );
    path
}

fn name_field(change: &Change) -> Option<String> {
    let data = change.fields.as_ref()?;
    match data.get("name")? {
        Value::VarChar { value, .. } | Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name value: {other:?}"),
    }
}

fn row_name_field(row: &Row) -> Option<String> {
    match row.fields.get("name")? {
        Value::VarChar { value, .. } | Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name value: {other:?}"),
    }
}

fn source_opts(conn_str: &str, slot: &str, tables: Vec<String>) -> SourceOpts {
    SourceOpts {
        connection_string: conn_str.to_string(),
        slot_name: slot.to_string(),
        tables,
        schema: "public".to_string(),
        relation_tables: vec![],
    }
}

async fn capture_head(
    conn_str: &str,
    slot: &str,
    tables: Vec<String>,
) -> Result<PostgreSQLLogicalCheckpoint> {
    let (pg_client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let client = Client::new(pg_client, tables);
    client.create_slot(slot).await?;
    client.get_current_wal_lsn_checkpoint().await
}

#[tokio::test]
async fn identity_pipeline_matches_direct_incremental_sync() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "xf_identity").await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;

    let checkpoint =
        capture_head(&conn_str, "xf_identity_slot", vec!["people".to_string()]).await?;

    client
        .execute(
            "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
            &[],
        )
        .await?;

    let sink = CaptureSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        &sink,
        source_opts(&conn_str, "xf_identity_slot", vec!["people".to_string()]),
        checkpoint,
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(changes.len(), 2, "expected two inserts, got {changes:?}");
    let mut names: Vec<_> = changes.iter().filter_map(name_field).collect();
    names.sort();
    assert_eq!(names, vec!["alice".to_string(), "bob".to_string()]);
    Ok(())
}

#[tokio::test]
async fn external_mutate_worker_transforms_incremental_changes() -> Result<()> {
    let worker = ensure_fixture_worker();
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "xf_external").await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;

    let checkpoint =
        capture_head(&conn_str, "xf_external_slot", vec!["people".to_string()]).await?;

    client
        .execute(
            "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
            &[],
        )
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
    run_incremental_sync_with_transforms(
        &sink,
        source_opts(&conn_str, "xf_external_slot", vec!["people".to_string()]),
        checkpoint,
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(
        changes.len(),
        2,
        "expected two transformed inserts, got {changes:?}"
    );
    for change in &changes {
        assert_eq!(
            name_field(change).as_deref(),
            Some("mutated"),
            "external mutate worker should rewrite name; got {change:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn external_mutate_worker_transforms_full_sync_rows() -> Result<()> {
    let worker = ensure_fixture_worker();
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "xf_full").await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;
    client
        .execute(
            "INSERT INTO people (id, name) VALUES (1, 'carol'), (2, 'dave')",
            &[],
        )
        .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![worker.to_string_lossy().into_owned(), "mutate".to_string()],
        FramerKind::Ndjson,
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity();

    let sink = CaptureSink::new();
    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 100,
        dry_run: false,
    };

    run_full_sync_with_transforms(
        &sink,
        source_opts(&conn_str, "xf_full_slot", vec!["people".to_string()]),
        sync_opts,
        None::<&checkpoint::SyncManager<checkpoint::NullStore>>,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2, "expected two snapshotted rows, got {rows:?}");
    for row in &rows {
        assert_eq!(
            row_name_field(row).as_deref(),
            Some("mutated"),
            "full sync write_rows should run external mutate; got {row:?}"
        );
    }
    Ok(())
}

async fn mem_surreal_sink() -> Result<(
    surreal2_sink::Surreal2Sink,
    surrealdb::Surreal<surrealdb::engine::any::Any>,
)> {
    let db = surrealdb::engine::any::connect("memory").await?;
    db.use_ns("test").use_db("test").await?;
    Ok((surreal2_sink::Surreal2Sink::new(db.clone()), db))
}

#[tokio::test]
async fn external_mutate_worker_writes_mutated_fields_to_surrealdb() -> Result<()> {
    let worker = ensure_fixture_worker();
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "xf_surreal").await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;

    let checkpoint = capture_head(&conn_str, "xf_surreal_slot", vec!["people".to_string()]).await?;

    client
        .execute(
            "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
            &[],
        )
        .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![worker.to_string_lossy().into_owned(), "mutate".to_string()],
        FramerKind::Ndjson,
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);

    let (sink, db) = mem_surreal_sink().await?;
    run_incremental_sync_with_transforms(
        &sink,
        source_opts(&conn_str, "xf_surreal_slot", vec!["people".to_string()]),
        checkpoint,
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;

    #[derive(Debug, serde::Deserialize)]
    struct PeopleRow {
        name: Option<String>,
    }
    let mut resp = db.query("SELECT name FROM people").await?;
    let rows: Vec<PeopleRow> = resp.take(0)?;
    assert_eq!(
        rows.len(),
        2,
        "expected two people in SurrealDB, got {rows:?}"
    );
    for row in &rows {
        assert_eq!(
            row.name.as_deref(),
            Some("mutated"),
            "SurrealDB sink path must receive external mutate; got {row:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn relation_change_path_records_junction_applies() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(
        container,
        &format!(
            "xf_rel_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                % 1_000_000_000
        ),
    )
    .await?;

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "
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

    let tables = vec![
        "authors".to_string(),
        "tags".to_string(),
        "books".to_string(),
        "book_tags".to_string(),
    ];
    let slot = "xf_rel_slot";
    let checkpoint = capture_head(&conn_str, slot, tables.clone()).await?;

    client
        .batch_execute(
            "
            INSERT INTO authors (id, name) VALUES (1, 'Ada');
            INSERT INTO tags (id, label) VALUES (7, 'sci');
            INSERT INTO books (id, title, author_id) VALUES (10, 'Notes', 1);
            INSERT INTO book_tags (book_id, tag_id) VALUES (10, 7);
            ",
        )
        .await?;

    let sink = CaptureSink::new();
    run_incremental_sync_with_transforms(
        &sink,
        source_opts(&conn_str, slot, tables),
        checkpoint,
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &Pipeline::new(),
        &ApplyOpts::identity(),
    )
    .await?;

    let relations = sink.relation_changes.lock().expect("lock").clone();
    assert!(
        !relations.is_empty(),
        "expected book_tags RelationChange via wal2json SourceDriver, got changes={:?} relations={:?}",
        sink.changes.lock().expect("lock"),
        relations
    );
    assert!(
        relations
            .iter()
            .any(|r| r.relation.relation_type == "book_tags"),
        "expected book_tags relation edge, got {relations:?}"
    );
    Ok(())
}
