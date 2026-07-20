//! Transform pipeline e2e for Neo4j source (identity + external mutate).

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use anyhow::Result;
use neo4rs::Query;
use surreal_sink::SurrealSink;
use surreal_sync_neo4j_source::testing::container::Neo4jContainer;
use surreal_sync_neo4j_source::{
    run_full_sync_with_transforms, run_incremental_sync_with_transforms, Neo4jCheckpoint,
    ReplicationTailOptions, SourceOpts, SyncOpts,
};
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline};

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
    rows: Mutex<Vec<UniversalRow>>,
    relation_changes: Mutex<Vec<UniversalRelationChange>>,
    relations: Mutex<Vec<UniversalRelation>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
            rows: Mutex::new(Vec::new()),
            relation_changes: Mutex::new(Vec::new()),
            relations: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        relations: &[UniversalRelation],
    ) -> anyhow::Result<()> {
        self.relations
            .lock()
            .expect("lock")
            .extend(relations.iter().cloned());
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        change: &UniversalRelationChange,
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

fn name_field(change: &UniversalChange) -> Option<String> {
    let data = change.data.as_ref()?;
    match data.get("name")? {
        UniversalValue::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn row_name_field(row: &UniversalRow) -> Option<String> {
    match row.fields.get("name")? {
        UniversalValue::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn source_opts(container: &Neo4jContainer) -> SourceOpts {
    SourceOpts {
        source_uri: container.bolt_uri(),
        source_database: Some(container.database.clone()),
        source_username: Some(container.username.clone()),
        source_password: Some(container.password.clone()),
        labels: vec!["Person".to_string()],
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: None,
        change_tracking_property: "updated_at".to_string(),
        assumed_start_timestamp: None,
        allow_empty_tracking_timestamp: false,
        id_property: "id".to_string(),
        composite_constituent: None,
    }
}

#[tokio::test]
async fn identity_and_external_mutate_incremental() -> Result<()> {
    let worker = ensure_fixture_worker();
    let name = format!("neo4j-xf-{}", std::process::id());
    let mut container = Neo4jContainer::new(&name);
    container.start()?;
    container.wait_until_ready(60).await?;

    let t1 = chrono::Utc::now();
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(container.bolt_uri())
        .user(&container.username)
        .password(&container.password)
        .db(&*container.database)
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    // Clear and seed nodes with tracking timestamps after t1.
    graph
        .run(Query::new("MATCH (n) DETACH DELETE n".to_string()))
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    graph
        .run(Query::new(
            "CREATE (a:Person {id: 1, name: 'alice', updated_at: datetime()}), \
             (b:Person {id: 2, name: 'bob', updated_at: datetime()}), \
             (a)-[:KNOWS {id: 10, updated_at: datetime()}]->(b)"
                .to_string(),
        ))
        .await?;

    let opts = source_opts(&container);
    let sync_opts = SyncOpts {
        batch_size: 100,
        dry_run: false,
    };
    let checkpoint = Neo4jCheckpoint { timestamp: t1 };
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(30);

    // Identity incremental
    let sink = CaptureSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        &sink,
        opts.clone(),
        sync_opts.clone(),
        checkpoint.clone(),
        ReplicationTailOptions::stream(deadline, None, false, 100),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    let rel_changes = sink.relation_changes.lock().expect("lock").clone();
    assert!(
        changes.len() >= 2,
        "expected node changes, got {changes:?}"
    );
    assert!(
        !rel_changes.is_empty(),
        "expected relation changes, got {rel_changes:?}"
    );

    // External mutate: create more nodes after a later checkpoint
    let t2 = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    graph
        .run(Query::new(
            "CREATE (c:Person {id: 3, name: 'carol', updated_at: datetime()}), \
             (d:Person {id: 4, name: 'dave', updated_at: datetime()})"
                .to_string(),
        ))
        .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);
    let sink = CaptureSink::new();
    run_incremental_sync_with_transforms(
        &sink,
        opts,
        sync_opts,
        Neo4jCheckpoint { timestamp: t2 },
        ReplicationTailOptions::stream(
            chrono::Utc::now() + chrono::Duration::seconds(30),
            None,
            false,
            100,
        ),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert!(
        changes.len() >= 2,
        "mutate expected node changes, got {changes:?}"
    );
    for change in &changes {
        if change.table == "person" {
            assert_eq!(name_field(change).as_deref(), Some("mutated"));
        }
    }

    Ok(())
}

#[tokio::test]
async fn external_mutate_full_sync_rows() -> Result<()> {
    let worker = ensure_fixture_worker();
    let name = format!("neo4j-xf-full-{}", std::process::id());
    let mut container = Neo4jContainer::new(&name);
    container.start()?;
    container.wait_until_ready(60).await?;

    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(container.bolt_uri())
        .user(&container.username)
        .password(&container.password)
        .db(&*container.database)
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;
    graph
        .run(Query::new("MATCH (n) DETACH DELETE n".to_string()))
        .await?;
    graph
        .run(Query::new(
            "CREATE (a:Person {id: 1, name: 'alice', updated_at: datetime()})".to_string(),
        ))
        .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);
    let sink = CaptureSink::new();
    let opts = source_opts(&container);
    let sync_opts = SyncOpts {
        batch_size: 100,
        dry_run: false,
    };
    run_full_sync_with_transforms::<_, checkpoint::NullStore>(
        &sink,
        opts,
        sync_opts,
        None,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let rows = sink.rows.lock().expect("lock").clone();
    assert!(!rows.is_empty(), "expected full sync rows");
    for row in &rows {
        if row.table == "person" {
            assert_eq!(row_name_field(row).as_deref(), Some("mutated"));
        }
    }
    Ok(())
}
