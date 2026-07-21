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
use sync_core::{
    UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow, UniversalValue,
};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline, FramerKind};

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
    rows: Mutex<Vec<UniversalRow>>,
    relation_changes: Mutex<Vec<UniversalRelationChange>>,
    relations: Mutex<Vec<UniversalRelation>>,
    /// Combined apply order: `change:{id}` or `relation:{id}`.
    apply_order: Mutex<Vec<String>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
            rows: Mutex::new(Vec::new()),
            relation_changes: Mutex::new(Vec::new()),
            relations: Mutex::new(Vec::new()),
            apply_order: Mutex::new(Vec::new()),
        }
    }

    fn apply_order_tags(&self) -> Vec<String> {
        self.apply_order.lock().expect("lock").clone()
    }
}

fn id_display(id: &UniversalValue) -> String {
    match id {
        UniversalValue::Int64(n) => n.to_string(),
        UniversalValue::Int32(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here. Mirror into `changes` /
        // `apply_order` so incremental tests that assert apply observations still
        // see coalesced writes (full sync keeps asserting on `rows`).
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
        let mut apply_order = self.apply_order.lock().expect("lock");
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            apply_order.push(format!("change:{}", id_display(&row.id)));
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
        relations: &[UniversalRelation],
    ) -> anyhow::Result<()> {
        self.relations
            .lock()
            .expect("lock")
            .extend(relations.iter().cloned());
        let mut apply_order = self.apply_order.lock().expect("lock");
        let mut relation_changes = self.relation_changes.lock().expect("lock");
        for relation in relations {
            apply_order.push(format!("relation:{}", id_display(&relation.id)));
            relation_changes.push(UniversalRelationChange::update(relation.clone()));
        }
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        self.apply_order
            .lock()
            .expect("lock")
            .push(format!("change:{}", id_display(&change.id)));
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        change: &UniversalRelationChange,
    ) -> anyhow::Result<()> {
        self.apply_order
            .lock()
            .expect("lock")
            .push(format!("relation:{}", id_display(&change.relation.id)));
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

fn relation_name_field(rel: &UniversalRelation) -> Option<String> {
    match rel.data.get("name")? {
        UniversalValue::Text(value) => Some(value.clone()),
        other => panic!("unexpected relation name: {other:?}"),
    }
}

fn relation_change_name(change: &UniversalRelationChange) -> Option<String> {
    relation_name_field(&change.relation)
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

/// Within a fetch batch, endpoint nodes must be applied before their relation.
///
/// Regression for SourceDriver LIFO `pop()` that emitted edges before nodes.
#[tokio::test]
async fn incremental_applies_nodes_before_relations_in_batch() -> Result<()> {
    let name = format!("neo4j-xf-order-{}", std::process::id());
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

    graph
        .run(Query::new("MATCH (n) DETACH DELETE n".to_string()))
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    graph
        .run(Query::new(
            "CREATE (a:Person {id: 1, name: 'alice', updated_at: datetime()}), \
             (b:Person {id: 2, name: 'bob', updated_at: datetime()}), \
             (a)-[:KNOWS {id: 10, name: 'friends', updated_at: datetime()}]->(b)"
                .to_string(),
        ))
        .await?;

    let opts = source_opts(&container);
    let sync_opts = SyncOpts {
        batch_size: 100,
        dry_run: false,
    };
    let sink = CaptureSink::new();
    let pipeline = Pipeline::new();
    // batch_size > 1 still preserves emission order inside the apply buffer.
    let apply_opts = ApplyOpts::identity().with_batch_size(100);
    run_incremental_sync_with_transforms(
        &sink,
        opts,
        sync_opts,
        Neo4jCheckpoint::at(t1),
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

    let order = sink.apply_order_tags();
    let changes = sink.changes.lock().expect("lock").clone();
    let rel_changes = sink.relation_changes.lock().expect("lock").clone();

    assert!(
        changes.len() >= 2,
        "expected endpoint node changes, got {changes:?}"
    );
    assert_eq!(
        rel_changes.len(),
        1,
        "expected one KNOWS relation change, got {rel_changes:?}"
    );

    let first_relation = order
        .iter()
        .position(|t| t.starts_with("relation:"))
        .unwrap_or_else(|| panic!("expected a relation apply, got order {order:?}"));
    let node_applies_before = &order[..first_relation];
    assert!(
        !node_applies_before.is_empty(),
        "relation applied with no prior node applies: {order:?}"
    );
    assert!(
        node_applies_before
            .iter()
            .all(|t| t.starts_with("change:")),
        "nodes must be applied before relations within a fetch batch, got {order:?}"
    );

    // Endpoint ids from the relation must already appear as change applies.
    let rel = &rel_changes[0].relation;
    let input_tag = format!("change:{}", id_display(&rel.input.id));
    let output_tag = format!("change:{}", id_display(&rel.output.id));
    assert!(
        node_applies_before.iter().any(|t| t == &input_tag),
        "input endpoint {input_tag} missing before relation in {order:?}"
    );
    assert!(
        node_applies_before.iter().any(|t| t == &output_tag),
        "output endpoint {output_tag} missing before relation in {order:?}"
    );

    Ok(())
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
             (a)-[:KNOWS {id: 10, name: 'friends', updated_at: datetime()}]->(b)"
                .to_string(),
        ))
        .await?;

    let opts = source_opts(&container);
    let sync_opts = SyncOpts {
        batch_size: 100,
        dry_run: false,
    };
    let checkpoint = Neo4jCheckpoint::at(t1);
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

    // External mutate: create more nodes + a relation after a later checkpoint
    let t2 = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    graph
        .run(Query::new(
            "CREATE (c:Person {id: 3, name: 'carol', updated_at: datetime()}), \
             (d:Person {id: 4, name: 'dave', updated_at: datetime()}), \
             (c)-[:KNOWS {id: 20, name: 'coworkers', updated_at: datetime()}]->(d)"
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
        FramerKind::Ndjson,
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);
    let sink = CaptureSink::new();
    run_incremental_sync_with_transforms(
        &sink,
        opts,
        sync_opts,
        Neo4jCheckpoint::at(t2),
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
    let rel_changes = sink.relation_changes.lock().expect("lock").clone();
    assert!(
        changes.len() >= 2,
        "mutate expected node changes, got {changes:?}"
    );
    assert!(
        !rel_changes.is_empty(),
        "mutate must apply relation changes through the pipeline, got {rel_changes:?}"
    );
    for change in &changes {
        if change.table == "person" {
            assert_eq!(name_field(change).as_deref(), Some("mutated"));
        }
    }
    for rel in &rel_changes {
        assert_eq!(
            relation_change_name(rel).as_deref(),
            Some("mutated"),
            "relation name must pass through external mutate; got {rel:?}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn external_mutate_full_sync_rows_and_relations() -> Result<()> {
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
            "CREATE (a:Person {id: 1, name: 'alice', updated_at: datetime()}), \
             (b:Person {id: 2, name: 'bob', updated_at: datetime()}), \
             (a)-[:KNOWS {id: 10, name: 'friends', updated_at: datetime()}]->(b)"
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
        FramerKind::Ndjson,
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
    let relations = sink.relations.lock().expect("lock").clone();
    assert!(!rows.is_empty(), "expected full sync rows");
    assert!(
        !relations.is_empty(),
        "full sync must write relations via write_relations, got {relations:?}"
    );
    for row in &rows {
        if row.table == "person" {
            assert_eq!(row_name_field(row).as_deref(), Some("mutated"));
        }
    }
    for rel in &relations {
        assert_eq!(
            relation_name_field(rel).as_deref(),
            Some("mutated"),
            "relation name must pass through external mutate on write_relations; got {rel:?}"
        );
    }
    Ok(())
}
