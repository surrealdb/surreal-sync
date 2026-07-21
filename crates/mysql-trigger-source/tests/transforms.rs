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
use sync_core::{UniversalChange, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline, FramerKind};

mod common;

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
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
    async fn write_universal_rows(&self, _rows: &[UniversalRow]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[sync_core::UniversalRelation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        _change: &sync_core::UniversalRelationChange,
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

fn name_field(change: &UniversalChange) -> Option<String> {
    let data = change.data.as_ref()?;
    match data.get("name")? {
        UniversalValue::VarChar { value, .. } | UniversalValue::Text(value) => Some(value.clone()),
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
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
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
