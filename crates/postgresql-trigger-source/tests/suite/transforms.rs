//! Transform pipeline e2e for PostgreSQL trigger source.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql_trigger_source::{
    run_incremental_sync_with_transforms, PostgreSQLCheckpoint, PostgresIncrementalSource,
    ReplicationTailOptions, SourceOpts,
};
use sync_core::{UniversalChange, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline};
use tokio::sync::Mutex as TokioMutex;

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

fn unique_table(prefix: &str) -> String {
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
    let conn_str = container.connection_string.clone();
    let table = unique_table("xf_id_people");
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
    let mut names: Vec<_> = changes.iter().filter_map(name_field).collect();
    names.sort();
    names.dedup();
    assert!(names.contains(&"alice".to_string()) && names.contains(&"bob".to_string()));
    Ok(())
}

#[tokio::test]
async fn external_mutate_worker_transforms_incremental_changes() -> Result<()> {
    let worker = ensure_fixture_worker();
    let container = crate::shared::postgres().await;
    let conn_str = container.connection_string.clone();
    let table = unique_table("xf_ext_people");
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
        if let Some(name) = name_field(change) {
            assert_eq!(name, "mutated");
        }
    }
    Ok(())
}
