//! Transform pipeline e2e for MongoDB change streams (identity + external mutate).

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use mongodb::bson::doc;
use surreal_sink::SurrealSink;
use surreal_sync::testing::generate_test_id;
use surreal_sync_mongodb_changestream_source::{
    run_full_sync_with_transforms, run_incremental_sync_with_transforms, MongoDBCheckpoint,
    ReplicationTailOptions, SourceOpts, SyncOpts,
};
use sync_core::{UniversalChange, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
    rows: Mutex<Vec<UniversalRow>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
            rows: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; mirror into `changes` for
        // incremental assertions (full sync still reads `rows`).
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
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
            .current_dir(env!("CARGO_MANIFEST_DIR"))
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

#[tokio::test]
async fn identity_and_external_mutate_incremental() -> Result<(), Box<dyn std::error::Error>> {
    let worker = ensure_fixture_worker();
    let container = surreal_sync::testing::shared_containers::shared_mongodb().await;
    let test_id = generate_test_id();
    let db_name = format!("xf_mongo_{test_id}");

    let client =
        surreal_sync::testing::mongodb::connect_mongodb(&container.connection_uri()).await?;
    let db = client.database(&db_name);
    let people = db.collection::<mongodb::bson::Document>("people");
    people.drop().await.ok();
    db.create_collection("people").await?;

    // Establish resume token via empty full sync checkpoint emission.
    let checkpoint_dir = format!(".test-mongo-xf-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    let store = checkpoint::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = checkpoint::SyncManager::new(store);

    let source_opts = SourceOpts {
        source_uri: container.connection_uri(),
        source_database: Some(db_name.clone()),
        collections: vec!["people".to_string()],
    };
    let sync_opts = SyncOpts {
        batch_size: 100,
        dry_run: false,
        schema: None,
    };

    let sink = CaptureSink::new();
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_with_transforms(
        &sink,
        source_opts.clone(),
        sync_opts.clone(),
        Some(&sync_manager),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, checkpoint::SyncPhase::FullSyncStart)
            .await?;
    let from_checkpoint: MongoDBCheckpoint = checkpoint_file.parse()?;

    people
        .insert_many(vec![
            doc! { "_id": "1", "name": "alice" },
            doc! { "_id": "2", "name": "bob" },
        ])
        .await?;

    let sink = CaptureSink::new();
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
    assert_eq!(changes.len(), 2, "identity incremental: {changes:?}");
    let mut names: Vec<_> = changes.iter().filter_map(name_field).collect();
    names.sort();
    assert_eq!(names, vec!["alice".to_string(), "bob".to_string()]);

    // Capture a new head token after identity sync idle-stop, then insert more.
    let head_token =
        surreal_sync_mongodb_changestream_source::checkpoint::get_resume_token(&client, &db_name)
            .await?;
    let from_checkpoint = MongoDBCheckpoint {
        resume_token: head_token,
        timestamp: chrono::Utc::now(),
    };

    people
        .insert_many(vec![
            doc! { "_id": "3", "name": "carol" },
            doc! { "_id": "4", "name": "dave" },
        ])
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
        source_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(chrono::Utc::now() + chrono::Duration::seconds(20), None),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(changes.len(), 2, "mutate incremental: {changes:?}");
    for change in &changes {
        assert_eq!(name_field(change).as_deref(), Some("mutated"));
    }

    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}

#[tokio::test]
async fn external_mutate_full_sync_rows() -> Result<(), Box<dyn std::error::Error>> {
    let worker = ensure_fixture_worker();
    let container = surreal_sync::testing::shared_containers::shared_mongodb().await;
    let test_id = generate_test_id();
    let db_name = format!("xf_mongo_full_{test_id}");

    let client =
        surreal_sync::testing::mongodb::connect_mongodb(&container.connection_uri()).await?;
    let db = client.database(&db_name);
    let people = db.collection::<mongodb::bson::Document>("people");
    people.drop().await.ok();
    db.create_collection("people").await?;
    people
        .insert_many(vec![
            doc! { "_id": "1", "name": "alice" },
            doc! { "_id": "2", "name": "bob" },
        ])
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
    run_full_sync_with_transforms::<_, checkpoint::NullStore>(
        &sink,
        SourceOpts {
            source_uri: container.connection_uri(),
            source_database: Some(db_name),
            collections: vec!["people".to_string()],
        },
        SyncOpts {
            batch_size: 100,
            dry_run: false,
            schema: None,
        },
        None,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2, "full sync rows: {rows:?}");
    for row in &rows {
        assert_eq!(row_name_field(row).as_deref(), Some("mutated"));
    }
    Ok(())
}
