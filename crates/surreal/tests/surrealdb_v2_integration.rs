//! Integration tests for Surreal2Store checkpoint storage.
//!
//! Requires a SurrealDB v2 server (via `SurrealDbContainer`). Skips when the
//! detected server major is v3 — that path lives behind feature `v3`.

use surreal_sync_core::{CheckpointID, CheckpointStore};
use surreal_sync_surreal::version::testing::SurrealDbContainer;
use surreal_sync_surreal::version::SurrealMajorVersion;
use surreal_sync_surreal::Surreal2Store;

struct TestCtx {
    store: Surreal2Store,
    client: surrealdb2::Surreal<surrealdb2::engine::any::Any>,
}

impl TestCtx {
    fn store(&self) -> &dyn CheckpointStore {
        &self.store
    }
}

async fn connect(db: &SurrealDbContainer, table: &str) -> anyhow::Result<TestCtx> {
    let endpoint = db.ws_endpoint();
    let client = surrealdb2::engine::any::connect(&endpoint).await?;
    client
        .signin(surrealdb2::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    client.use_ns("test").use_db("test").await?;
    let store = Surreal2Store::new(client.clone(), table.to_string());
    Ok(TestCtx { store, client })
}

async fn cleanup_record(ctx: &TestCtx, table: &str, id: &CheckpointID) -> anyhow::Result<()> {
    let id_str = format!("{}_{}", id.database_type.replace('-', "_"), id.phase);
    ctx.client
        .query("DELETE type::thing($record_tb, $record_id)")
        .bind(("record_tb", table.to_string()))
        .bind((
            "record_id",
            surrealdb2::sql::Value::Strand(surrealdb2::sql::Strand::from(id_str)),
        ))
        .await?;
    Ok(())
}

fn skip_unless_v2(db: &SurrealDbContainer) -> bool {
    matches!(db.detected_version, Some(SurrealMajorVersion::V3))
}

#[tokio::test]
async fn test_checkpoint_store_roundtrip() -> anyhow::Result<()> {
    let mut db = SurrealDbContainer::new("test-checkpoint-roundtrip");
    db.start()?;
    db.wait_until_ready(30)?;
    if skip_unless_v2(&db) {
        eprintln!("skip: Surreal2Store tests require a v2 server");
        return Ok(());
    }

    let ctx = connect(&db, "test_checkpoints").await?;
    let store = ctx.store();

    let id = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };

    let checkpoint_data = r#"{"lsn":"0/1949850","timestamp":"2024-01-01T00:00:00Z"}"#;

    store
        .store_checkpoint(&id, checkpoint_data.to_string())
        .await?;

    let loaded = store.read_checkpoint(&id).await?;
    assert!(loaded.is_some(), "Checkpoint should be found after storing");

    let loaded = loaded.unwrap();
    assert_eq!(loaded.checkpoint_data, checkpoint_data);
    assert_eq!(loaded.database_type, "postgresql-wal2json");
    assert_eq!(loaded.phase, "full_sync_start");

    cleanup_record(&ctx, "test_checkpoints", &id).await?;

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_store_not_found() -> anyhow::Result<()> {
    let mut db = SurrealDbContainer::new("test-checkpoint-not-found");
    db.start()?;
    db.wait_until_ready(30)?;
    if skip_unless_v2(&db) {
        eprintln!("skip: Surreal2Store tests require a v2 server");
        return Ok(());
    }

    let ctx = connect(&db, "test_checkpoints").await?;
    let store = ctx.store();

    let id = CheckpointID {
        database_type: "nonexistent-source".to_string(),
        phase: "full_sync_start".to_string(),
    };

    let loaded = store.read_checkpoint(&id).await?;
    assert!(
        loaded.is_none(),
        "Non-existent checkpoint should return None"
    );

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_store_multiple_phases() -> anyhow::Result<()> {
    let mut db = SurrealDbContainer::new("test-checkpoint-phases");
    db.start()?;
    db.wait_until_ready(30)?;
    if skip_unless_v2(&db) {
        eprintln!("skip: Surreal2Store tests require a v2 server");
        return Ok(());
    }

    let ctx = connect(&db, "test_checkpoints").await?;
    let store = ctx.store();

    let id_t1 = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };
    let checkpoint_data_t1 = r#"{"lsn":"0/1949850","timestamp":"2024-01-01T00:00:00Z"}"#;
    store
        .store_checkpoint(&id_t1, checkpoint_data_t1.to_string())
        .await?;

    let id_t2 = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_end".to_string(),
    };
    let checkpoint_data_t2 = r#"{"lsn":"0/2000000","timestamp":"2024-01-01T01:00:00Z"}"#;
    store
        .store_checkpoint(&id_t2, checkpoint_data_t2.to_string())
        .await?;

    let loaded_t1 = store.read_checkpoint(&id_t1).await?;
    assert!(loaded_t1.is_some());
    assert_eq!(loaded_t1.unwrap().checkpoint_data, checkpoint_data_t1);

    let loaded_t2 = store.read_checkpoint(&id_t2).await?;
    assert!(loaded_t2.is_some());
    assert_eq!(loaded_t2.unwrap().checkpoint_data, checkpoint_data_t2);

    cleanup_record(&ctx, "test_checkpoints", &id_t1).await?;
    cleanup_record(&ctx, "test_checkpoints", &id_t2).await?;

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_store_update_existing() -> anyhow::Result<()> {
    let mut db = SurrealDbContainer::new("test-checkpoint-update");
    db.start()?;
    db.wait_until_ready(30)?;
    if skip_unless_v2(&db) {
        eprintln!("skip: Surreal2Store tests require a v2 server");
        return Ok(());
    }

    let ctx = connect(&db, "test_checkpoints").await?;
    let store = ctx.store();

    let id = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };

    let checkpoint_data_v1 = r#"{"lsn":"0/1000000","timestamp":"2024-01-01T00:00:00Z"}"#;
    store
        .store_checkpoint(&id, checkpoint_data_v1.to_string())
        .await?;

    let checkpoint_data_v2 = r#"{"lsn":"0/2000000","timestamp":"2024-01-01T01:00:00Z"}"#;
    store
        .store_checkpoint(&id, checkpoint_data_v2.to_string())
        .await?;

    let loaded = store.read_checkpoint(&id).await?;
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().checkpoint_data, checkpoint_data_v2);

    cleanup_record(&ctx, "test_checkpoints", &id).await?;

    Ok(())
}
