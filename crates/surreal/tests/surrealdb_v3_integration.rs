//! Integration tests for Surreal3Store checkpoint storage.
//!
//! Requires a SurrealDB v3 server (via `SurrealDbContainer`). Skips when the
//! detected server major is v2.

use surreal_sync_core::{CheckpointID, CheckpointStore};
use surreal_sync_surreal::version::testing::SurrealDbContainer;
use surreal_sync_surreal::version::SurrealMajorVersion;
use surreal_sync_surreal::Surreal3Store;
use surrealdb3::types::Value as SurrealValue;

struct TestCtx {
    store: Surreal3Store,
    client: surrealdb3::Surreal<surrealdb3::engine::any::Any>,
}

impl TestCtx {
    fn store(&self) -> &dyn CheckpointStore {
        &self.store
    }
}

async fn connect(db: &SurrealDbContainer, table: &str) -> anyhow::Result<TestCtx> {
    let endpoint = db.ws_endpoint();
    let client = surrealdb3::engine::any::connect(&endpoint).await?;
    client
        .signin(surrealdb3::opt::auth::Root {
            username: "root".to_string(),
            password: "root".to_string(),
        })
        .await?;
    client.use_ns("test").use_db("test").await?;
    let store = Surreal3Store::new(client.clone(), table.to_string());
    Ok(TestCtx { store, client })
}

async fn cleanup_record(ctx: &TestCtx, table: &str, id: &CheckpointID) -> anyhow::Result<()> {
    let id_str = format!("{}_{}", id.database_type.replace('-', "_"), id.phase);
    ctx.client
        .query("DELETE type::record($record_tb, $record_key)")
        .bind(("record_tb", table.to_string()))
        .bind(("record_key", SurrealValue::String(id_str)))
        .await?;
    Ok(())
}

fn skip_unless_v3(db: &SurrealDbContainer) -> bool {
    !matches!(db.detected_version, Some(SurrealMajorVersion::V3))
}

#[tokio::test]
async fn test_checkpoint_store_roundtrip() -> anyhow::Result<()> {
    let mut db = SurrealDbContainer::new("test-checkpoint-roundtrip-v3");
    db.start()?;
    db.wait_until_ready(30)?;
    if skip_unless_v3(&db) {
        eprintln!("skip: Surreal3Store tests require a v3 server");
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
    let mut db = SurrealDbContainer::new("test-checkpoint-not-found-v3");
    db.start()?;
    db.wait_until_ready(30)?;
    if skip_unless_v3(&db) {
        eprintln!("skip: Surreal3Store tests require a v3 server");
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
