//! Integration tests for checkpoint storage in SurrealDB.
//!
//! These tests verify that CheckpointStore correctly stores and retrieves
//! checkpoints from a real SurrealDB instance running on hostname "surrealdb".

use checkpoint::{CheckpointID, CheckpointStore};
use surrealdb::engine::any;

#[tokio::test]
async fn test_checkpoint_store_roundtrip() -> anyhow::Result<()> {
    // Connect to SurrealDB test instance
    let surreal = any::connect("ws://surrealdb:8000").await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    let store = CheckpointStore::new(surreal.clone(), "test_checkpoints".to_string());

    let id = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };

    let checkpoint_data = r#"{"lsn":"0/1949850","timestamp":"2024-01-01T00:00:00Z"}"#;

    // Store checkpoint
    store
        .store_checkpoint(&id, checkpoint_data.to_string())
        .await?;

    // Read checkpoint back
    let loaded = store.read_checkpoint(&id).await?;
    assert!(loaded.is_some(), "Checkpoint should be found after storing");

    let loaded = loaded.unwrap();
    assert_eq!(loaded.checkpoint_data, checkpoint_data);
    assert_eq!(loaded.database_type, "postgresql-wal2json");
    assert_eq!(loaded.phase, "full_sync_start");

    // Cleanup: delete test checkpoint
    surreal
        .query("DELETE $record_id")
        .bind(("record_id", id.to_thing("test_checkpoints")))
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_store_not_found() -> anyhow::Result<()> {
    // Connect to SurrealDB test instance
    let surreal = any::connect("ws://surrealdb:8000").await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    let store = CheckpointStore::new(surreal.clone(), "test_checkpoints".to_string());

    let id = CheckpointID {
        database_type: "nonexistent-source".to_string(),
        phase: "full_sync_start".to_string(),
    };

    // Try to read non-existent checkpoint
    let loaded = store.read_checkpoint(&id).await?;
    assert!(
        loaded.is_none(),
        "Non-existent checkpoint should return None"
    );

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_store_multiple_phases() -> anyhow::Result<()> {
    // Connect to SurrealDB test instance
    let surreal = any::connect("ws://surrealdb:8000").await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    let store = CheckpointStore::new(surreal.clone(), "test_checkpoints".to_string());

    // Store t1 checkpoint
    let id_t1 = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };
    let checkpoint_data_t1 = r#"{"lsn":"0/1949850","timestamp":"2024-01-01T00:00:00Z"}"#;
    store
        .store_checkpoint(&id_t1, checkpoint_data_t1.to_string())
        .await?;

    // Store t2 checkpoint
    let id_t2 = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_end".to_string(),
    };
    let checkpoint_data_t2 = r#"{"lsn":"0/2000000","timestamp":"2024-01-01T01:00:00Z"}"#;
    store
        .store_checkpoint(&id_t2, checkpoint_data_t2.to_string())
        .await?;

    // Read both checkpoints back
    let loaded_t1 = store.read_checkpoint(&id_t1).await?;
    assert!(loaded_t1.is_some());
    assert_eq!(loaded_t1.unwrap().checkpoint_data, checkpoint_data_t1);

    let loaded_t2 = store.read_checkpoint(&id_t2).await?;
    assert!(loaded_t2.is_some());
    assert_eq!(loaded_t2.unwrap().checkpoint_data, checkpoint_data_t2);

    // Cleanup
    surreal
        .query("DELETE $record_id1; DELETE $record_id2;")
        .bind(("record_id1", id_t1.to_thing("test_checkpoints")))
        .bind(("record_id2", id_t2.to_thing("test_checkpoints")))
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_checkpoint_store_update_existing() -> anyhow::Result<()> {
    // Connect to SurrealDB test instance
    let surreal = any::connect("ws://surrealdb:8000").await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal.use_ns("test").use_db("test").await?;

    let store = CheckpointStore::new(surreal.clone(), "test_checkpoints".to_string());

    let id = CheckpointID {
        database_type: "postgresql-wal2json".to_string(),
        phase: "full_sync_start".to_string(),
    };

    // Store initial checkpoint
    let checkpoint_data_v1 = r#"{"lsn":"0/1000000","timestamp":"2024-01-01T00:00:00Z"}"#;
    store
        .store_checkpoint(&id, checkpoint_data_v1.to_string())
        .await?;

    // Update the same checkpoint
    let checkpoint_data_v2 = r#"{"lsn":"0/2000000","timestamp":"2024-01-01T01:00:00Z"}"#;
    store
        .store_checkpoint(&id, checkpoint_data_v2.to_string())
        .await?;

    // Read checkpoint - should have the updated value
    let loaded = store.read_checkpoint(&id).await?;
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().checkpoint_data, checkpoint_data_v2);

    // Cleanup
    surreal
        .query("DELETE $record_id")
        .bind(("record_id", id.to_thing("test_checkpoints")))
        .await?;

    Ok(())
}
