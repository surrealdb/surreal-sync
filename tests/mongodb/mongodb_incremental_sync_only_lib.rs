//! MongoDB all-types incremental sync ONLY E2E test
//!
//! This test validates that MongoDB incremental sync operations work correctly
//! by starting with empty collections, running full sync to generate checkpoint,
//! then adding data and running incremental sync.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_mongodb_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    // Clean up checkpoint directory to prevent cross-test contamination
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints")?;

    // Setup MongoDB connection
    let mongodb_client = surreal_sync::testing::mongodb::connect_mongodb().await?;
    let db = mongodb_client.database("testdb");

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::new(test_id, "mongodb-incremental-only");
    let conn = connect_auto(&surreal_config).await?;

    // Clean up any existing test data
    surreal_sync::testing::mongodb::cleanup(&db, &dataset).await?;
    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: "mongodb://root:root@mongodb:27017".to_string(),
        source_database: Some("testdb".to_string()),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Run full sync on empty collection to establish baseline checkpoint
    println!("🔧 Phase 2: Running full sync on empty collection to get checkpoint...");

    // Get current timestamp as checkpoint baseline
    let checkpoint_timestamp = chrono::Utc::now();

    // Create sync manager with filesystem checkpoint store
    let checkpoint_store = checkpoint::FilesystemStore::new(".test-checkpoints");
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    // Run full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
    }

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-checkpoints")?;

    println!(
        "📍 Timestamp checkpoint from empty collection: {}",
        checkpoint_timestamp.to_rfc3339()
    );

    surreal_sync::testing::mongodb::insert_docs(&db, &dataset).await?;

    // Set target timestamp right after data insertion
    let deadline = chrono::Utc::now() + chrono::Duration::milliseconds(100);

    // Read the t1 (FullSyncStart) checkpoint file - this is needed
    // for incremental sync to pick up changes made after full sync started
    let main_checkpoint = checkpoint::get_checkpoint_for_phase(
        ".test-checkpoints",
        checkpoint::SyncPhase::FullSyncStart,
    )
    .await?;
    // Convert to mongodb crate's checkpoint type
    let sync_checkpoint: surreal_sync_mongodb_changestream_source::MongoDBCheckpoint =
        main_checkpoint.parse()?;

    // Run TRUE incremental sync using the checkpoint with target timestamp
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                deadline,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                deadline,
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "MongoDB incremental sync only",
        SourceDatabase::MongoDB,
    )
    .await?;

    // Clean up
    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
