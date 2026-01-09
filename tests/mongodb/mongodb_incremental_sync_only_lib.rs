//! MongoDB all-types incremental sync ONLY E2E test
//!
//! This test validates that MongoDB incremental sync operations work correctly
//! by starting with empty collections, running full sync to generate checkpoint,
//! then adding data and running incremental sync.

use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};
use surreal_sync::SurrealOpts;

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

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "mongodb-incremental-only");
    let surreal = connect_surrealdb(&surreal_config).await?;

    // Clean up any existing test data
    surreal_sync::testing::mongodb::cleanup(&db, &dataset).await?;
    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    let source_opts = surreal_sync_mongodb::SourceOpts {
        source_uri: "mongodb://root:root@mongodb:27017".to_string(),
        source_database: Some("testdb".to_string()),
    };

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 1000,
        dry_run: false,
    };

    // Run full sync on empty collection to establish baseline checkpoint
    println!("🔧 Phase 2: Running full sync on empty collection to get checkpoint...");

    // Get current timestamp as checkpoint baseline
    let checkpoint_timestamp = chrono::Utc::now();

    let sync_config = checkpoint::SyncConfig {
        incremental: false, // This is full sync to set up infrastructure
        emit_checkpoints: true,
        checkpoint_dir: Some(".test-checkpoints".to_string()),
    };

    surreal_sync_mongodb::run_full_sync(
        source_opts.clone(),
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_sync_mongodb::SurrealOpts::from(&surreal_opts),
        Some(sync_config),
    )
    .await?;

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
    let sync_checkpoint: surreal_sync_mongodb::MongoDBCheckpoint = main_checkpoint.parse()?;

    // Run TRUE incremental sync using the checkpoint with target timestamp
    surreal_sync_mongodb::run_incremental_sync(
        source_opts,
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_sync_mongodb::SurrealOpts::from(&surreal_opts),
        sync_checkpoint,
        deadline,
        None, // No target checkpoint - sync until deadline
    )
    .await?;

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "MongoDB incremental sync only",
    )
    .await?;

    // Clean up
    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
