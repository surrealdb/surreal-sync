//! MongoDB all-types incremental sync CLI E2E test
//!
//! This test validates that the CLI correctly handles MongoDB incremental sync by:
//! 1. Running full sync with EMPTY database (sets up change streams and emits checkpoint)
//! 2. Reading the checkpoint file to get the resume token
//! 3. Adding data to MongoDB AFTER the full sync
//! 4. Running INCREMENTAL sync using the checkpoint to capture only the new data
//! 5. Validating all data types are preserved correctly via incremental sync

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};

#[tokio::test]
async fn test_mongodb_incremental_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();
    let checkpoint_dir = format!(".surreal-sync-checkpoints-test-{test_id}");

    // Setup MongoDB
    let mongodb_client = surreal_sync::testing::mongodb::connect_mongodb().await?;
    let db = mongodb_client.database("testdb");

    // Setup SurrealDB connection for validation
    let surreal_config = TestConfig::new(test_id, "mongodb-incr-cli");
    let surreal = connect_surrealdb(&surreal_config).await?;

    surreal_sync::testing::mongodb::cleanup(&db, &dataset).await?;
    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    // Run FULL SYNC with EMPTY MongoDB database
    // This sets up the change stream infrastructure and emits a checkpoint with resume token
    println!("📝 Phase 1: Running full sync with empty database to setup change stream and get checkpoint...");
    let full_args = vec![
        "from".to_string(),
        "mongodb".to_string(),
        "full".to_string(),
        "--connection-string".to_string(),
        "mongodb://root:root@mongodb:27017".to_string(),
        "--database".to_string(),
        "testdb".to_string(),
        "--surreal-endpoint".to_string(),
        surreal_config.surreal_endpoint.clone(),
        "--to-namespace".to_string(),
        surreal_config.surreal_namespace.clone(),
        "--to-database".to_string(),
        surreal_config.surreal_database.clone(),
        "--surreal-username".to_string(),
        "root".to_string(),
        "--surreal-password".to_string(),
        "root".to_string(),
        "--emit-checkpoints".to_string(),
        "--checkpoint-dir".to_string(),
        checkpoint_dir.clone(),
    ];

    let full_output =
        execute_surreal_sync(&full_args.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
    assert_cli_success(
        &full_output,
        "MongoDB full sync (empty) to setup change stream",
    );

    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(checkpoint_dir.clone())?;

    let checkpoint_string = {
        // Read the t1 (FullSyncStart) checkpoint file - this is needed
        // for incremental sync to pick up changes made after full sync started
        use checkpoint::{Checkpoint, SyncPhase};
        let checkpoint_file =
            checkpoint::get_checkpoint_for_phase(&checkpoint_dir, SyncPhase::FullSyncStart).await?;
        // Parse the CheckpointFile into database-specific checkpoint type and convert to CLI string
        let mongodb_checkpoint: surreal_sync_mongodb_changestream_source::MongoDBCheckpoint =
            checkpoint_file.parse()?;
        mongodb_checkpoint.to_cli_string()
    };

    surreal_sync::testing::mongodb::insert_docs(&db, &dataset).await?;

    // Give change stream time to register the changes
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Run INCREMENTAL sync using the checkpoint to capture only the new data
    println!("📝 Phase 4: Running incremental sync from checkpoint to capture newly added data...");
    let incremental_args = vec![
        "from".to_string(),
        "mongodb".to_string(),
        "incremental".to_string(),
        "--connection-string".to_string(),
        "mongodb://root:root@mongodb:27017".to_string(),
        "--database".to_string(),
        "testdb".to_string(),
        "--to-namespace".to_string(),
        surreal_config.surreal_namespace.clone(),
        "--to-database".to_string(),
        surreal_config.surreal_database.clone(),
        "--surreal-endpoint".to_string(),
        surreal_config.surreal_endpoint.clone(),
        "--surreal-username".to_string(),
        "root".to_string(),
        "--surreal-password".to_string(),
        "root".to_string(),
        "--incremental-from".to_string(),
        checkpoint_string,
        "--timeout".to_string(),
        "10".to_string(), // Run for 10 seconds to capture the changes
    ];

    let incremental_output = execute_surreal_sync(
        &incremental_args
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>(),
    )?;
    assert_cli_success(
        &incremental_output,
        "MongoDB incremental sync capturing new data",
    );

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "MongoDB incremental sync only",
    )
    .await?;

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
