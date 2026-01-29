//! PostgreSQL all-types incremental sync ONLY E2E test
//!
//! This test validates that PostgreSQL incremental sync operations work correctly
//! by starting with empty tables, running full sync to generate checkpoint and setup
//! triggers, then adding data and running incremental sync.

use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};

#[tokio::test]
async fn test_postgresql_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let test_id = generate_test_id();

    // Clean up checkpoint directory to prevent cross-test contamination
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints")?;

    // Setup PostgreSQL connection
    let pg_config = surreal_sync::testing::postgresql::create_postgres_config();
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&pg_config.get_connection_string(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let dataset = create_unified_full_dataset();

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "incremental-only-test");
    let surreal = connect_surrealdb(&surreal_config).await?;

    // Clean up any existing test data
    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    // Create the empty table in PostgreSQL
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: pg_config.get_connection_string(),
        source_database: Some("testdb".to_string()),
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Create SurrealDB v2 sink
    let sink = surreal2_sink::Surreal2Sink::new(surreal.clone());

    // Create sync manager with filesystem checkpoint store
    let checkpoint_store = checkpoint::FilesystemStore::new(".test-checkpoints");
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    surreal_sync_postgresql_trigger_source::run_full_sync(
        &sink,
        source_opts.clone(),
        sync_opts,
        Some(&sync_manager),
    )
    .await?;

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-checkpoints")?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Read the t1 (FullSyncStart) checkpoint file - this is needed
    // for incremental sync to pick up changes made after full sync started
    let checkpoint_file = checkpoint::get_checkpoint_for_phase(
        ".test-checkpoints",
        checkpoint::SyncPhase::FullSyncStart,
    )
    .await?;
    // Parse the CheckpointFile into database-specific checkpoint type
    let sync_checkpoint: surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint =
        checkpoint_file.parse()?;

    // Run incremental sync using the checkpoint
    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        sync_checkpoint,
        chrono::Utc::now() + chrono::Duration::hours(1), // 1 hour deadline
        None, // No target checkpoint - sync all available changes
    )
    .await?;

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "PostgreSQL incremental sync only",
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
