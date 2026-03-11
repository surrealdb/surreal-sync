//! MySQL all-types incremental sync ONLY E2E test
//!
//! This test validates that MySQL incremental sync operations work correctly
//! by starting with empty tables, running full sync to generate checkpoint and setup
//! triggers, then adding data and running incremental sync.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_mysql_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mysql().await;

    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-mysql-incr-lib-checkpoints-{test_id}");

    // Clean up checkpoint directory to prevent cross-test contamination
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_conn_str = surreal_sync::testing::shared_containers::create_mysql_test_db(container, test_id).await?;
    let test_db_name = format!("test_{test_id}");

    // Setup MySQL connection
    let pool = mysql_async::Pool::from_url(&test_conn_str)?;
    let mut mysql_conn = pool.get_conn().await?;

    let dataset = create_unified_full_dataset();

    // Setup SurrealDB container
    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Clean up any existing test data
    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    surreal_sync::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: test_conn_str.clone(),
        source_database: Some(test_db_name),
        tables: vec![],
        mysql_boolean_paths: Some(vec!["all_types_posts.post_categories".to_string()]),
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Create sync manager with filesystem checkpoint store
    let checkpoint_store = checkpoint::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    // Run full sync to set up triggers and get checkpoint with appropriate sink
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
    }

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    // Insert test data into MySQL (data only, tables already exist with triggers)
    surreal_sync::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    // Read the t1 (FullSyncStart) checkpoint file - this is needed
    // for incremental sync to pick up changes made after full sync started
    let checkpoint_file = checkpoint::get_checkpoint_for_phase(
        &checkpoint_dir,
        checkpoint::SyncPhase::FullSyncStart,
    )
    .await?;
    // Parse the CheckpointFile into database-specific checkpoint type
    let sync_checkpoint: surreal_sync_mysql_trigger_source::MySQLCheckpoint =
        checkpoint_file.parse()?;

    // Run incremental sync using the checkpoint with appropriate sink
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1),
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1),
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "MySQL incremental sync only",
        SourceDatabase::MySQL,
    )
    .await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    // Close connection
    drop(mysql_conn);
    pool.disconnect().await?;

    Ok(())
}
