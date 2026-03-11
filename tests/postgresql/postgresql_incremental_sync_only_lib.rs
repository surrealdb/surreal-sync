//! PostgreSQL all-types incremental sync ONLY E2E test
//!
//! This test validates that PostgreSQL incremental sync operations work correctly
//! by starting with empty tables, running full sync to generate checkpoint and setup
//! triggers, then adding data and running incremental sync.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_postgresql_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let container = surreal_sync::testing::shared_containers::shared_postgres().await;

    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-pg-incr-lib-checkpoints-{test_id}");
    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgres_test_db(container, test_id)
            .await?;

    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let (pg_client, pg_connection) =
        tokio_postgres::connect(&test_conn_str, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let dataset = create_unified_full_dataset();

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    // Clean up any existing test data
    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Create the empty table in PostgreSQL
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: test_conn_str.clone(),
        source_database: Some(format!("test_{test_id}")),
        tables: vec![],
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Create sync manager with filesystem checkpoint store
    let checkpoint_store = checkpoint::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    // Run full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
    }

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Read the t1 (FullSyncStart) checkpoint file - this is needed
    // for incremental sync to pick up changes made after full sync started
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, checkpoint::SyncPhase::FullSyncStart)
            .await?;
    // Parse the CheckpointFile into database-specific checkpoint type
    let sync_checkpoint: surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint =
        checkpoint_file.parse()?;

    // Run incremental sync using the checkpoint with appropriate sink
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_incremental_sync(
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
            surreal_sync_postgresql_trigger_source::run_incremental_sync(
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
        "PostgreSQL incremental sync only",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
