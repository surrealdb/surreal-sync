//! Neo4j all-types incremental sync ONLY E2E test
//!
//! This test validates that Neo4j incremental sync operations work correctly
//! by capturing a timestamp before data insertion, then running incremental sync
//! to capture all newly created nodes.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_neo4j_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let test_id = generate_test_id();

    // Capture timestamp BEFORE any operations - this ensures all nodes created later
    // will have updated_at > t1 and will be picked up by incremental sync
    let t1 = chrono::Utc::now();

    let dataset = create_unified_full_dataset();

    // Clean up checkpoint directory to prevent cross-test contamination
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints")?;

    // Setup Neo4j connection
    let neo4j_config = surreal_sync::testing::neo4j::Neo4jConfig::default();
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(neo4j_config.get_uri())
        .user(neo4j_config.get_username())
        .password(neo4j_config.get_password())
        .db(neo4j_config.get_database())
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::new(test_id, "neo4j-incremental-only");
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Clean up any existing test data
    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    // Create schema/constraints but don't insert data yet
    surreal_sync::testing::neo4j::create_constraints_and_indices(&graph, &dataset).await?;

    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: neo4j_config.get_uri(),
        source_database: Some(neo4j_config.get_database()),
        source_username: Some(neo4j_config.get_username()),
        source_password: Some(neo4j_config.get_password()),
        labels: vec![],
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: Some(vec![
            "all_types_users.metadata".to_string(),
            "all_types_posts.post_categories".to_string(),
        ]),
        change_tracking_property: "updated_at".to_string(),
        assumed_start_timestamp: None,
        allow_empty_tracking_timestamp: false,
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Run full sync with empty data to verify it works (no checkpoint tracking)
    // Note: We don't use checkpoint tracking here because empty data has no timestamps.
    //
    // Alternative approach for testing with checkpoint tracking on empty data:
    // Set assumed_start_timestamp and allow_empty_tracking_timestamp flags:
    //   assumed_start_timestamp: Some(chrono::Utc::now())
    //   allow_empty_tracking_timestamp: true
    // This would emit checkpoints using the assumed timestamp, similar to how the
    // loadtest infrastructure works. However, for this test we use the simpler
    // approach of running without checkpoint tracking, then using a manually
    // captured timestamp for incremental sync.
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts.clone(),
                sync_opts.clone(),
                None, // No checkpoint tracking for empty data
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts.clone(),
                sync_opts.clone(),
                None, // No checkpoint tracking for empty data
            )
            .await?;
        }
    }

    // Now insert test data into Neo4j (with timestamps for incremental tracking)
    surreal_sync::testing::neo4j::create_nodes(&graph, &dataset).await?;

    // Run incremental sync using the timestamp from before data insertion
    // This ensures all newly created nodes (with updated_at > t1) are synced
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            let neo4j_checkpoint = surreal_sync_neo4j_source::Neo4jCheckpoint { timestamp: t1 };
            surreal_sync_neo4j_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_opts,
                neo4j_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1), // 1 hour deadline
                None, // No target checkpoint - sync all available changes
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            let neo4j_checkpoint = surreal_sync_neo4j_source::Neo4jCheckpoint { timestamp: t1 };
            surreal_sync_neo4j_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_opts,
                neo4j_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1), // 1 hour deadline
                None, // No target checkpoint - sync all available changes
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "Neo4j incremental sync only",
        SourceDatabase::Neo4j,
    )
    .await?;

    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    Ok(())
}
