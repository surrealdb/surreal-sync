//! Neo4j all-types E2E test
//!
//! This test validates that Neo4j full sync operations preserve all data types
//! correctly when syncing to SurrealDB, using the unified dataset.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::surrealdb_container::SurrealDbContainer;
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};
use surreal_sync_neo4j_source::testing::container::Neo4jContainer;

#[tokio::test]
async fn test_neo4j_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let mut surrealdb = SurrealDbContainer::new("test-neo4j-full-sync-lib-sdb");
    surrealdb.start()?;
    surrealdb.wait_until_ready(30)?;

    let mut container = Neo4jContainer::new("test-neo4j-full-sync-lib");
    container.start()?;
    container.wait_until_ready(60).await?;

    // Create test dataset
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup Neo4j connection and data
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(&container.bolt_uri())
        .user(&container.username)
        .password(&container.password)
        .db(&*container.database)
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    // Create schema and insert all test data
    surreal_sync::testing::neo4j::create_constraints_and_indices(&graph, &dataset).await?;
    surreal_sync::testing::neo4j::create_nodes(&graph, &dataset).await?;

    // Perform full sync from Neo4j to SurrealDB
    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: container.bolt_uri(),
        source_database: Some(container.database.clone()),
        source_username: Some(container.username.clone()),
        source_password: Some(container.password.clone()),
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

    // Execute full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_neo4j_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "Neo4j full sync only",
        SourceDatabase::Neo4j,
    )
    .await?;

    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    Ok(())
}
