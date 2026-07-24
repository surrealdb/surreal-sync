//! Neo4j composite database full sync test
//!
//! This test validates that full sync works through a composite database
//! using the USE clause to route queries to a local constituent alias.
//! Requires Neo4j Enterprise Edition (composite databases are Enterprise-only).
//!
//! Configure via environment variables:
//!   NEO4J_ENTERPRISE_URI  - e.g., "neo4j://127.0.0.1:7687"
//!   NEO4J_ENTERPRISE_USER - e.g., "neo4j" (default: "neo4j")
//!   NEO4J_ENTERPRISE_PASS - e.g., "neo4jneo4j" (default: "neo4j")

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
#[ignore] // Requires Neo4j Enterprise Edition; run manually with --ignored
async fn test_neo4j_composite_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let neo4j_uri = std::env::var("NEO4J_ENTERPRISE_URI")
        .unwrap_or_else(|_| "neo4j://127.0.0.1:7687".to_string());
    let neo4j_user = std::env::var("NEO4J_ENTERPRISE_USER").unwrap_or_else(|_| "neo4j".to_string());
    let neo4j_pass = std::env::var("NEO4J_ENTERPRISE_PASS").unwrap_or_else(|_| "neo4j".to_string());

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Connect to the default neo4j database to insert test data
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(&neo4j_uri)
        .user(&neo4j_user)
        .password(&neo4j_pass)
        .db("neo4j")
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    // Setup SurrealDB connection
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    // Create schema and insert all test data into the neo4j database
    surreal_sync::testing::neo4j::create_constraints_and_indices(&graph, &dataset).await?;
    surreal_sync::testing::neo4j::create_nodes(&graph, &dataset).await?;
    surreal_sync::testing::neo4j::create_relationships(&graph, &dataset).await?;

    // Create a composite database with a local alias pointing to the neo4j database
    let system_config = neo4rs::ConfigBuilder::default()
        .uri(&neo4j_uri)
        .user(&neo4j_user)
        .password(&neo4j_pass)
        .db("system")
        .build()?;
    let system_graph = neo4rs::Graph::connect(system_config)?;

    let composite_name = format!("comp{test_id}");
    let alias_name = format!("{composite_name}.localdb");

    system_graph
        .run(neo4rs::query(&format!(
            "CREATE COMPOSITE DATABASE {composite_name}"
        )))
        .await?;

    // Wait for composite database to come online
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    system_graph
        .run(neo4rs::query(&format!(
            "CREATE ALIAS {alias_name} FOR DATABASE neo4j"
        )))
        .await?;

    // Wait for the alias to be available
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Perform full sync using composite constituent notation (composite.alias)
    // This tests that the dot in the database name triggers USE clause routing.
    let source_opts = surreal_sync_neo4j_source::SourceOpts {
        source_uri: neo4j_uri.clone(),
        source_database: Some(composite_name.clone()),
        source_username: Some(neo4j_user.clone()),
        source_password: Some(neo4j_pass.clone()),
        labels: vec![],
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: Some(vec![
            "all_types_users.metadata".to_string(),
            "all_types_posts.post_categories".to_string(),
        ]),
        change_tracking_property: "updated_at".to_string(),
        assumed_start_timestamp: None,
        allow_empty_tracking_timestamp: false,
        id_property: "id".to_string(),
        composite_constituent: Some(alias_name.clone()),
    };

    let sync_opts = surreal_sync_neo4j_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal_sync_surreal::v2::Surreal2Sink::new(client.clone());
            surreal_sync_neo4j_source::run_full_sync::<_, surreal_sync_core::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal_sync_surreal::v3::Surreal3Sink::new(client.clone());
            surreal_sync_neo4j_source::run_full_sync::<_, surreal_sync_core::NullStore>(
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
        "Neo4j composite full sync",
        SourceDatabase::Neo4j,
    )
    .await?;

    // Cleanup
    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;
    let _ = system_graph
        .run(neo4rs::query(&format!("DROP ALIAS {alias_name}")))
        .await;
    let _ = system_graph
        .run(neo4rs::query(&format!(
            "DROP COMPOSITE DATABASE {composite_name}"
        )))
        .await;

    Ok(())
}
