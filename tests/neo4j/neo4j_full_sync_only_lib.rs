//! Neo4j all-types E2E test
//!
//! This test validates that Neo4j full sync operations preserve all data types
//! correctly when syncing to SurrealDB, using the unified dataset.

use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};
use surreal_sync::{neo4j, SourceOpts, SurrealOpts};

#[tokio::test]
async fn test_neo4j_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test dataset
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup Neo4j connection and data
    let neo4j_config = surreal_sync::testing::neo4j::Neo4jConfig::default();
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(neo4j_config.get_uri())
        .user(neo4j_config.get_username())
        .password(neo4j_config.get_password())
        .db(neo4j_config.get_database())
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "neo4j");
    let surreal = connect_surrealdb(&surreal_config).await?;

    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;
    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    // Create schema and insert all test data
    surreal_sync::testing::neo4j::create_constraints_and_indices(&graph, &dataset).await?;
    surreal_sync::testing::neo4j::create_nodes(&graph, &dataset).await?;

    // Perform full sync from Neo4j to SurrealDB
    let source_opts = SourceOpts {
        source_uri: neo4j_config.get_uri(),
        source_database: Some(neo4j_config.get_database()),
        source_username: Some(neo4j_config.get_username()),
        source_password: Some(neo4j_config.get_password()),
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: Some(vec![
            "all_types_users.metadata".to_string(),
            "all_types_posts.post_categories".to_string(),
        ]),
        mysql_boolean_paths: None,
    };

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 1000,
        dry_run: false,
    };

    // Execute full sync from Neo4j to SurrealDB
    neo4j::run_full_sync(
        source_opts,
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_opts,
        None,
    )
    .await?;

    surreal_sync::testing::surrealdb::assert_synced(&surreal, &dataset, "Neo4j full sync only")
        .await?;

    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    Ok(())
}
