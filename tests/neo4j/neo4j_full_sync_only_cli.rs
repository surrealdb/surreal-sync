//! Neo4j all-types full sync CLI E2E test
//!
//! This test validates that the CLI handles Neo4j full sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};
use surreal_sync_neo4j_source::testing::container::Neo4jContainer;

#[tokio::test]
async fn test_neo4j_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    let mut container = Neo4jContainer::new(&format!("test-neo4j-{test_id}"));
    container.start()?;
    container.wait_until_ready(30).await?;

    // Setup Neo4j with test data
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(container.bolt_uri())
        .user(&container.username)
        .password(&container.password)
        .db(&*container.database)
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    // Create schema and insert all test data
    surreal_sync::testing::neo4j::create_constraints_and_indices(&graph, &dataset).await?;
    surreal_sync::testing::neo4j::create_nodes(&graph, &dataset).await?;

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Execute CLI command for Neo4j full sync with data
    let bolt_uri = container.bolt_uri();
    let args = [
        "from",
        "neo4j",
        "full",
        "--connection-string",
        &bolt_uri,
        "--database",
        &container.database,
        "--username",
        &container.username,
        "--password",
        &container.password,
        "--surreal-endpoint",
        &surreal_config.surreal_endpoint,
        "--to-namespace",
        &surreal_config.surreal_namespace,
        "--to-database",
        &surreal_config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--timezone",
        "UTC",
        "--json-properties",
        "all_types_users.metadata,all_types_posts.post_categories",
    ];

    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "Neo4j all-types full sync CLI");

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&output.stderr));

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
