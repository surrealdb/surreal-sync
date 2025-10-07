//! Neo4j all-types incremental sync CLI E2E test
//!
//! This test validates that the CLI handles Neo4j incremental sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};

#[tokio::test]
async fn test_neo4j_incremental_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let t1 = chrono::Utc::now();
    let dataset = create_unified_full_dataset();

    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints")?;

    // Setup Neo4j with test data
    let neo4j_config = surreal_sync::testing::neo4j::Neo4jConfig::default();
    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(neo4j_config.get_uri())
        .user(neo4j_config.get_username())
        .password(neo4j_config.get_password())
        .db(neo4j_config.get_database())
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;

    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    // Create schema/constraints but don't insert data yet
    surreal_sync::testing::neo4j::create_constraints_and_indices(&graph, &dataset).await?;

    // Setup SurrealDB connection for validation
    let surreal_config = TestConfig::new(test_id, "neo4j-incremental-cli");
    let surreal = connect_surrealdb(&surreal_config).await?;
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    // Execute CLI command for initial full sync (without data to get checkpoint)
    let full_sync_args = [
        "full",
        "neo4j",
        "--source-uri",
        &neo4j_config.get_uri(),
        "--source-database",
        &neo4j_config.get_database(),
        "--source-username",
        &neo4j_config.get_username(),
        "--source-password",
        &neo4j_config.get_password(),
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
        "--neo4j-timezone",
        "UTC",
        "--emit-checkpoints",
        "--checkpoint-dir",
        ".test-checkpoints",
    ];

    let output = execute_surreal_sync(&full_sync_args)?;
    assert_cli_success(&output, "Neo4j all-types full sync CLI");

    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-checkpoints")?;

    // let t1 = surreal_sync::checkpoint::get_first_checkpoint_from_dir(".test-checkpoints").await?;
    // let t1_checkpoint = t1.to_neo4j_timestamp()?.to_rfc3339();
    // let t1_str = t1_checkpoint.as_str();
    // println!("T1 Checkpoint: {t1_str}");

    let t1_string = t1.to_rfc3339();
    let t1_str = t1_string.as_str();

    // Now insert test data into Neo4j (with timestamps for incremental tracking)
    surreal_sync::testing::neo4j::create_nodes(&graph, &dataset).await?;

    // Execute CLI incremental sync command
    // For Neo4j incremental sync, we use timestamp-based checkpoints
    let incremental_args = [
        "incremental",
        "neo4j",
        "--source-uri",
        &neo4j_config.get_uri(),
        "--source-database",
        &neo4j_config.get_database(),
        "--source-username",
        &neo4j_config.get_username(),
        "--source-password",
        &neo4j_config.get_password(),
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
        "--neo4j-timezone",
        "UTC",
        "--neo4j-json-properties",
        "all_types_users.metadata,all_types_posts.post_categories",
        "--incremental-from",
        &format!("neo4j:{t1_str}"), // Start from beginning of time
    ];

    let incremental_output = execute_surreal_sync(&incremental_args)?;
    assert_cli_success(&incremental_output, "Neo4j all-types incremental sync CLI");

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stderr));

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "Neo4j incremental sync only",
    )
    .await?;

    surreal_sync::testing::neo4j::delete_nodes_and_relationships(&graph).await?;

    Ok(())
}
