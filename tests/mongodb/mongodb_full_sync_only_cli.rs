//! MongoDB all-types full sync CLI E2E test
//!
//! This test validates that the CLI handles MongoDB full sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};

#[tokio::test]
async fn test_mongodb_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    // Setup MongoDB with test data
    let mongodb_client = surreal_sync::testing::mongodb::connect_mongodb().await?;
    let db = mongodb_client.database("testdb");

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    surreal_sync::testing::mongodb::insert_docs(&db, &dataset).await?;

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::new(test_id, "neo4j-test8");
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Execute CLI command for MongoDB full sync with data
    let args = [
        "from",
        "mongodb",
        "full",
        "--connection-string",
        "mongodb://root:root@mongodb:27017",
        "--database",
        "testdb",
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
    ];

    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "MongoDB all-types full sync CLI");

    assert_synced_auto(&conn, &dataset, "MongoDB full sync CLI").await?;

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
