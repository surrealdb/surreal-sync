//! MongoDB all-types E2E test
//!
//! This test validates that MongoDB full sync operations preserve all data types
//! correctly when syncing to SurrealDB, using the unified dataset.

use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};
use surreal_sync::{mongodb, SourceOpts, SurrealOpts};

#[tokio::test]
async fn test_mongodb_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test dataset
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup MongoDB connection and data
    let mongodb_client = surreal_sync::testing::mongodb::connect_mongodb().await?;
    let db = mongodb_client.database("testdb");

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "neo4j-test4");
    let surreal = connect_surrealdb(&surreal_config).await?;

    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;
    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    surreal_sync::testing::mongodb::insert_docs(&db, &dataset).await?;

    let source_opts = SourceOpts {
        source_uri: "mongodb://root:root@mongodb:27017".to_string(),
        source_database: Some("testdb".to_string()),
        source_username: None,
        source_password: None,
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: None,
        mysql_boolean_paths: None,
    };

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 1000,
        dry_run: false,
    };

    // Execute full sync from MongoDB to SurrealDB
    mongodb::migrate_from_mongodb(
        source_opts,
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_opts,
    )
    .await?;

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "MongoDB full sync - all data types",
    )
    .await?;

    // Clean up
    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
