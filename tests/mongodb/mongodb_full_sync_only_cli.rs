//! MongoDB all-types full sync CLI E2E test
//!
//! This test validates that the CLI handles MongoDB full sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_mongodb_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let container = surreal_sync::testing::shared_containers::shared_mongodb().await;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    let mongodb_client =
        surreal_sync::testing::mongodb::connect_mongodb(&container.connection_uri()).await?;
    let mongodb_database = format!("test_{test_id}");
    let db = mongodb_client.database(&mongodb_database);

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    surreal_sync::testing::mongodb::insert_docs(&db, &dataset).await?;

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Execute CLI command for MongoDB full sync with data
    let args = [
        "from",
        "mongodb",
        "full",
        "--connection-string",
        &container.connection_uri(),
        "--database",
        &mongodb_database,
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

    assert_synced_auto(
        &conn,
        &dataset,
        "MongoDB full sync CLI",
        SourceDatabase::MongoDB,
    )
    .await?;

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
