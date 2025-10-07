//! MySQL all-types E2E test
//!
//! This test validates that MySQL full sync operations preserve all data types
//! correctly when syncing to SurrealDB, using the unified dataset.

use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};
use surreal_sync::{mysql, SourceOpts, SurrealOpts};

#[tokio::test]
async fn test_mysql_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test dataset
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup MySQL connection and data
    let mysql_config = surreal_sync::testing::mysql::create_mysql_config();
    let pool = mysql_async::Pool::from_url(mysql_config.get_connection_string())?;
    let mut mysql_conn = pool.get_conn().await?;

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "neo4j-test3");
    let surreal = connect_surrealdb(&surreal_config).await?;

    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;
    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    surreal_sync::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;
    surreal_sync::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    // Perform full sync from MySQL to SurrealDB
    let source_opts = SourceOpts {
        source_uri: mysql_config.get_connection_string(),
        source_database: Some("testdb".to_string()),
        source_username: None,
        source_password: None,
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: None,
        mysql_boolean_paths: Some(vec![
            "all_types_users.metadata=settings.notifications".to_string()
        ]),
    };

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 1000,
        dry_run: false,
    };

    let surreal2 = surreal_sync::connect::connect_to_surrealdb(
        &surreal_opts,
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
    )
    .await?;

    // Execute full sync from MySQL to SurrealDB
    mysql::run_full_sync(&source_opts, &surreal_opts, None, &surreal2).await?;

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "PostgreSQL full sync only",
    )
    .await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}
