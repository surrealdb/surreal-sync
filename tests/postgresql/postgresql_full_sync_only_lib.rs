//! PostgreSQL all-types E2E test
//!
//! This test validates that PostgreSQL full and incremental sync operations
//! preserve all data types correctly when syncing to SurrealDB, using the
//! unified dataset.

use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};
use surreal_sync::{SourceOpts, SurrealOpts};

#[tokio::test]
async fn test_postgresql_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    // Test edge cases for complex PostgreSQL types
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup connections
    let pg_config = surreal_sync::testing::postgresql::create_postgres_config();
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&pg_config.get_connection_string(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let surreal_config = TestConfig::new(test_id, "neo4j-test2");
    let surreal = connect_surrealdb(&surreal_config).await?;

    // Clean up
    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    let source_opts = SourceOpts {
        source_uri: pg_config.get_connection_string(),
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

    // Execute full sync for the users table
    surreal_sync_postgresql_trigger::run_full_sync(
        surreal_sync_postgresql_trigger::SourceOpts::from(&source_opts),
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_sync_postgresql_trigger::SurrealOpts::from(&surreal_opts),
        None,
    )
    .await?;

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "PostgreSQL full sync only",
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
