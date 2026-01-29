//! CLI Integration Tests with All Data Types
//!
//! These tests verify that the surreal-sync CLI handles all database-specific
//! data types correctly using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};

/// Test PostgreSQL CLI with data types
#[tokio::test]
async fn test_postgresql_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    // Setup PostgreSQL with test data
    let pg_config = surreal_sync::testing::postgresql::create_postgres_config();
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&pg_config.get_connection_string(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::new(test_id, "neo4j-test6");
    let conn = connect_auto(&surreal_config).await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Execute CLI command with data
    // Note: database is extracted from connection string, not passed separately
    let args = [
        "from",
        "postgresql-trigger",
        "full",
        "--connection-string",
        &pg_config.get_connection_string(),
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
    assert_cli_success(&output, "PostgreSQL all-types CLI sync");

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&output.stderr));

    assert_synced_auto(&conn, &dataset, "PostgreSQL full sync CLI").await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
