//! CLI Integration Tests with All Data Types
//!
//! These tests verify that the surreal-sync CLI handles all database-specific
//! data types correctly using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

/// Test PostgreSQL CLI with data types
#[tokio::test]
async fn test_postgresql_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let container = surreal_sync::testing::shared_containers::shared_postgres().await;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();
    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgres_test_db(container, test_id)
            .await?;

    let (pg_client, pg_connection) =
        tokio_postgres::connect(&test_conn_str, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
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
        &test_conn_str,
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

    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL full sync CLI",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
