//! CLI Integration Tests for PostgreSQL Logical Replication Full Sync
//!
//! These tests verify that the surreal-sync CLI handles PostgreSQL logical
//! replication full sync correctly using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::surrealdb_container::SurrealDbContainer;
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};
use surreal_sync_postgresql::testing::container::PostgresContainer;

/// Test PostgreSQL logical replication full sync CLI
#[tokio::test]
async fn test_postgresql_logical_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let mut surrealdb = SurrealDbContainer::new("test-pgl-full-sync-cli-sdb");
    surrealdb.start()?;
    surrealdb.wait_until_ready(30)?;

    let mut container = PostgresContainer::new("test-logical-full-sync-cli");
    container.build_image()?;
    container.start()?;
    container.wait_until_ready(30).await?;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    // Setup PostgreSQL with test data
    let connection_string = container.connection_url();
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;
    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Get table names from the dataset
    let table_names: Vec<String> = dataset.tables.iter().map(|t| t.name.clone()).collect();
    let tables_arg = table_names.join(",");

    // Execute CLI command with data
    // Note: database is extracted from connection string, not passed separately
    let args = [
        "from",
        "postgresql",
        "full",
        "--connection-string",
        &connection_string,
        "--slot",
        "surreal_sync_test_slot",
        "--tables",
        &tables_arg,
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
    assert_cli_success(&output, "PostgreSQL logical full sync CLI");

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&output.stderr));

    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL logical full sync CLI",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    // Cleanup: drop the replication slot
    pg_client
        .execute(
            "SELECT pg_drop_replication_slot('surreal_sync_test_slot')",
            &[],
        )
        .await
        .ok(); // Ignore errors if slot doesn't exist

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
