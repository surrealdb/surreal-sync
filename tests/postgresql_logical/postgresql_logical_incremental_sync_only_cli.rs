//! CLI Integration Tests for PostgreSQL Logical Replication Incremental Sync
//!
//! These tests verify that the surreal-sync CLI handles PostgreSQL logical
//! replication incremental sync correctly using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::postgresql::create_tables_and_indices;
use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};

/// Test PostgreSQL logical replication incremental sync CLI
#[tokio::test]
#[ignore = "Requires wal2json PostgreSQL extension not available in devcontainer"]
async fn test_postgresql_logical_incremental_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();

    // Clean up checkpoint directory to prevent cross-test contamination
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-logical-checkpoints")?;

    // Setup PostgreSQL with test data
    let pg_config = surreal_sync::testing::postgresql::create_postgres_config();
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&pg_config.get_connection_string(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Create the empty table in PostgreSQL
    let dataset = create_unified_full_dataset();
    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    create_tables_and_indices(&pg_client, &dataset).await?;

    // Setup SurrealDB connection for validation
    let surreal_config = TestConfig::new(test_id, "postgresql-logical-incr-test1");
    let surreal = connect_surrealdb(&surreal_config).await?;

    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    // Get table names from the dataset
    let table_names: Vec<String> = dataset.tables.iter().map(|t| t.name.clone()).collect();
    let tables_arg = table_names.join(",");

    // Execute CLI command for initial full sync
    let args = [
        "from",
        "postgresql",
        "full",
        "--connection-string",
        &pg_config.get_connection_string(),
        "--slot",
        "surreal_sync_incr_test_slot",
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
        "--emit-checkpoints",
        "--checkpoint-dir",
        ".test-logical-checkpoints",
    ];

    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "PostgreSQL logical full sync CLI");

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-logical-checkpoints")?;

    // Now insert some data for incremental sync to pick up
    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Read the t1 (FullSyncStart) checkpoint from the file
    use checkpoint::{Checkpoint, SyncPhase};
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(".test-logical-checkpoints", SyncPhase::FullSyncStart)
            .await?;
    let pg_checkpoint: surreal_sync_postgresql_wal2json_source::PostgreSQLLogicalCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = pg_checkpoint.to_cli_string();

    // Execute CLI incremental sync command
    let incremental_args = [
        "from",
        "postgresql",
        "incremental",
        "--connection-string",
        &pg_config.get_connection_string(),
        "--slot",
        "surreal_sync_incr_test_slot",
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
        "--incremental-from",
        &checkpoint_string,
        "--timeout",
        "30", // Short timeout for test
    ];

    let incremental_output = execute_surreal_sync(&incremental_args)?;
    assert_cli_success(
        &incremental_output,
        "PostgreSQL logical incremental sync CLI",
    );

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stderr));

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "PostgreSQL logical incremental sync only",
    )
    .await?;

    // Cleanup: drop the replication slot
    pg_client
        .execute(
            "SELECT pg_drop_replication_slot('surreal_sync_incr_test_slot')",
            &[],
        )
        .await
        .ok(); // Ignore errors if slot doesn't exist

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
