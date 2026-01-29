//! PostgreSQL all-types incremental sync CLI E2E test
//!
//! This test validates that the CLI handles PostgreSQL incremental sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::postgresql::create_tables_and_indices;
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_postgresql_incremental_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();

    // Clean up checkpoint directory to prevent cross-test contamination
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints")?;

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

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::new(test_id, "neo4j-test6");
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Execute CLI command for initial full sync
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
        "--emit-checkpoints",
        "--checkpoint-dir",
        ".test-checkpoints",
    ];

    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "PostgreSQL all-types full sync CLI");

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-checkpoints")?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Read the t1 (FullSyncStart) checkpoint from the file - this is the one we need
    // for incremental sync to pick up changes made after full sync started
    use checkpoint::{Checkpoint, SyncPhase};
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(".test-checkpoints", SyncPhase::FullSyncStart).await?;
    let pg_checkpoint: surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = pg_checkpoint.to_cli_string();

    // Execute CLI incremental sync command
    // For PostgreSQL incremental sync, we need to provide a checkpoint (sequence-based)
    // Note: database is extracted from connection string, not passed separately
    let incremental_args = [
        "from",
        "postgresql-trigger",
        "incremental",
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
        "--incremental-from",
        &checkpoint_string, // Use checkpoint read from file
    ];

    let incremental_output = execute_surreal_sync(&incremental_args)?;
    assert_cli_success(
        &incremental_output,
        "PostgreSQL all-types incremental sync CLI",
    );

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stderr));

    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL incremental sync CLI",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
