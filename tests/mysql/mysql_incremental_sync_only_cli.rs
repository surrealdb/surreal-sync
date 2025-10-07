//! MySQL all-types incremental sync CLI E2E test
//!
//! This test validates that the CLI handles MySQL incremental sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::{
    connect_surrealdb, create_unified_full_dataset, generate_test_id, TestConfig,
};

#[tokio::test]
async fn test_mysql_incremental_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints")?;

    // Setup MySQL with test data
    let mysql_config = surreal_sync::testing::mysql::create_mysql_config();
    let pool = mysql_async::Pool::from_url(mysql_config.get_connection_string())?;
    let mut mysql_conn = pool.get_conn().await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    surreal_sync::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;

    // Setup SurrealDB connection for validation
    let surreal_config = TestConfig::new(test_id, "neo4j-test7");
    let surreal = connect_surrealdb(&surreal_config).await?;
    surreal_sync::testing::test_helpers::cleanup_surrealdb(&surreal, &dataset).await?;

    // Execute CLI command for initial full sync
    let full_sync_args = [
        "full",
        "mysql",
        "--source-uri",
        &mysql_config.get_connection_string(),
        "--source-database",
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
        "--emit-checkpoints",
        "--checkpoint-dir",
        ".test-checkpoints",
    ];

    let output = execute_surreal_sync(&full_sync_args)?;
    assert_cli_success(&output, "MySQL all-types full sync CLI");

    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-checkpoints")?;

    let t1 = surreal_sync::testing::checkpoint::read_t1_checkpoint(".test-checkpoints")?;

    surreal_sync::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    // Execute CLI incremental sync command
    // For MySQL incremental sync, we need to provide a checkpoint
    // Since this is a test, we'll use a sequence-based checkpoint that starts from the beginning
    let incremental_args = [
        "incremental",
        "mysql",
        "--source-uri",
        &mysql_config.get_connection_string(),
        "--source-database",
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
        "--incremental-from",
        &t1.to_string(), // Start from beginning of sequence
    ];

    let incremental_output = execute_surreal_sync(&incremental_args)?;
    assert_cli_success(&incremental_output, "MySQL all-types incremental sync CLI");

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&incremental_output.stderr));

    surreal_sync::testing::surrealdb::assert_synced(
        &surreal,
        &dataset,
        "PostgreSQL incremental sync only",
    )
    .await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}
