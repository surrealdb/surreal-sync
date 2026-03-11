//! MySQL all-types full sync CLI E2E test
//!
//! This test validates that the CLI handles MySQL full sync with all data types
//! correctly, using the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_mysql_full_sync_cli() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mysql().await;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    let test_conn_str = surreal_sync::testing::shared_containers::create_mysql_test_db(container, test_id).await?;

    // Setup MySQL with test data
    let pool = mysql_async::Pool::from_url(&test_conn_str)?;
    let mut mysql_conn = pool.get_conn().await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    surreal_sync::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;
    surreal_sync::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    // Setup SurrealDB container
    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    // Execute CLI command for MySQL full sync with data
    let mysql_conn_str = test_conn_str.clone();
    let test_db_name = format!("test_{test_id}");
    let args = [
        "from",
        "mysql",
        "full",
        "--connection-string",
        &mysql_conn_str,
        "--database",
        &test_db_name,
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
        "--boolean-paths",
        "all_types_users.metadata=settings.notifications",
    ];

    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "MySQL all-types full sync CLI");

    println!("Standard Output:");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    println!("Error Output:");
    println!("{}", String::from_utf8_lossy(&output.stderr));

    assert_synced_auto(
        &conn,
        &dataset,
        "MySQL full sync CLI",
        SourceDatabase::MySQL,
    )
    .await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}
