//! CLI e2e: `from mssql sync --snapshot-mode only` with the unified dataset.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::mssql::{
    assert_synced_mssql, cleanup_unified_dataset_tables, create_tables_and_indices, insert_rows,
    unified_table_args,
};
use surreal_sync::testing::surreal::{cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};

use crate::common::mssql_client;

#[tokio::test]
async fn test_mssql_sync_snapshot_only_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mssql().await;
    let test_id = generate_test_id();
    let conn_str =
        surreal_sync::testing::shared_containers::create_mssql_test_db(container, test_id).await?;
    let client = mssql_client(&conn_str).await?;

    let dataset = create_unified_full_dataset();
    cleanup_unified_dataset_tables(&client).await?;
    create_tables_and_indices(&client, &dataset, &[]).await?;
    insert_rows(&client, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let surreal = connect_auto(&config).await?;
    cleanup_surrealdb_auto(&surreal, &dataset).await?;

    let tables = unified_table_args().join(",");
    let args = [
        "from",
        "mssql",
        "sync",
        "--snapshot-mode",
        "only",
        "--connection-string",
        &conn_str,
        "--tables",
        &tables,
        "--relation-tables",
        "authored_by",
        "--surreal-endpoint",
        &config.surreal_endpoint,
        "--to-namespace",
        &config.surreal_namespace,
        "--to-database",
        &config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--chunk-size",
        "32",
    ];
    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "from mssql sync snapshot-only");

    assert_synced_mssql(&surreal, &dataset, "MSSQL full sync CLI").await?;
    Ok(())
}
