//! PostgreSQL pgoutput WAL snapshot-only CLI E2E test.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::postgresql_pgoutput_e2e::{connect_pg, WalTestIds};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_postgresql_pgoutput_snapshot_only_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_postgresql_pgoutput().await;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();
    let ids = WalTestIds::new(test_id);

    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgresql_pgoutput_test_db(
            container, test_id,
        )
        .await?;

    let pg_client = connect_pg(&test_conn_str).await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;
    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    let args = [
        "from",
        "postgresql-pgoutput",
        "sync",
        "--snapshot-mode",
        "only",
        "--connection-string",
        &test_conn_str,
        "--slot",
        &ids.slot_name,
        "--publication",
        &ids.publication_name,
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
    assert_cli_success(&output, "PostgreSQL WAL snapshot-only sync CLI");

    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL WAL snapshot-only sync CLI",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
