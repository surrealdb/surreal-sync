//! PostgreSQL pgoutput WAL stream-only CLI E2E test (snapshot then catch-up stream).

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::postgresql_pgoutput_e2e::{connect_pg, WalTestIds};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_postgresql_pgoutput_stream_only_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_postgresql_pgoutput().await;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();
    let ids = WalTestIds::new(test_id);

    let checkpoint_dir = format!(".test-postgresql-pgoutput-stream-cli-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgresql_pgoutput_test_db(
            container, test_id,
        )
        .await?;

    let pg_client = connect_pg(&test_conn_str).await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    let snapshot_args = [
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
        "--checkpoint-dir",
        &checkpoint_dir,
    ];

    let output = execute_surreal_sync(&snapshot_args)?;
    assert_cli_success(&output, "PostgreSQL WAL snapshot phase CLI");

    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    use checkpoint::{Checkpoint, SyncPhase};
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, SyncPhase::FullSyncStart).await?;
    let wal_checkpoint: surreal_sync_postgresql_pgoutput_source::PgoutputCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = wal_checkpoint.to_cli_string();

    let stream_args = [
        "from",
        "postgresql-pgoutput",
        "sync",
        "--snapshot-mode",
        "never",
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
        "--from",
        &checkpoint_string,
        "--stop-after",
        "10s",
    ];

    let stream_output = execute_surreal_sync(&stream_args)?;
    assert_cli_success(&stream_output, "PostgreSQL WAL stream-only sync CLI");

    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL WAL stream-only sync CLI",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
