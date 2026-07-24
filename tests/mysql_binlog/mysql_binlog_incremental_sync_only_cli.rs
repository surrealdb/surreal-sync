//! MySQL binlog stream-only CLI E2E test (snapshot then catch-up stream).

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{assert_synced_auto, cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_mysql_binlog_stream_only_cli() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mysql_binlog().await;

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    let checkpoint_dir = format!(".test-mysql-binlog-stream-cli-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_conn_str =
        surreal_sync::testing::shared_containers::create_binlog_test_db(container, test_id).await?;
    let test_db_name = format!("test_{test_id}");

    let pool = mysql_async::Pool::from_url(&test_conn_str)?;
    let mut mysql_conn = pool.get_conn().await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    surreal_sync::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    let mysql_conn_str = test_conn_str.clone();
    let snapshot_args = [
        "from",
        "mysql-binlog",
        "sync",
        "--snapshot-mode",
        "only",
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
        "--checkpoint-dir",
        &checkpoint_dir,
    ];

    let output = execute_surreal_sync(&snapshot_args)?;
    assert_cli_success(&output, "MySQL binlog snapshot phase CLI");

    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    surreal_sync::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    use surreal_sync_core::{Checkpoint, SyncPhase};
    let checkpoint_file = surreal_sync_runtime::checkpoint_fs::get_checkpoint_for_phase(
        &checkpoint_dir,
        SyncPhase::FullSyncStart,
    )
    .await?;
    let binlog_checkpoint: surreal_sync_mysql::from_binlog::BinlogCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = binlog_checkpoint.to_cli_string();

    let stream_args = [
        "from",
        "mysql-binlog",
        "sync",
        "--snapshot-mode",
        "never",
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
        "--from",
        &checkpoint_string,
        "--stop-after",
        "10s",
    ];

    let stream_output = execute_surreal_sync(&stream_args)?;
    assert_cli_success(&stream_output, "MySQL binlog stream-only sync CLI");

    assert_synced_auto(
        &conn,
        &dataset,
        "MySQL binlog stream-only sync CLI",
        SourceDatabase::MySQL,
    )
    .await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}
