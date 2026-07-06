//! Shared end-to-end sync test bodies for the MySQL/MariaDB binlog CDC source.
//!
//! MySQL and MariaDB share the same binlog source code path (they differ only by
//! container image and JSON storage nuances handled inside the source), so their
//! lib e2e tests are identical apart from which binlog container they run against.

use crate::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use crate::testing::{create_unified_full_dataset, SourceDatabase, TestConfig};
use surreal_sync_mysql_binlog_source::{
    BinlogCheckpoint, ReplicationTailOptions, SourceOpts, SyncOpts,
};

/// Which MySQL-compatible engine a binlog e2e test runs against.
#[derive(Clone, Copy, Debug)]
pub enum Engine {
    MySql,
    MariaDb,
}

/// Run a full-sync-only e2e test against a MySQL-compatible database reachable at
/// `test_conn_str` (database `test_{test_id}`), asserting parity in SurrealDB.
pub async fn run_binlog_full_sync_e2e(
    test_conn_str: &str,
    test_id: u64,
    surreal_ws_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let dataset = create_unified_full_dataset();

    let pool = mysql_async::Pool::from_url(test_conn_str)?;
    let mut mysql_conn = pool.get_conn().await?;

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, surreal_ws_endpoint);
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;
    crate::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    crate::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;
    crate::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    let source_opts = SourceOpts {
        connection_string: test_conn_str.to_string(),
        database: Some(format!("test_{test_id}")),
        tables: vec![],
        server_id: None,
        flavor: None,
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_binlog_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_binlog_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "binlog full sync only",
        SourceDatabase::MySQL,
    )
    .await?;

    crate::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}

/// Run an incremental-sync-only e2e test: full sync on empty tables captures a
/// binlog checkpoint, rows are inserted, then incremental sync replays them.
pub async fn run_binlog_incremental_e2e(
    test_conn_str: &str,
    test_id: u64,
    surreal_ws_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    run_binlog_incremental_e2e_inner(test_conn_str, test_id, surreal_ws_endpoint, false).await
}

/// Same as [`run_binlog_incremental_e2e`] but connects with the least-privilege
/// `surreal_sync` replication user instead of root.
pub async fn run_binlog_incremental_e2e_repl_user(
    container: &crate::testing::mysql_binlog_container::MySQLBinlogContainer,
    test_id: u64,
    surreal_ws_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_name = format!("test_{test_id}");
    crate::testing::shared_containers::setup_binlog_user(container, &db_name).await?;
    let repl_conn = container
        .connection_string
        .replace("root:testpass@", "surreal_sync:surreal_sync_pass@")
        .replace("/testdb", &format!("/{db_name}"));
    run_binlog_incremental_e2e_inner(&repl_conn, test_id, surreal_ws_endpoint, true).await
}

async fn run_binlog_incremental_e2e_inner(
    test_conn_str: &str,
    test_id: u64,
    surreal_ws_endpoint: &str,
    repl_user: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let checkpoint_dir = format!(".test-mysql-binlog-incr-lib-checkpoints-{test_id}");
    crate::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_db_name = format!("test_{test_id}");

    let pool = mysql_async::Pool::from_url(test_conn_str)?;
    let mut mysql_conn = pool.get_conn().await?;

    let dataset = create_unified_full_dataset();

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, surreal_ws_endpoint);
    let conn = connect_auto(&surreal_config).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;

    crate::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    crate::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;

    let source_opts = SourceOpts {
        connection_string: test_conn_str.to_string(),
        database: Some(test_db_name.clone()),
        tables: vec![],
        server_id: None,
        flavor: None,
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let checkpoint_store = checkpoint::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_binlog_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_binlog_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
    }

    crate::testing::checkpoint::verify_t1_t2_checkpoints(&checkpoint_dir)?;

    crate::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, checkpoint::SyncPhase::FullSyncStart)
            .await?;
    let sync_checkpoint: BinlogCheckpoint = checkpoint_file.parse()?;

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_binlog_source::run_replication_tail_with_checkpoints::<
                _,
                checkpoint::NullStore,
            >(
                &sink,
                source_opts,
                sync_checkpoint,
                ReplicationTailOptions::stream(
                    Some(chrono::Utc::now() + chrono::Duration::seconds(10)),
                    None,
                ),
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_binlog_source::run_replication_tail_with_checkpoints::<
                _,
                checkpoint::NullStore,
            >(
                &sink,
                source_opts,
                sync_checkpoint,
                ReplicationTailOptions::stream(
                    Some(chrono::Utc::now() + chrono::Duration::seconds(10)),
                    None,
                ),
                None,
            )
            .await?;
        }
    }

    let label = if repl_user {
        "binlog incremental sync (repl user)"
    } else {
        "binlog incremental sync only"
    };
    assert_synced_auto(&conn, &dataset, label, SourceDatabase::MySQL).await?;

    crate::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    drop(mysql_conn);
    pool.disconnect().await?;

    Ok(())
}
