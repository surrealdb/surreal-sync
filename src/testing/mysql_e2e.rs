//! Shared end-to-end sync test bodies for the MySQL trigger source.
//!
//! MySQL and MariaDB share the exact same trigger-based source code path (they
//! differ only by container image and JSON storage nuances handled inside the
//! source), so their lib e2e tests are identical apart from which container they
//! run against. These helpers hold the shared bodies; each `tests/mysql/*_lib.rs`
//! and `tests/mariadb/*_lib.rs` file is a thin wrapper that starts the right
//! container and calls in here with the resulting connection string.

use crate::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use crate::testing::{create_unified_full_dataset, SourceDatabase, TestConfig};

/// Run a full-sync-only e2e test against a MySQL-compatible database reachable at
/// `test_conn_str` (database `test_{test_id}`), asserting parity in SurrealDB.
///
/// Shared verbatim by the MySQL and MariaDB `*_full_sync_only_lib` tests.
pub async fn run_full_sync_e2e(
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

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: test_conn_str.to_string(),
        source_database: Some(format!("test_{test_id}")),
        tables: vec![],
        mysql_boolean_paths: Some(vec![
            "all_types_users.metadata=settings.notifications".to_string()
        ]),
        id_column_overrides: Default::default(),
        ssl: Default::default(),
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(&conn, &dataset, "full sync only", SourceDatabase::MySQL).await?;

    crate::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}

/// Run an incremental-sync-only e2e test against a MySQL-compatible database
/// reachable at `test_conn_str` (database `test_{test_id}`): full sync sets up
/// triggers/checkpoints on empty tables, then rows are inserted and replayed via
/// incremental sync, asserting parity in SurrealDB.
///
/// Shared verbatim by the MySQL and MariaDB `*_incremental_sync_only_lib` tests.
pub async fn run_incremental_e2e(
    test_conn_str: &str,
    test_id: u64,
    surreal_ws_endpoint: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    let checkpoint_dir = format!(".test-mysql-incr-lib-checkpoints-{test_id}");
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

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: test_conn_str.to_string(),
        source_database: Some(test_db_name),
        tables: vec![],
        mysql_boolean_paths: Some(vec!["all_types_posts.post_categories".to_string()]),
        id_column_overrides: Default::default(),
        ssl: Default::default(),
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    let checkpoint_store = checkpoint::FilesystemStore::new(&checkpoint_dir);
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync(
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
    let sync_checkpoint: surreal_sync_mysql_trigger_source::MySQLCheckpoint =
        checkpoint_file.parse()?;

    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1),
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1),
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "incremental sync only",
        SourceDatabase::MySQL,
    )
    .await?;

    crate::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    drop(mysql_conn);
    pool.disconnect().await?;

    Ok(())
}
