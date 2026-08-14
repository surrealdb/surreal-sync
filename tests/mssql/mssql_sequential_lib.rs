//! Sequential SNAPSHOT-isolation dump, and the disabled-isolation error.

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{exec_sql, ordinary_schema_sql, query_debug, run_mssql_sync, sync_args};

#[tokio::test]
async fn test_mssql_sequential_snapshot_lib() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mssql().await;
    let test_id = generate_test_id();
    let conn_str =
        surreal_sync::testing::shared_containers::create_mssql_test_db(container, test_id).await?;
    exec_sql(&conn_str, ordinary_schema_sql()).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let surreal = connect_auto(&config).await?;
    cleanup_auto(&surreal, &["users", "posts", "authored"]).await?;

    let checkpoint_dir = format!(".test-mssql-sequential-{test_id}");
    cleanup_checkpoint_dir(&checkpoint_dir)?;

    let args = sync_args(
        conn_str,
        vec!["dbo.users".into(), "dbo.posts".into()],
        &config,
        SnapshotModeArg::Only,
        SyncStrategy::SequentialSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        None,
        vec![],
    );
    run_mssql_sync(args).await?;

    let users = query_debug(&surreal, "SELECT * FROM users").await?;
    assert!(users.contains("alice"), "{users}");
    let posts = query_debug(&surreal, "SELECT * FROM posts").await?;
    assert!(posts.contains("hello"), "{posts}");

    cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}

#[tokio::test]
async fn test_mssql_sequential_snapshot_isolation_disabled(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mssql().await;
    let test_id = generate_test_id();
    let db_name = format!("nosnap_{test_id}");
    let master = container.connection_string_for("master");
    exec_sql(
        &master,
        &format!("IF DB_ID(N'{db_name}') IS NULL CREATE DATABASE [{db_name}];"),
    )
    .await?;
    let conn_str = container.connection_string_for(&db_name);

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());

    let args = sync_args(
        conn_str,
        vec![],
        &config,
        SnapshotModeArg::Only,
        SyncStrategy::SequentialSnapshot,
        None,
        false,
        None,
        vec![],
    );
    let err = run_mssql_sync(args)
        .await
        .expect_err("sequential dump must fail when snapshot isolation is off");
    let msg = err.to_string();
    assert!(
        msg.contains("ALTER DATABASE") && msg.contains("ALLOW_SNAPSHOT_ISOLATION ON"),
        "expected copy-paste T-SQL, got: {msg}"
    );
    Ok(())
}
