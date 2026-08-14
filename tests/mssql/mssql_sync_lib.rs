//! Interleaved watermark snapshot + CDC tail (library).

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{exec_sql, ordinary_schema_sql, query_debug, run_mssql_sync, sync_args};

#[tokio::test]
async fn test_mssql_interleaved_snapshot_and_tail_lib() -> Result<(), Box<dyn std::error::Error>> {
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

    let checkpoint_dir = format!(".test-mssql-sync-lib-{test_id}");
    cleanup_checkpoint_dir(&checkpoint_dir)?;

    let args = sync_args(
        conn_str.clone(),
        vec![
            "dbo.users".into(),
            "dbo.posts".into(),
            "dbo.authored".into(),
        ],
        &config,
        SnapshotModeArg::Only,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        None,
        vec!["authored".into()],
    );
    run_mssql_sync(args).await?;

    let users = query_debug(&surreal, "SELECT * FROM users").await?;
    assert!(users.contains("alice"), "{users}");
    assert!(users.contains("bob"), "{users}");
    let posts = query_debug(&surreal, "SELECT * FROM posts").await?;
    assert!(posts.contains("hello"), "{posts}");
    assert!(
        posts.contains("users") || posts.contains("user_id"),
        "FK to users should be a record link: {posts}"
    );
    let authored = query_debug(&surreal, "SELECT * FROM authored").await?;
    assert!(
        !authored.trim().is_empty(),
        "expected relation rows: {authored}"
    );

    exec_sql(
        &conn_str,
        "INSERT INTO dbo.users (id, name) VALUES (3, N'carol'); \
         UPDATE dbo.posts SET title = N'updated' WHERE id = 10; \
         DELETE FROM dbo.authored WHERE post_id = 11; \
         DELETE FROM dbo.posts WHERE id = 11;",
    )
    .await?;

    let tail = sync_args(
        conn_str,
        vec![
            "dbo.users".into(),
            "dbo.posts".into(),
            "dbo.authored".into(),
        ],
        &config,
        SnapshotModeArg::Never,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        Some("20s".into()),
        vec!["authored".into()],
    );
    run_mssql_sync(tail).await?;

    let users = query_debug(&surreal, "SELECT * FROM users").await?;
    assert!(users.contains("carol"), "CDC insert missing: {users}");
    let posts = query_debug(&surreal, "SELECT * FROM posts").await?;
    assert!(posts.contains("updated"), "CDC update missing: {posts}");
    assert!(!posts.contains("world"), "CDC delete missing: {posts}");

    cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
