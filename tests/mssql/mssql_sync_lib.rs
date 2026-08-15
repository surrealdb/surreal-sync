//! Interleaved watermark snapshot + CDC tail (library) on the unified dataset.

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::mssql::{
    assert_synced_mssql, cleanup_unified_dataset_tables, create_tables_and_indices, insert_rows,
    unified_table_args,
};
use surreal_sync::testing::surreal::{cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{exec_sql, mssql_client, query_debug, run_mssql_sync, sync_args};

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
    let client = mssql_client(&conn_str).await?;

    let dataset = create_unified_full_dataset();
    cleanup_unified_dataset_tables(&client).await?;
    create_tables_and_indices(&client, &dataset, &[]).await?;
    insert_rows(&client, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let surreal = connect_auto(&config).await?;
    cleanup_surrealdb_auto(&surreal, &dataset).await?;

    let checkpoint_dir = format!(".test-mssql-sync-lib-{test_id}");
    cleanup_checkpoint_dir(&checkpoint_dir)?;

    let args = sync_args(
        conn_str.clone(),
        unified_table_args(),
        &config,
        SnapshotModeArg::Only,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        None,
        vec!["authored_by".into()],
    );
    run_mssql_sync(args).await?;

    assert_synced_mssql(&surreal, &dataset, "MSSQL interleaved snapshot").await?;

    exec_sql(
        &conn_str,
        "INSERT INTO dbo.all_types_users (user_id, name, email) \
         VALUES (N'user_003', N'Carol Lee', N'carol@example.com'); \
         UPDATE dbo.all_types_posts SET title = N'updated' WHERE post_id = N'post_001'; \
         DELETE FROM dbo.authored_by WHERE post_id = N'post_002'; \
         DELETE FROM dbo.all_types_posts WHERE post_id = N'post_002';",
    )
    .await?;

    let tail = sync_args(
        conn_str,
        unified_table_args(),
        &config,
        SnapshotModeArg::Never,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        Some("20s".into()),
        vec!["authored_by".into()],
    );
    run_mssql_sync(tail).await?;

    let users = query_debug(&surreal, "SELECT * FROM all_types_users").await?;
    assert!(users.contains("Carol Lee"), "CDC insert missing: {users}");
    let posts = query_debug(&surreal, "SELECT * FROM all_types_posts").await?;
    assert!(posts.contains("updated"), "CDC update missing: {posts}");
    assert!(
        !posts.contains("Advanced Sync Patterns"),
        "CDC delete missing: {posts}"
    );

    cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
