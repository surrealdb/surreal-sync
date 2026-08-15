//! Temporal UNION ALL snapshot + live UPDATE versions on unified users.

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::mssql::{
    assert_synced_mssql_temporal, cleanup_unified_dataset_tables, create_tables_and_indices,
    insert_rows, unified_table_args,
};
use surreal_sync::testing::surreal::{cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{exec_sql, mssql_client, query_debug, run_mssql_sync, sync_args};

#[tokio::test]
async fn test_mssql_temporal_versions_and_scalar_fk_lib() -> Result<(), Box<dyn std::error::Error>>
{
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
    create_tables_and_indices(&client, &dataset, &["all_types_users"]).await?;
    insert_rows(&client, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let surreal = connect_auto(&config).await?;
    cleanup_surrealdb_auto(&surreal, &dataset).await?;

    let checkpoint_dir = format!(".test-mssql-temporal-{test_id}");
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

    assert_synced_mssql_temporal(
        &surreal,
        &dataset,
        "MSSQL temporal unified snapshot",
        &["all_types_users"],
    )
    .await?;

    let posts = query_debug(&surreal, "SELECT * FROM all_types_posts").await?;
    assert!(
        !surreal_sync::testing::mssql::dump_has_record_link(&posts, "all_types_users"),
        "FK to temporal users must stay scalar: {posts}"
    );

    exec_sql(
        &conn_str,
        "UPDATE dbo.all_types_users SET name = N'Alice Smith v2' WHERE user_id = N'user_001';",
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
    assert!(
        users.contains("Alice Smith v2"),
        "live UPDATE version missing: {users}"
    );
    let current = query_debug(
        &surreal,
        "SELECT * FROM all_types_users WHERE is_current AND user_id = 'user_001'",
    )
    .await?;
    assert!(current.contains("Alice Smith v2"), "{current}");
    let history = query_debug(
        &surreal,
        "SELECT * FROM all_types_users WHERE is_current = false AND user_id = 'user_001'",
    )
    .await?;
    assert!(
        history.contains("Alice Smith"),
        "history version missing original unified name: {history}"
    );

    cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
