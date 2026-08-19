//! `--schemafull` emits DEFINE TABLE/INDEX; default stays schemaless.

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::mssql::{
    assert_synced_mssql_temporal, cleanup_unified_dataset_tables, create_tables_and_indices,
    insert_rows, unified_table_args,
};
use surreal_sync::testing::surreal::{cleanup_surrealdb_auto, connect_auto};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{mssql_client, query_debug, run_mssql_sync, sync_args};

#[tokio::test]
async fn test_mssql_schemafull_emits_define_default_does_not(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let container = surreal_sync::testing::shared_containers::shared_mssql().await;
    let test_id = generate_test_id();
    let conn_str =
        surreal_sync::testing::shared_containers::create_mssql_test_db(container, test_id).await?;
    let client = mssql_client(&conn_str).await?;

    // Ordinary users (unique email index) + temporal posts (cookbook indexes).
    let dataset = create_unified_full_dataset();
    cleanup_unified_dataset_tables(&client).await?;
    create_tables_and_indices(&client, &dataset, &["all_types_posts"]).await?;
    insert_rows(&client, &dataset).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let full_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let mut schemaless_config = full_config.clone();
    schemaless_config.surreal_namespace = format!("test_ns_sl_{test_id}");
    schemaless_config.surreal_database = format!("test_db_sl_{test_id}");

    let full = connect_auto(&full_config).await?;
    let sl = connect_auto(&schemaless_config).await?;
    cleanup_surrealdb_auto(&full, &dataset).await?;
    cleanup_surrealdb_auto(&sl, &dataset).await?;

    let tables = unified_table_args();
    let relations = vec!["authored_by".into()];

    let checkpoint_full = format!(".test-mssql-schemafull-{test_id}");
    let checkpoint_sl = format!(".test-mssql-schemaless-{test_id}");
    cleanup_checkpoint_dir(&checkpoint_full)?;
    cleanup_checkpoint_dir(&checkpoint_sl)?;

    run_mssql_sync(sync_args(
        conn_str.clone(),
        tables.clone(),
        &full_config,
        SnapshotModeArg::Only,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_full.clone()),
        true,
        None,
        relations.clone(),
    ))
    .await?;

    run_mssql_sync(sync_args(
        conn_str,
        tables,
        &schemaless_config,
        SnapshotModeArg::Only,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_sl.clone()),
        false,
        None,
        relations,
    ))
    .await?;

    assert_synced_mssql_temporal(
        &full,
        &dataset,
        "MSSQL schemafull content",
        &["all_types_posts"],
    )
    .await?;
    assert_synced_mssql_temporal(
        &sl,
        &dataset,
        "MSSQL schemaless content",
        &["all_types_posts"],
    )
    .await?;

    let users_info = query_debug(&full, "INFO FOR TABLE all_types_users").await?;
    assert!(
        users_info.contains("idx_users_email") || users_info.contains("UNIQUE"),
        "ordinary unique index missing under --schemafull: {users_info}"
    );

    let posts_info = query_debug(&full, "INFO FOR TABLE all_types_posts").await?;
    assert!(
        posts_info.contains("is_current"),
        "temporal cookbook index missing: {posts_info}"
    );
    assert!(
        !posts_info.to_ascii_uppercase().contains("UNIQUE") || !posts_info.contains("post_id"),
        "must not copy source UNIQUE/PK onto unified temporal table: {posts_info}"
    );

    let sl_users = query_debug(&sl, "INFO FOR TABLE all_types_users").await?;
    assert!(
        !sl_users.contains("idx_users_email"),
        "default run must not copy indexes: {sl_users}"
    );
    let sl_posts = query_debug(&sl, "INFO FOR TABLE all_types_posts").await?;
    assert!(
        !sl_posts.contains("all_types_posts_is_current"),
        "default run must not emit cookbook indexes: {sl_posts}"
    );

    cleanup_checkpoint_dir(&checkpoint_full)?;
    cleanup_checkpoint_dir(&checkpoint_sl)?;
    Ok(())
}
