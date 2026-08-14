//! `--schemafull` emits DEFINE TABLE/INDEX; default stays schemaless.

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{exec_sql, query_debug, run_mssql_sync, sync_args, temporal_schema_sql};

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
    exec_sql(
        &conn_str,
        &format!(
            "{}\n{}",
            r#"
CREATE TABLE dbo.users (
  id INT NOT NULL PRIMARY KEY,
  name NVARCHAR(64) NOT NULL
);
CREATE UNIQUE INDEX UX_users_name ON dbo.users(name);
INSERT INTO dbo.users (id, name) VALUES (1, N'alice');
"#,
            temporal_schema_sql()
        ),
    )
    .await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let full_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let mut schemaless_config = full_config.clone();
    schemaless_config.surreal_namespace = format!("test_ns_sl_{test_id}");
    schemaless_config.surreal_database = format!("test_db_sl_{test_id}");

    let full = connect_auto(&full_config).await?;
    let sl = connect_auto(&schemaless_config).await?;
    cleanup_auto(&full, &["users", "Article", "Comment"]).await?;
    cleanup_auto(&sl, &["users", "Article", "Comment"]).await?;

    let tables = vec![
        "dbo.users".into(),
        "dbo.Article".into(),
        "dbo.Comment".into(),
    ];

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
        vec![],
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
        vec![],
    ))
    .await?;

    let users_info = query_debug(&full, "INFO FOR TABLE users").await?;
    assert!(
        users_info.contains("UX_users_name") || users_info.contains("UNIQUE"),
        "ordinary unique index missing under --schemafull: {users_info}"
    );

    let article_info = query_debug(&full, "INFO FOR TABLE Article").await?;
    assert!(
        article_info.contains("is_current"),
        "temporal cookbook index missing: {article_info}"
    );
    assert!(
        !article_info.to_ascii_uppercase().contains("UNIQUE") || !article_info.contains("Id"),
        "must not copy source UNIQUE/PK onto unified temporal table: {article_info}"
    );

    let sl_users = query_debug(&sl, "INFO FOR TABLE users").await?;
    assert!(
        !sl_users.contains("UX_users_name"),
        "default run must not copy indexes: {sl_users}"
    );
    let sl_article = query_debug(&sl, "INFO FOR TABLE Article").await?;
    assert!(
        !sl_article.contains("Article_is_current"),
        "default run must not emit cookbook indexes: {sl_article}"
    );

    cleanup_checkpoint_dir(&checkpoint_full)?;
    cleanup_checkpoint_dir(&checkpoint_sl)?;
    Ok(())
}
