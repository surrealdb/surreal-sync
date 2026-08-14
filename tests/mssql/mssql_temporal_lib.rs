//! Temporal UNION ALL snapshot + live UPDATE versions (library).

use surreal_sync::testing::checkpoint::cleanup_checkpoint_dir;
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncStrategy};

use crate::common::{exec_sql, query_debug, run_mssql_sync, sync_args, temporal_schema_sql};

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
    exec_sql(&conn_str, temporal_schema_sql()).await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let surreal = connect_auto(&config).await?;
    cleanup_auto(&surreal, &["Article", "Comment"]).await?;

    let checkpoint_dir = format!(".test-mssql-temporal-{test_id}");
    cleanup_checkpoint_dir(&checkpoint_dir)?;

    let args = sync_args(
        conn_str.clone(),
        vec!["dbo.Article".into(), "dbo.Comment".into()],
        &config,
        SnapshotModeArg::Only,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        None,
        vec![],
    );
    run_mssql_sync(args).await?;

    let articles = query_debug(&surreal, "SELECT * FROM Article").await?;
    assert!(
        articles.contains("first"),
        "history version missing: {articles}"
    );
    assert!(
        articles.contains("second"),
        "current version missing: {articles}"
    );
    assert!(articles.contains("is_current"), "{articles}");

    let current = query_debug(&surreal, "SELECT * FROM Article WHERE is_current").await?;
    assert!(current.contains("second"), "{current}");
    assert!(!current.contains("first"), "{current}");

    let comments = query_debug(&surreal, "SELECT * FROM Comment").await?;
    assert!(comments.contains("note"), "{comments}");
    assert!(
        !comments.contains("Article:"),
        "FK to temporal table must stay scalar: {comments}"
    );

    exec_sql(
        &conn_str,
        "UPDATE dbo.Article SET Title = N'third' WHERE Id = 1;",
    )
    .await?;

    let tail = sync_args(
        conn_str,
        vec!["dbo.Article".into(), "dbo.Comment".into()],
        &config,
        SnapshotModeArg::Never,
        SyncStrategy::InterleavedSnapshot,
        Some(checkpoint_dir.clone()),
        false,
        Some("20s".into()),
        vec![],
    );
    run_mssql_sync(tail).await?;

    let articles = query_debug(&surreal, "SELECT * FROM Article").await?;
    assert!(
        articles.contains("third"),
        "live UPDATE version missing: {articles}"
    );
    let current = query_debug(&surreal, "SELECT * FROM Article WHERE is_current").await?;
    assert!(current.contains("third"), "{current}");
    assert!(
        !current.contains("second"),
        "prior is_current should be cleared: {current}"
    );

    cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
