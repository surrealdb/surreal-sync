//! CLI e2e: `from mssql sync --snapshot-mode only`.

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto};
use surreal_sync::testing::{generate_test_id, TestConfig};

use crate::common::{exec_sql, ordinary_schema_sql, query_debug};

#[tokio::test]
async fn test_mssql_sync_snapshot_only_cli() -> Result<(), Box<dyn std::error::Error>> {
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

    let args = [
        "from",
        "mssql",
        "sync",
        "--snapshot-mode",
        "only",
        "--connection-string",
        &conn_str,
        "--tables",
        "dbo.users,dbo.posts,dbo.authored",
        "--relation-tables",
        "authored",
        "--surreal-endpoint",
        &config.surreal_endpoint,
        "--to-namespace",
        &config.surreal_namespace,
        "--to-database",
        &config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--chunk-size",
        "32",
    ];
    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "from mssql sync snapshot-only");

    let users = query_debug(&surreal, "SELECT * FROM users").await?;
    assert!(users.contains("alice"), "{users}");
    let posts = query_debug(&surreal, "SELECT * FROM posts").await?;
    assert!(posts.contains("hello"), "{posts}");
    Ok(())
}
