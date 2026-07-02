//! MySQL binlog incremental sync E2E test (thin wrapper).

use surreal_sync::testing::generate_test_id;
use surreal_sync::testing::mysql_binlog_e2e::{
    run_binlog_incremental_e2e, run_binlog_incremental_e2e_repl_user,
};
use surreal_sync::testing::shared_containers::{
    create_binlog_test_db, shared_mysql_binlog, shared_surrealdb,
};

#[tokio::test]
async fn test_mysql_binlog_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    let container = shared_mysql_binlog().await;
    let test_id = generate_test_id();
    let test_conn_str = create_binlog_test_db(container, test_id).await?;
    let surrealdb = shared_surrealdb();
    run_binlog_incremental_e2e(&test_conn_str, test_id, &surrealdb.ws_endpoint()).await
}

#[tokio::test]
async fn test_mysql_binlog_incremental_sync_repl_user_lib() -> Result<(), Box<dyn std::error::Error>>
{
    let container = shared_mysql_binlog().await;
    let test_id = generate_test_id();
    let _test_conn_str = create_binlog_test_db(container, test_id).await?;
    let surrealdb = shared_surrealdb();
    run_binlog_incremental_e2e_repl_user(container, test_id, &surrealdb.ws_endpoint()).await
}
