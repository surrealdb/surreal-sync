//! MariaDB binlog full sync E2E test (thin wrapper).

use surreal_sync::testing::generate_test_id;
use surreal_sync::testing::mysql_binlog_e2e::run_binlog_full_sync_e2e;
use surreal_sync::testing::shared_containers::{
    create_binlog_test_db, shared_mariadb_binlog, shared_surrealdb,
};

#[tokio::test]
async fn test_mariadb_binlog_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    let container = shared_mariadb_binlog().await;
    let test_id = generate_test_id();
    let test_conn_str = create_binlog_test_db(container, test_id).await?;
    let surrealdb = shared_surrealdb();
    run_binlog_full_sync_e2e(&test_conn_str, test_id, &surrealdb.ws_endpoint()).await
}
