//! MySQL incremental sync E2E test (thin wrapper).
//!
//! The shared body lives in `surreal_sync::testing::mysql_e2e` so the MySQL and
//! MariaDB variants differ only by which container they run against.

use surreal_sync::testing::generate_test_id;
use surreal_sync::testing::mysql_e2e::run_incremental_e2e;
use surreal_sync::testing::shared_containers::{
    create_mysql_test_db, shared_mysql, shared_surrealdb,
};

#[tokio::test]
async fn test_mysql_incremental_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    let container = shared_mysql().await;
    let test_id = generate_test_id();
    let test_conn_str = create_mysql_test_db(container, test_id).await?;
    let surrealdb = shared_surrealdb();
    run_incremental_e2e(&test_conn_str, test_id, &surrealdb.ws_endpoint()).await
}
