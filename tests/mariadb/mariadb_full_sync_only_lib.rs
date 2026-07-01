//! MariaDB full sync E2E test (thin wrapper).
//!
//! Reuses the shared body in `surreal_sync::testing::mysql_e2e`; the only
//! difference from the MySQL variant is the `mariadb:11` container. This
//! exercises MariaDB's `LONGTEXT`-backed JSON handling on the unified dataset's
//! nested `metadata` column.

use surreal_sync::testing::generate_test_id;
use surreal_sync::testing::mysql_e2e::run_full_sync_e2e;
use surreal_sync::testing::shared_containers::{
    create_mariadb_test_db, shared_mariadb, shared_surrealdb,
};

#[tokio::test]
async fn test_mariadb_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    let container = shared_mariadb().await;
    let test_id = generate_test_id();
    let test_conn_str = create_mariadb_test_db(container, test_id).await?;
    let surrealdb = shared_surrealdb();
    run_full_sync_e2e(&test_conn_str, test_id, &surrealdb.ws_endpoint()).await
}
