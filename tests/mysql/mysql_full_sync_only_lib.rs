//! MySQL all-types E2E test
//!
//! This test validates that MySQL full sync operations preserve all data types
//! correctly when syncing to SurrealDB, using the unified dataset.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};

#[tokio::test]
async fn test_mysql_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test dataset
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup MySQL connection and data
    let mysql_config = surreal_sync::testing::mysql::create_mysql_config();
    let pool = mysql_async::Pool::from_url(mysql_config.get_connection_string())?;
    let mut mysql_conn = pool.get_conn().await?;

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::new(test_id, "neo4j-test3");
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;
    surreal_sync::testing::mysql::create_tables_and_indices(&mut mysql_conn, &dataset).await?;
    surreal_sync::testing::mysql::insert_rows(&mut mysql_conn, &dataset).await?;

    // Perform full sync from MySQL to SurrealDB
    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: mysql_config.get_connection_string(),
        source_database: Some("testdb".to_string()),
        mysql_boolean_paths: Some(vec![
            "all_types_users.metadata=settings.notifications".to_string()
        ]),
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Execute full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mysql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(&conn, &dataset, "MySQL full sync only").await?;

    surreal_sync::testing::mysql::cleanup_mysql_test_data(&mut mysql_conn).await?;

    Ok(())
}
