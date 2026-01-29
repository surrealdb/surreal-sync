//! MongoDB all-types E2E test
//!
//! This test validates that MongoDB full sync operations preserve all data types
//! correctly when syncing to SurrealDB, using the unified dataset.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_mongodb_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=debug")
        .try_init()
        .ok();

    // Create test dataset
    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();

    // Setup MongoDB connection and data
    let mongodb_client = surreal_sync::testing::mongodb::connect_mongodb().await?;
    let db = mongodb_client.database("testdb");

    // Setup SurrealDB connection with auto-detection
    let surreal_config = TestConfig::new(test_id, "neo4j-test4");
    let conn = connect_auto(&surreal_config).await?;

    cleanup_surrealdb_auto(&conn, &dataset).await?;

    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;
    surreal_sync::testing::mongodb::create_collections(&db, &dataset).await?;
    surreal_sync::testing::mongodb::insert_docs(&db, &dataset).await?;

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: "mongodb://root:root@mongodb:27017".to_string(),
        source_database: Some("testdb".to_string()),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: 1000,
        dry_run: false,
        schema: None,
    };

    // Execute full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::migrate_from_mongodb::<_>(
                &sink,
                source_opts,
                sync_opts,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::migrate_from_mongodb::<_>(
                &sink,
                source_opts,
                sync_opts,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "MongoDB full sync - all data types",
        SourceDatabase::MongoDB,
    )
    .await?;

    // Clean up
    surreal_sync::testing::mongodb::cleanup_mongodb_test_data(&db).await?;

    Ok(())
}
