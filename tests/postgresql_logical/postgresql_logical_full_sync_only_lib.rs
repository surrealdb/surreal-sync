//! Lib Integration Tests for PostgreSQL Logical Replication Full Sync
//!
//! These tests verify that the PostgreSQL logical replication library handles
//! full sync correctly using the unified dataset.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{create_unified_full_dataset, generate_test_id, TestConfig};

/// Test PostgreSQL logical replication full sync via library
#[tokio::test]
#[ignore = "Requires wal2json PostgreSQL extension not available in devcontainer"]
async fn test_postgresql_logical_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let test_id = generate_test_id();
    let dataset = create_unified_full_dataset();

    // Setup PostgreSQL with test data
    let pg_config = surreal_sync::testing::postgresql::create_postgres_config();
    let (pg_client, pg_connection) =
        tokio_postgres::connect(&pg_config.get_connection_string(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Setup SurrealDB connection with auto-detection for validation
    let surreal_config = TestConfig::new(test_id, "postgresql-logical-lib-test1");
    let conn = connect_auto(&surreal_config).await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;
    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    // Get table names from the dataset
    let table_names: Vec<String> = dataset.tables.iter().map(|t| t.name.clone()).collect();

    // Create source options
    let source_opts = surreal_sync_postgresql_wal2json_source::SourceOpts {
        connection_string: pg_config.get_connection_string(),
        slot_name: "surreal_sync_lib_test_slot".to_string(),
        tables: table_names,
        schema: "public".to_string(),
    };

    // Create SurrealDB sync options
    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Run full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_postgresql_wal2json_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
    }

    // Verify synced data
    assert_synced_auto(&conn, &dataset, "PostgreSQL logical full sync lib").await?;

    // Cleanup: drop the replication slot
    pg_client
        .execute(
            "SELECT pg_drop_replication_slot('surreal_sync_lib_test_slot')",
            &[],
        )
        .await
        .ok(); // Ignore errors if slot doesn't exist

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
