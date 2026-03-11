//! PostgreSQL all-types E2E test
//!
//! This test validates that PostgreSQL full and incremental sync operations
//! preserve all data types correctly when syncing to SurrealDB, using the
//! unified dataset.

use surreal_sync::testing::surreal::{
    assert_synced_auto, cleanup_surrealdb_auto, connect_auto, SurrealConnection,
};
use surreal_sync::testing::{
    create_unified_full_dataset, generate_test_id, SourceDatabase, TestConfig,
};

#[tokio::test]
async fn test_postgresql_full_sync_lib() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let container = surreal_sync::testing::shared_containers::shared_postgres().await;

    let dataset = create_unified_full_dataset();
    let test_id = generate_test_id();
    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgres_test_db(container, test_id)
            .await?;

    let (pg_client, pg_connection) =
        tokio_postgres::connect(&test_conn_str, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;

    // Clean up
    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;
    cleanup_surrealdb_auto(&conn, &dataset).await?;
    surreal_sync::testing::postgresql::create_tables_and_indices(&pg_client, &dataset).await?;

    surreal_sync::testing::postgresql::insert_rows(&pg_client, &dataset).await?;

    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: test_conn_str.clone(),
        source_database: Some(format!("test_{test_id}")),
        tables: vec![],
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    // Execute full sync with appropriate sink based on detected version
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
    }

    assert_synced_auto(
        &conn,
        &dataset,
        "PostgreSQL full sync only",
        SourceDatabase::PostgreSQL,
    )
    .await?;

    surreal_sync::testing::postgresql_cleanup::cleanup_unified_dataset_tables(&pg_client).await?;

    Ok(())
}
