//! MySQL load testing integration test.
//!
//! This test demonstrates the populate -> sync -> verify workflow:
//! 1. Clean MySQL and SurrealDB of any existing test data
//! 2. Populate MySQL with deterministic test data using a fixed seed
//! 3. Run full sync from MySQL to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_mysql::MySQLPopulator;
use surreal_sync::testing::{
    generate_test_id,
    surreal::{connect_auto, SurrealConnection},
    TestConfig,
};
use sync_core::Schema;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;

/// Test the full populate -> sync -> verify workflow with MySQL.
///
/// NOTE: Currently ignored because the verifier expects specific ID formats that
/// differ from how MySQL sync stores records in SurrealDB. The sync uses UUID
/// strings while the verifier constructs Thing IDs differently.
#[tokio::test]
async fn test_mysql_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,loadtest=info")
        .try_init()
        .ok();

    // Load schema from fixture file
    let schema = Schema::from_file("tests/fixtures/loadtest_schema.yaml")
        .expect("Failed to load test schema");

    let test_id = generate_test_id();
    // All tables including 'products' with complex types (UUID, JSON, Array, Enum)
    let table_names: Vec<&str> = schema.table_names();

    // Connect to MySQL
    let mysql_config = surreal_sync::testing::mysql::create_mysql_config();
    let mysql_conn_string = mysql_config.get_connection_string();
    let pool = mysql_async::Pool::from_url(&mysql_conn_string)?;
    let mut mysql_conn = pool.get_conn().await?;

    // Connect to SurrealDB (auto-detect v2 or v3)
    let surreal_config = TestConfig::new(test_id, "loadtest-mysql");
    let surreal = connect_auto(&surreal_config).await?;

    // === CLEANUP BEFORE (ensure clean initial state) ===
    // Clean ALL schema tables (including 'products' which may be left over from other tests)
    let all_table_names: Vec<&str> = schema.table_names();
    tracing::info!("Cleaning up MySQL tables: {:?}", all_table_names);
    surreal_sync::testing::mysql_cleanup::full_cleanup(&mut mysql_conn, &all_table_names).await?;

    tracing::info!("Cleaning up SurrealDB tables: {:?}", all_table_names);
    match &surreal {
        SurrealConnection::V2(client) => {
            surreal_sync::testing::test_helpers::cleanup_surrealdb_test_data(
                client,
                &all_table_names,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            surreal_sync::testing::surreal3::cleanup_surrealdb_test_data_v3(
                client,
                &all_table_names,
            )
            .await?;
        }
    }

    // === PHASE 1: POPULATE MySQL with deterministic test data ===
    tracing::info!(
        "Populating MySQL with {} rows per table (seed={})",
        ROW_COUNT,
        SEED
    );

    // Create a fresh populator for each table so sequential IDs start from the configured start value
    for table_name in &table_names {
        let mut populator = MySQLPopulator::new(&mysql_conn_string, schema.clone(), SEED)
            .await?
            .with_batch_size(BATCH_SIZE);
        populator.create_table(table_name).await?;
        let metrics = populator.populate(table_name, ROW_COUNT).await?;
        tracing::info!(
            "Populated {}: {} rows in {:?}",
            table_name,
            metrics.rows_inserted,
            metrics.total_duration
        );
    }

    // === PHASE 2: RUN SYNC from MySQL to SurrealDB ===
    tracing::info!("Running full sync from MySQL to SurrealDB");

    let source_opts = surreal_sync_mysql_trigger_source::SourceOpts {
        source_uri: mysql_conn_string.clone(),
        source_database: Some("testdb".to_string()),
        tables: vec![],
        mysql_boolean_paths: None,
    };

    let sync_opts = surreal_sync_mysql_trigger_source::SyncOpts {
        batch_size: BATCH_SIZE,
        dry_run: false,
    };

    // Create version-aware sink and run sync
    match &surreal {
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

    tracing::info!("Sync completed successfully");

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} rows per table, seed={})",
        ROW_COUNT,
        SEED
    );

    match &surreal {
        SurrealConnection::V2(client) => {
            for table_name in &table_names {
                let mut verifier = loadtest_verify_surreal2::StreamingVerifier::new(
                    client.clone(),
                    schema.clone(),
                    SEED,
                    table_name,
                )?
                // Skip updated_at - it uses timestamp_now generator which is non-deterministic
                .with_skip_fields(vec!["updated_at".to_string()]);

                let report = verifier.verify_streaming(ROW_COUNT).await?;

                tracing::info!(
                    "Verified {}: {} matched, {} missing, {} mismatched",
                    table_name,
                    report.matched,
                    report.missing,
                    report.mismatched
                );

                assert!(
                    report.is_success(),
                    "Verification failed for table '{}': {} missing, {} mismatched",
                    table_name,
                    report.missing,
                    report.mismatched
                );
                assert_eq!(
                    report.matched, ROW_COUNT,
                    "Not all rows matched for table '{table_name}'"
                );
            }
        }
        SurrealConnection::V3(client) => {
            for table_name in &table_names {
                let mut verifier = loadtest_verify_surreal3::StreamingVerifier3::new(
                    client.clone(),
                    schema.clone(),
                    SEED,
                    table_name,
                )?
                // Skip updated_at - it uses timestamp_now generator which is non-deterministic
                .with_skip_fields(vec!["updated_at".to_string()]);

                let report = verifier.verify_streaming(ROW_COUNT).await?;

                tracing::info!(
                    "Verified {}: {} matched, {} missing, {} mismatched",
                    table_name,
                    report.matched,
                    report.missing,
                    report.mismatched
                );

                assert!(
                    report.is_success(),
                    "Verification failed for table '{}': {} missing, {} mismatched",
                    table_name,
                    report.missing,
                    report.mismatched
                );
                assert_eq!(
                    report.matched, ROW_COUNT,
                    "Not all rows matched for table '{table_name}'"
                );
            }
        }
    }

    // === CLEANUP AFTER (no test artifacts remaining) ===
    tracing::info!("Cleaning up test data");
    surreal_sync::testing::mysql_cleanup::full_cleanup(&mut mysql_conn, &table_names).await?;
    match &surreal {
        SurrealConnection::V2(client) => {
            surreal_sync::testing::test_helpers::cleanup_surrealdb_test_data(client, &table_names)
                .await?;
        }
        SurrealConnection::V3(client) => {
            surreal_sync::testing::surreal3::cleanup_surrealdb_test_data_v3(client, &table_names)
                .await?;
        }
    }

    drop(mysql_conn);
    pool.disconnect().await?;

    tracing::info!("MySQL loadtest completed successfully!");
    Ok(())
}
