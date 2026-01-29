//! MongoDB load testing integration test.
//!
//! This test demonstrates the populate -> sync -> verify workflow:
//! 1. Clean MongoDB and SurrealDB of any existing test data
//! 2. Populate MongoDB with deterministic test data using a fixed seed
//! 3. Run full sync from MongoDB to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_mongodb::MongoDBPopulator;
use surreal_sync::testing::surreal::{connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use sync_core::Schema;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;
const MONGODB_URI: &str = "mongodb://root:root@mongodb:27017";
const MONGODB_DATABASE: &str = "loadtest";

/// Test the full populate -> sync -> verify workflow with MongoDB.
///
/// NOTE: Currently ignored because MongoDB sync uses ObjectId by default while
/// the verifier expects UUID-based Thing IDs. The ID format conversion needs
/// alignment between populator, sync, and verifier.
#[tokio::test]
async fn test_mongodb_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
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

    // Connect to MongoDB
    let mongo_client = mongodb::Client::with_uri_str(MONGODB_URI).await?;
    let mongo_db = mongo_client.database(MONGODB_DATABASE);

    // Connect to SurrealDB (auto-detect v2 or v3)
    let surreal_config = TestConfig::new(test_id, "loadtest-mongodb");
    let surreal_conn = connect_auto(&surreal_config).await?;

    // === CLEANUP BEFORE (ensure clean initial state) ===
    // Clean ALL schema tables (including 'products' which may be left over from other tests)
    let all_table_names: Vec<&str> = schema.table_names();
    tracing::info!("Cleaning up MongoDB collections: {:?}", all_table_names);
    for table_name in &all_table_names {
        let collection: mongodb::Collection<mongodb::bson::Document> =
            mongo_db.collection(table_name);
        collection.drop().await.ok();
    }

    tracing::info!("Cleaning up SurrealDB tables: {:?}", all_table_names);
    match &surreal_conn {
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

    // === PHASE 1: POPULATE MongoDB with deterministic test data ===
    tracing::info!(
        "Populating MongoDB with {} documents per collection (seed={})",
        ROW_COUNT,
        SEED
    );

    // Create a fresh populator for each table so sequential IDs start from the configured start value
    for table_name in &table_names {
        let mut populator =
            MongoDBPopulator::new(MONGODB_URI, MONGODB_DATABASE, schema.clone(), SEED)
                .await?
                .with_batch_size(BATCH_SIZE);
        populator.drop_collection(table_name).await.ok();
        let metrics = populator.populate(table_name, ROW_COUNT).await?;
        tracing::info!(
            "Populated {}: {} documents in {:?}",
            table_name,
            metrics.rows_inserted,
            metrics.total_duration
        );
    }

    // === PHASE 2: RUN SYNC from MongoDB to SurrealDB ===
    tracing::info!("Running full sync from MongoDB to SurrealDB");

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: MONGODB_URI.to_string(),
        source_database: Some(MONGODB_DATABASE.to_string()),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: BATCH_SIZE,
        dry_run: false,
        schema: None,
    };

    // Create version-aware sink and run sync
    match &surreal_conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
            )
            .await?;
        }
    }

    tracing::info!("Sync completed successfully");

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} documents per collection, seed={})",
        ROW_COUNT,
        SEED
    );

    match &surreal_conn {
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

                // Print first 3 mismatches for debugging
                for (i, mismatch) in report.mismatched_rows.iter().take(3).enumerate() {
                    tracing::warn!(
                        "Mismatch {}: record_id={}, field_mismatches={:?}",
                        i,
                        mismatch.record_id,
                        mismatch.field_mismatches
                    );
                }

                assert!(
                    report.is_success(),
                    "Verification failed for collection '{}': {} missing, {} mismatched",
                    table_name,
                    report.missing,
                    report.mismatched
                );
                assert_eq!(
                    report.matched, ROW_COUNT,
                    "Not all documents matched for collection '{table_name}'"
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

                // Print first 3 mismatches for debugging
                for (i, mismatch) in report.mismatched_rows.iter().take(3).enumerate() {
                    tracing::warn!(
                        "Mismatch {}: record_id={}, field_mismatches={:?}",
                        i,
                        mismatch.record_id,
                        mismatch.field_mismatches
                    );
                }

                assert!(
                    report.is_success(),
                    "Verification failed for collection '{}': {} missing, {} mismatched",
                    table_name,
                    report.missing,
                    report.mismatched
                );
                assert_eq!(
                    report.matched, ROW_COUNT,
                    "Not all documents matched for collection '{table_name}'"
                );
            }
        }
    }

    // === CLEANUP AFTER (no test artifacts remaining) ===
    tracing::info!("Cleaning up test data");
    for table_name in &table_names {
        let collection: mongodb::Collection<mongodb::bson::Document> =
            mongo_db.collection(table_name);
        collection.drop().await.ok();
    }
    match &surreal_conn {
        SurrealConnection::V2(client) => {
            surreal_sync::testing::test_helpers::cleanup_surrealdb_test_data(client, &table_names)
                .await?;
        }
        SurrealConnection::V3(client) => {
            surreal_sync::testing::surreal3::cleanup_surrealdb_test_data_v3(client, &table_names)
                .await?;
        }
    }

    tracing::info!("MongoDB loadtest completed successfully!");
    Ok(())
}
