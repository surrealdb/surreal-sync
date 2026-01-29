//! MongoDB incremental sync load testing integration test.
//!
//! This test demonstrates the incremental sync workflow:
//! 1. Clean MongoDB and SurrealDB of any existing test data
//! 2. Run full sync with checkpoint emission (captures resume token)
//! 3. Populate MongoDB with deterministic test data using a fixed seed
//! 4. Run incremental sync from checkpoint to sync populated data
//! 5. Verify synced data matches expected values using the same seed
//! 6. Clean up all test data

use loadtest_populate_mongodb::MongoDBPopulator;
use surreal_sync::testing::surreal::{connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use sync_core::Schema;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;
const MONGODB_URI: &str = "mongodb://root:root@mongodb:27017";
const MONGODB_DATABASE: &str = "loadtest_incr";
const CHECKPOINT_DIR: &str = ".loadtest-mongodb-incremental-checkpoints";

/// Test the incremental sync workflow with MongoDB.
///
/// Sequence:
/// 1. Drop collections to ensure clean state
/// 2. Full sync (captures resume token, emits t1/t2 checkpoints)
/// 3. Populate data (captured by change stream from resume token)
/// 4. Incremental sync (syncs from checkpoint)
/// 5. Verify data
#[tokio::test]
async fn test_mongodb_incremental_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
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
    let surreal_config = TestConfig::new(test_id, "loadtest-mongo-incr");
    let conn = connect_auto(&surreal_config).await?;

    // === CLEANUP BEFORE (ensure clean initial state) ===
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(CHECKPOINT_DIR)?;

    let all_table_names: Vec<&str> = schema.table_names();
    tracing::info!("Cleaning up MongoDB collections: {:?}", all_table_names);
    for table_name in &all_table_names {
        let collection: mongodb::Collection<mongodb::bson::Document> =
            mongo_db.collection(table_name);
        collection.drop().await.ok();
    }

    tracing::info!("Cleaning up SurrealDB tables: {:?}", all_table_names);
    match &conn {
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

    // === PHASE 1: RUN FULL SYNC WITH CHECKPOINTS (captures resume token) ===
    // Note: For MongoDB, we run full sync first to get the resume token
    // Then populate data after that point
    tracing::info!("Running full sync to capture resume token and emit checkpoints");

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: MONGODB_URI.to_string(),
        source_database: Some(MONGODB_DATABASE.to_string()),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: BATCH_SIZE,
        dry_run: false,
    };

    // Create sync manager with filesystem checkpoint store
    let checkpoint_store = checkpoint::FilesystemStore::new(CHECKPOINT_DIR);
    let sync_manager = checkpoint::SyncManager::new(checkpoint_store);

    // Run full sync with version-aware sink
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_full_sync(
                &sink,
                source_opts.clone(),
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
    }

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(CHECKPOINT_DIR)?;
    tracing::info!("Full sync completed, checkpoints emitted");

    // === PHASE 2: POPULATE MongoDB with deterministic test data ===
    // This data will be captured by the change stream when we resume
    tracing::info!(
        "Populating MongoDB with {} documents per collection (seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        // Create fresh populator for each table so sequential IDs start from configured start value
        let mut populator =
            MongoDBPopulator::new(MONGODB_URI, MONGODB_DATABASE, schema.clone(), SEED)
                .await?
                .with_batch_size(BATCH_SIZE);

        // Use populate to insert data
        let metrics = populator.populate(table_name, ROW_COUNT).await?;
        tracing::info!(
            "Populated {}: {} documents in {:?}",
            table_name,
            metrics.rows_inserted,
            metrics.total_duration
        );
    }

    // === PHASE 3: RUN INCREMENTAL SYNC ===
    tracing::info!("Running incremental sync from checkpoint");

    // Read the t1 (FullSyncStart) checkpoint file - this is needed
    // for incremental sync to pick up changes made after full sync started
    let main_checkpoint =
        checkpoint::get_checkpoint_for_phase(CHECKPOINT_DIR, checkpoint::SyncPhase::FullSyncStart)
            .await?;
    // Convert to mongodb crate's checkpoint type
    let sync_checkpoint: surreal_sync_mongodb_changestream_source::MongoDBCheckpoint =
        main_checkpoint.parse()?;

    tracing::info!(
        "Starting incremental sync from checkpoint: {:?}",
        sync_checkpoint
    );

    // Run incremental sync with version-aware sink
    match &conn {
        SurrealConnection::V2(client) => {
            let sink = surreal2_sink::Surreal2Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1), // 1 hour deadline
                None, // No target checkpoint - sync all available changes
            )
            .await?;
        }
        SurrealConnection::V3(client) => {
            let sink = surreal3_sink::Surreal3Sink::new(client.clone());
            surreal_sync_mongodb_changestream_source::run_incremental_sync(
                &sink,
                source_opts,
                sync_checkpoint,
                chrono::Utc::now() + chrono::Duration::hours(1), // 1 hour deadline
                None, // No target checkpoint - sync all available changes
            )
            .await?;
        }
    }

    tracing::info!("Incremental sync completed");

    // === PHASE 4: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} documents per collection, seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        match &conn {
            SurrealConnection::V2(client) => {
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
            SurrealConnection::V3(client) => {
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
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(CHECKPOINT_DIR)?;
    for table_name in &table_names {
        let collection: mongodb::Collection<mongodb::bson::Document> =
            mongo_db.collection(table_name);
        collection.drop().await.ok();
    }
    match &conn {
        SurrealConnection::V2(client) => {
            surreal_sync::testing::test_helpers::cleanup_surrealdb_test_data(client, &table_names)
                .await?;
        }
        SurrealConnection::V3(client) => {
            surreal_sync::testing::surreal3::cleanup_surrealdb_test_data_v3(client, &table_names)
                .await?;
        }
    }

    tracing::info!("MongoDB incremental loadtest completed successfully!");
    Ok(())
}
