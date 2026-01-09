//! MongoDB load testing integration test.
//!
//! This test demonstrates the populate -> sync -> verify workflow:
//! 1. Clean MongoDB and SurrealDB of any existing test data
//! 2. Populate MongoDB with deterministic test data using a fixed seed
//! 3. Run full sync from MongoDB to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_mongodb::MongoDBPopulator;
use loadtest_verify::StreamingVerifier;
use surreal_sync::testing::{generate_test_id, test_helpers, TestConfig};
use surreal_sync::{SourceOpts, SurrealOpts};
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

    // Connect to SurrealDB
    let surreal_config = TestConfig::new(test_id, "loadtest-mongodb");
    let surreal = surrealdb::engine::any::connect(&surreal_config.surreal_endpoint).await?;
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    surreal
        .use_ns(&surreal_config.surreal_namespace)
        .use_db(&surreal_config.surreal_database)
        .await?;

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
    test_helpers::cleanup_surrealdb_test_data(&surreal, &all_table_names).await?;

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

    let source_opts = SourceOpts {
        source_uri: MONGODB_URI.to_string(),
        source_database: Some(MONGODB_DATABASE.to_string()),
        source_username: None,
        source_password: None,
        neo4j_timezone: "UTC".to_string(),
        neo4j_json_properties: None,
        mysql_boolean_paths: None,
    };

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: BATCH_SIZE,
        dry_run: false,
    };

    surreal_sync_mongodb::run_full_sync(
        surreal_sync_mongodb::SourceOpts::from(&source_opts),
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_sync_mongodb::SurrealOpts::from(&surreal_opts),
        None,
    )
    .await?;

    tracing::info!("Sync completed successfully");

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} documents per collection, seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        let mut verifier =
            StreamingVerifier::new(surreal.clone(), schema.clone(), SEED, table_name)?
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

    // === CLEANUP AFTER (no test artifacts remaining) ===
    tracing::info!("Cleaning up test data");
    for table_name in &table_names {
        let collection: mongodb::Collection<mongodb::bson::Document> =
            mongo_db.collection(table_name);
        collection.drop().await.ok();
    }
    test_helpers::cleanup_surrealdb_test_data(&surreal, &table_names).await?;

    tracing::info!("MongoDB loadtest completed successfully!");
    Ok(())
}
