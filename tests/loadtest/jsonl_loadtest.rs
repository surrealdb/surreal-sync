//! JSONL load testing integration test.
//!
//! This test demonstrates the generate -> sync -> verify workflow:
//! 1. Clean SurrealDB of any existing test data
//! 2. Generate JSONL files with deterministic test data using a fixed seed
//! 3. Run sync from JSONL files to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_jsonl::JsonlPopulator;
use surreal_sync::jsonl::{sync, Config, FileSource};
use surreal_sync::testing::surreal::{connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use sync_core::Schema;
use tempfile::TempDir;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;

/// Test the full generate -> sync -> verify workflow with JSONL files.
///
/// NOTE: Currently ignored because JSONL sync parses UUIDs as strings and creates
/// String IDs, while the verifier expects UUID Thing IDs. The ID format needs
/// alignment between the sync's ID parsing and the verifier's expectations.
#[tokio::test]
async fn test_jsonl_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,loadtest=info")
        .try_init()
        .ok();

    // Load schema from fixture file
    let schema = Schema::from_file("tests/fixtures/loadtest_schema.yaml")
        .expect("Failed to load test schema");

    let test_id = generate_test_id();
    let table_names: Vec<&str> = schema.table_names();

    // Create temp directory for JSONL files
    let temp_dir = TempDir::new()?;

    // Connect to SurrealDB with auto-detection
    let surreal_config = TestConfig::new(test_id, "loadtest-jsonl");
    let conn = connect_auto(&surreal_config).await?;

    // === CLEANUP BEFORE (ensure clean initial state) ===
    tracing::info!("Cleaning up SurrealDB tables: {:?}", table_names);
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

    // === PHASE 1: GENERATE JSONL files with deterministic test data ===
    tracing::info!(
        "Generating JSONL files with {} rows per table (seed={})",
        ROW_COUNT,
        SEED
    );

    let mut jsonl_files: Vec<(String, std::path::PathBuf)> = Vec::new();

    // Create a fresh populator for each table so sequential IDs start from the configured start value
    for table_name in &table_names {
        let mut populator = JsonlPopulator::new(schema.clone(), SEED);
        let output_path = temp_dir.path().join(format!("{table_name}.jsonl"));
        let metrics = populator.populate(table_name, &output_path, ROW_COUNT)?;
        tracing::info!(
            "Generated {}: {} rows in {:?} ({} bytes)",
            table_name,
            metrics.rows_written,
            metrics.total_duration,
            metrics.file_size_bytes
        );
        jsonl_files.push((table_name.to_string(), output_path));
    }

    // === PHASE 2: RUN SYNC from JSONL files to SurrealDB ===
    tracing::info!("Running sync from JSONL files to SurrealDB");

    // Sync each JSONL file (table name is derived from filename)
    for (_table_name, jsonl_path) in &jsonl_files {
        let config = Config {
            sources: vec![FileSource::Local(jsonl_path.clone())],
            files: vec![],
            s3_uris: vec![],
            http_uris: vec![],
            id_field: "id".to_string(),
            conversion_rules: vec![],
            batch_size: BATCH_SIZE,
            dry_run: false,
            schema: Some(schema.to_database_schema()), // Pass schema for type-aware conversion
        };

        match &conn {
            SurrealConnection::V2(client) => {
                let sink = surreal2_sink::Surreal2Sink::new(client.clone());
                sync(&sink, config).await?;
            }
            SurrealConnection::V3(client) => {
                let sink = surreal3_sink::Surreal3Sink::new(client.clone());
                sync(&sink, config).await?;
            }
        }
        tracing::info!("Synced {} to SurrealDB", jsonl_path.display());
    }

    tracing::info!("Sync completed successfully");

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} rows per table, seed={})",
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
    // Temp directory is automatically cleaned up when TempDir is dropped

    tracing::info!("JSONL loadtest completed successfully!");
    Ok(())
}
