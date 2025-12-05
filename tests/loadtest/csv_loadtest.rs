//! CSV load testing integration test.
//!
//! This test demonstrates the generate -> sync -> verify workflow:
//! 1. Clean SurrealDB of any existing test data
//! 2. Generate CSV files with deterministic test data using a fixed seed
//! 3. Run sync from CSV files to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_csv::CSVPopulator;
use loadtest_verify::StreamingVerifier;
use surreal_sync::csv::{sync, Config, FileSource};
use surreal_sync::testing::{generate_test_id, test_helpers, TestConfig};
use sync_core::SyncSchema;
use tempfile::TempDir;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;

/// Test the full generate -> sync -> verify workflow with CSV files.
///
/// NOTE: Currently ignored because CSV sync parses UUIDs as strings and creates
/// String IDs, while the verifier expects UUID Thing IDs. The ID format needs
/// alignment between the sync's ID parsing and the verifier's expectations.
#[tokio::test]
async fn test_csv_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,loadtest=info")
        .try_init()
        .ok();

    // Load schema from fixture file
    let schema = SyncSchema::from_file("tests/fixtures/loadtest_schema.yaml")
        .expect("Failed to load test schema");

    let test_id = generate_test_id();
    let table_names: Vec<&str> = schema.table_names();

    // Create temp directory for CSV files
    let temp_dir = TempDir::new()?;

    // Connect to SurrealDB
    let surreal_config = TestConfig::new(test_id, "loadtest-csv");
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
    tracing::info!("Cleaning up SurrealDB tables: {:?}", table_names);
    test_helpers::cleanup_surrealdb_test_data(&surreal, &table_names).await?;

    // === PHASE 1: GENERATE CSV files with deterministic test data ===
    tracing::info!(
        "Generating CSV files with {} rows per table (seed={})",
        ROW_COUNT,
        SEED
    );

    let mut csv_files: Vec<(String, std::path::PathBuf)> = Vec::new();

    // Create a fresh populator for each table so sequential IDs start from the configured start value
    for table_name in &table_names {
        let mut populator = CSVPopulator::new(schema.clone(), SEED);
        let output_path = temp_dir.path().join(format!("{table_name}.csv"));
        let metrics = populator.populate(table_name, &output_path, ROW_COUNT)?;
        tracing::info!(
            "Generated {}: {} rows in {:?} ({} bytes)",
            table_name,
            metrics.rows_written,
            metrics.total_duration,
            metrics.file_size_bytes
        );
        csv_files.push((table_name.to_string(), output_path));
    }

    // === PHASE 2: RUN SYNC from CSV files to SurrealDB ===
    tracing::info!("Running sync from CSV files to SurrealDB");

    let surreal_opts = surreal_sync::csv::surreal::SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
    };

    // Sync each CSV file to its corresponding table
    for (table_name, csv_path) in &csv_files {
        let config = Config {
            sources: vec![FileSource::Local(csv_path.clone())],
            files: vec![],
            s3_uris: vec![],
            http_uris: vec![],
            table: table_name.clone(),
            batch_size: BATCH_SIZE,
            namespace: surreal_config.surreal_namespace.clone(),
            database: surreal_config.surreal_database.clone(),
            surreal_opts: surreal_opts.clone(),
            has_headers: true,
            delimiter: b',',
            id_field: Some("id".to_string()),
            column_names: None,
            emit_metrics: None,
            dry_run: false,
        };

        sync(config).await?;
        tracing::info!("Synced {} to SurrealDB", table_name);
    }

    tracing::info!("Sync completed successfully");

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} rows per table, seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        let mut verifier =
            StreamingVerifier::new(surreal.clone(), schema.clone(), SEED, table_name)?;

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

    // === CLEANUP AFTER (no test artifacts remaining) ===
    tracing::info!("Cleaning up test data");
    test_helpers::cleanup_surrealdb_test_data(&surreal, &table_names).await?;
    // Temp directory is automatically cleaned up when TempDir is dropped

    tracing::info!("CSV loadtest completed successfully!");
    Ok(())
}

/// Debug test to understand what's happening with field extraction
#[tokio::test]
async fn test_csv_debug_field_extraction() -> Result<(), Box<dyn std::error::Error>> {
    use loadtest_generator::DataGenerator;

    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,loadtest=info")
        .try_init()
        .ok();

    // Load schema from fixture file
    let schema = SyncSchema::from_file("tests/fixtures/loadtest_schema.yaml")
        .expect("Failed to load test schema");

    // Test 1: Check what the generator produces
    let mut generator = DataGenerator::new(schema.clone(), SEED);
    let row = generator.next_internal_row("users")?;
    println!("\n=== GENERATOR OUTPUT ===");
    println!("ID: {:?}", row.id);
    for (name, value) in &row.fields {
        println!("  {name}: {value:?}");
    }

    // Test 2: Check what's in the CSV
    let temp_dir = TempDir::new()?;
    let mut populator = CSVPopulator::new(schema.clone(), SEED);
    let csv_path = temp_dir.path().join("users.csv");
    populator.populate("users", &csv_path, 3)?;

    let csv_content = std::fs::read_to_string(&csv_path)?;
    println!("\n=== CSV CONTENT ===");
    println!("{csv_content}");

    // Verify CSV content has the expected age values (not empty)
    assert!(csv_content.contains(",51,"), "CSV should contain age 51");
    assert!(csv_content.contains(",43,"), "CSV should contain age 43");
    assert!(csv_content.contains(",64,"), "CSV should contain age 64");

    // Test 3: Sync to SurrealDB and query directly
    let test_id = generate_test_id();
    let surreal_config = TestConfig::new(test_id, "debug-csv");
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

    test_helpers::cleanup_surrealdb_test_data(&surreal, &["users"]).await?;

    // Create a new CSV populator with the same seed (generator state was consumed)
    let mut populator2 = CSVPopulator::new(schema.clone(), SEED);
    let csv_path2 = temp_dir.path().join("users2.csv");
    populator2.populate("users", &csv_path2, 3)?;

    let surreal_opts = surreal_sync::csv::surreal::SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
    };

    let config = Config {
        sources: vec![FileSource::Local(csv_path2.clone())],
        files: vec![],
        s3_uris: vec![],
        http_uris: vec![],
        table: "users".to_string(),
        batch_size: BATCH_SIZE,
        namespace: surreal_config.surreal_namespace.clone(),
        database: surreal_config.surreal_database.clone(),
        surreal_opts: surreal_opts.clone(),
        has_headers: true,
        delimiter: b',',
        id_field: Some("id".to_string()),
        column_names: None,
        emit_metrics: None,
        dry_run: false,
    };

    sync(config).await?;

    // Query the record directly using proper type extraction
    println!("\n=== SURREALDB FIELD EXTRACTION ===");
    let thing = surrealdb::sql::Thing::from(("users", surrealdb::sql::Id::Number(1)));
    let mut response = surreal
        .query("SELECT * FROM $record_id")
        .bind(("record_id", thing))
        .await?;

    // Extract specific fields with proper types
    let age: Option<i64> = response.take((0, "age"))?;
    println!("age as i64: {age:?}");
    assert_eq!(age, Some(51), "Age should be 51 for user 1");

    let mut response2 = surreal
        .query("SELECT * FROM $record_id")
        .bind((
            "record_id",
            surrealdb::sql::Thing::from(("users", surrealdb::sql::Id::Number(1))),
        ))
        .await?;
    let email: Option<String> = response2.take((0, "email"))?;
    println!("email: {email:?}");
    assert_eq!(email, Some("user_0@test.com".to_string()));

    // Cleanup
    test_helpers::cleanup_surrealdb_test_data(&surreal, &["users"]).await?;

    Ok(())
}
