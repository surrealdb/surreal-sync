//! MySQL load testing integration test.
//!
//! This test demonstrates the populate -> sync -> verify workflow:
//! 1. Clean MySQL and SurrealDB of any existing test data
//! 2. Populate MySQL with deterministic test data using a fixed seed
//! 3. Run full sync from MySQL to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_mysql::MySQLPopulator;
use loadtest_verify::StreamingVerifier;
use surreal_sync::testing::{generate_test_id, test_helpers, TestConfig};
use surreal_sync::{mysql, SourceOpts, SurrealOpts};
use sync_core::SyncSchema;

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
    let schema = SyncSchema::from_file("tests/fixtures/loadtest_schema.yaml")
        .expect("Failed to load test schema");

    let test_id = generate_test_id();
    // All tables including 'products' with complex types (UUID, JSON, Array, Enum)
    let table_names: Vec<&str> = schema.table_names();

    // Connect to MySQL
    let mysql_config = surreal_sync::testing::mysql::create_mysql_config();
    let mysql_conn_string = mysql_config.get_connection_string();
    let pool = mysql_async::Pool::from_url(&mysql_conn_string)?;
    let mut mysql_conn = pool.get_conn().await?;

    // Connect to SurrealDB
    let surreal_config = TestConfig::new(test_id, "loadtest-mysql");
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
    tracing::info!("Cleaning up MySQL tables: {:?}", all_table_names);
    surreal_sync::testing::mysql_cleanup::full_cleanup(&mut mysql_conn, &all_table_names).await?;

    tracing::info!("Cleaning up SurrealDB tables: {:?}", all_table_names);
    test_helpers::cleanup_surrealdb_test_data(&surreal, &all_table_names).await?;

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

    let source_opts = SourceOpts {
        source_uri: mysql_conn_string.clone(),
        source_database: Some("testdb".to_string()),
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

    let surreal_for_sync = surreal_sync::surreal::surreal_connect(
        &surreal_opts,
        &surreal_config.surreal_namespace,
        &surreal_config.surreal_database,
    )
    .await?;

    mysql::run_full_sync(&source_opts, &surreal_opts, None, &surreal_for_sync).await?;

    tracing::info!("Sync completed successfully");

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} rows per table, seed={})",
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
    surreal_sync::testing::mysql_cleanup::full_cleanup(&mut mysql_conn, &table_names).await?;
    test_helpers::cleanup_surrealdb_test_data(&surreal, &table_names).await?;

    drop(mysql_conn);
    pool.disconnect().await?;

    tracing::info!("MySQL loadtest completed successfully!");
    Ok(())
}
