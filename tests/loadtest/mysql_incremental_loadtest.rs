//! MySQL incremental sync load testing integration test.
//!
//! This test demonstrates the incremental sync workflow:
//! 1. Clean MySQL and SurrealDB of any existing test data
//! 2. Create empty tables in MySQL
//! 3. Run full sync with checkpoint emission (sets up triggers)
//! 4. Populate MySQL with deterministic test data using a fixed seed
//! 5. Run incremental sync from checkpoint to sync populated data
//! 6. Verify synced data matches expected values using the same seed
//! 7. Clean up all test data

use loadtest_populate_mysql::MySQLPopulator;
use loadtest_verify::StreamingVerifier;
use surreal_sync::testing::{generate_test_id, test_helpers, TestConfig};
use surreal_sync::{SourceOpts, SurrealOpts};
use sync_core::SyncSchema;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;
const CHECKPOINT_DIR: &str = ".loadtest-mysql-incremental-checkpoints";

/// Test the incremental sync workflow with MySQL.
///
/// Sequence:
/// 1. Create empty tables
/// 2. Full sync (sets up triggers, emits t1/t2 checkpoints)
/// 3. Populate data (triggers capture inserts)
/// 4. Incremental sync (syncs from checkpoint)
/// 5. Verify data
#[tokio::test]
async fn test_mysql_incremental_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
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
    let surreal_config = TestConfig::new(test_id, "loadtest-mysql-incr");
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
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(CHECKPOINT_DIR)?;

    let all_table_names: Vec<&str> = schema.table_names();
    tracing::info!("Cleaning up MySQL tables: {:?}", all_table_names);
    surreal_sync::testing::mysql_cleanup::full_cleanup(&mut mysql_conn, &all_table_names).await?;

    tracing::info!("Cleaning up SurrealDB tables: {:?}", all_table_names);
    test_helpers::cleanup_surrealdb_test_data(&surreal, &all_table_names).await?;

    // === PHASE 1: CREATE EMPTY TABLES ===
    tracing::info!("Creating empty MySQL tables");

    for table_name in &table_names {
        let populator = MySQLPopulator::new(&mysql_conn_string, schema.clone(), SEED)
            .await?
            .with_batch_size(BATCH_SIZE);
        populator.create_table(table_name).await?;
        tracing::info!("Created table: {}", table_name);
    }

    // === PHASE 2: RUN FULL SYNC WITH CHECKPOINTS (sets up triggers) ===
    tracing::info!("Running full sync to set up triggers and emit checkpoints");

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

    // Create sync config for checkpoint emission
    let sync_config = surreal_sync::sync::SyncConfig {
        incremental: false, // This is full sync to set up infrastructure
        incremental_from: None,
        emit_checkpoints: true,
        checkpoint_dir: Some(CHECKPOINT_DIR.to_string()),
    };

    let surreal_for_sync = surreal_sync::surreal::surreal_connect(
        &surreal_opts,
        &surreal_config.surreal_namespace,
        &surreal_config.surreal_database,
    )
    .await?;

    surreal_sync::mysql::run_full_sync(
        &source_opts,
        &surreal_opts,
        Some(sync_config),
        &surreal_for_sync,
    )
    .await?;

    // Verify checkpoint emission (t1 and t2 checkpoints)
    surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(CHECKPOINT_DIR)?;
    tracing::info!("Full sync completed, checkpoints emitted");

    // === PHASE 3: POPULATE MySQL with deterministic test data ===
    // This data will be captured by the triggers set up in phase 2
    tracing::info!(
        "Populating MySQL with {} rows per table (seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        // Create fresh populator for each table so sequential IDs start from configured start value
        let mut populator = MySQLPopulator::new(&mysql_conn_string, schema.clone(), SEED)
            .await?
            .with_batch_size(BATCH_SIZE);

        // Use populate to insert data (tables already exist with triggers from full sync)
        let metrics = populator.populate(table_name, ROW_COUNT).await?;
        tracing::info!(
            "Populated {}: {} rows in {:?}",
            table_name,
            metrics.rows_inserted,
            metrics.total_duration
        );
    }

    // === PHASE 4: RUN INCREMENTAL SYNC ===
    tracing::info!("Running incremental sync from checkpoint");

    // Read the t1 checkpoint file generated by full sync
    let sync_checkpoint =
        surreal_sync::checkpoint::get_first_checkpoint_from_dir(CHECKPOINT_DIR).await?;

    tracing::info!(
        "Starting incremental sync from checkpoint: {:?}",
        sync_checkpoint
    );

    surreal_sync::mysql::run_incremental_sync(
        source_opts,
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_opts,
        sync_checkpoint,
        chrono::Utc::now() + chrono::Duration::hours(1), // 1 hour deadline
        None, // No target checkpoint - sync all available changes
    )
    .await?;

    tracing::info!("Incremental sync completed");

    // === PHASE 5: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} rows per table, seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        // WORKAROUND: MySQL incremental sync stores IDs as strings (row_id is VARCHAR in the
        // audit table) so we need to tell the verifier to use string IDs.
        //
        // TODO: Remove this workaround once MySQL incremental source is enhanced to:
        // 1. Preserve primary key value types in the tracking table (instead of VARCHAR)
        // 2. Support composite primary keys (not just single-column 'id')
        // 3. Read/leverage schema to convert PKs to correct SurrealDB types
        //
        // See `with_force_string_ids` documentation for more details.
        let mut verifier =
            StreamingVerifier::new(surreal.clone(), schema.clone(), SEED, table_name)?
                .with_force_string_ids(true);

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
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(CHECKPOINT_DIR)?;
    surreal_sync::testing::mysql_cleanup::full_cleanup(&mut mysql_conn, &table_names).await?;
    test_helpers::cleanup_surrealdb_test_data(&surreal, &table_names).await?;

    drop(mysql_conn);
    pool.disconnect().await?;

    tracing::info!("MySQL incremental loadtest completed successfully!");
    Ok(())
}
