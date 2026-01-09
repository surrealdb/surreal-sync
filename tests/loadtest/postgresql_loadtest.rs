//! PostgreSQL load testing integration test.
//!
//! This test demonstrates the populate -> sync -> verify workflow:
//! 1. Clean PostgreSQL and SurrealDB of any existing test data
//! 2. Populate PostgreSQL with deterministic test data using a fixed seed
//! 3. Run full sync from PostgreSQL to SurrealDB
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data

use loadtest_populate_postgresql::PostgreSQLPopulator;
use loadtest_verify::StreamingVerifier;
use surreal_sync::testing::{generate_test_id, test_helpers, TestConfig};
use surreal_sync::SurrealOpts;
use sync_core::Schema;
use tokio_postgres::NoTls;

const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;

/// Test the full populate -> sync -> verify workflow with PostgreSQL.
///
/// NOTE: Currently ignored because the loadtest schema uses TinyInt for booleans
/// which PostgreSQL doesn't support (it uses BOOLEAN). The populator needs
/// database-specific type mapping.
#[tokio::test]
async fn test_postgresql_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
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

    // Connect to PostgreSQL
    let pg_config = surreal_sync::testing::postgresql::create_postgres_config();
    let pg_conn_string = pg_config.get_connection_string();

    let (pg_client, connection) = tokio_postgres::connect(&pg_conn_string, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("PostgreSQL connection error: {}", e);
        }
    });

    // Connect to SurrealDB
    let surreal_config = TestConfig::new(test_id, "loadtest-postgresql");
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
    tracing::info!("Cleaning up PostgreSQL tables: {:?}", all_table_names);
    surreal_sync::testing::postgresql_cleanup::full_cleanup(&pg_client, &all_table_names).await?;

    tracing::info!("Cleaning up SurrealDB tables: {:?}", all_table_names);
    test_helpers::cleanup_surrealdb_test_data(&surreal, &all_table_names).await?;

    // === PHASE 1: POPULATE PostgreSQL with deterministic test data ===
    tracing::info!(
        "Populating PostgreSQL with {} rows per table (seed={})",
        ROW_COUNT,
        SEED
    );

    // Create a fresh populator for each table so sequential IDs start from the configured start value
    for table_name in &table_names {
        let mut populator = PostgreSQLPopulator::new(&pg_conn_string, schema.clone(), SEED)
            .await?
            .with_batch_size(BATCH_SIZE);
        populator.recreate_table(table_name).await?;
        let metrics = populator.populate(table_name, ROW_COUNT).await?;
        tracing::info!(
            "Populated {}: {} rows in {:?}",
            table_name,
            metrics.rows_inserted,
            metrics.total_duration
        );
    }

    // === PHASE 2: RUN SYNC from PostgreSQL to SurrealDB ===
    tracing::info!("Running full sync from PostgreSQL to SurrealDB");

    let source_opts = surreal_sync_postgresql_trigger::SourceOpts {
        source_uri: pg_conn_string.clone(),
        source_database: Some("public".to_string()),
    };

    let surreal_opts = SurrealOpts {
        surreal_endpoint: surreal_config.surreal_endpoint.clone(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: BATCH_SIZE,
        dry_run: false,
    };

    surreal_sync_postgresql_trigger::run_full_sync(
        source_opts,
        surreal_config.surreal_namespace.clone(),
        surreal_config.surreal_database.clone(),
        surreal_sync_postgresql::SurrealOpts::from(&surreal_opts),
        None,
    )
    .await?;

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
    surreal_sync::testing::postgresql_cleanup::full_cleanup(&pg_client, &table_names).await?;
    test_helpers::cleanup_surrealdb_test_data(&surreal, &table_names).await?;

    tracing::info!("PostgreSQL loadtest completed successfully!");
    Ok(())
}
