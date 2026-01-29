//! Kafka load testing integration test.
//!
//! This test demonstrates the populate -> sync -> verify workflow for Kafka:
//! 1. Clean SurrealDB of any existing test data
//! 2. Populate Kafka topics with deterministic test data using a fixed seed
//!    - Dynamically generates .proto files from the loadtest schema
//! 3. Run streaming sync from Kafka to SurrealDB (streaming-only, no full sync)
//! 4. Verify synced data matches expected values using the same seed
//! 5. Clean up all test data
//!
//! NOTE: Kafka is streaming-only:
//! - No full sync phase required
//! - Consumer offsets are managed by Kafka internally
//! - Test uses timeout-based sync (runs until deadline)
//!
//! ## Import Note
//!
//! This test imports directly from `surreal_sync_kafka_source` crate (not re-exported
//! through main crate). This follows the pattern established for other database-
//! specific sync crates (mysql-trigger, postgresql-trigger, etc.).

use chrono::Utc;
use loadtest_populate_kafka::KafkaPopulator;
use loadtest_verify::StreamingVerifier;
use std::{sync::Arc, time::Duration};
use surreal_sync::testing::{generate_test_id, test_helpers, TestConfig};
use surreal_sync_kafka_source::Config as KafkaConfig;
use sync_core::Schema;
use tokio::time::sleep;

/// Kafka broker address for testing
const KAFKA_BROKER: &str = "kafka:9092";
const SEED: u64 = 42;
const ROW_COUNT: u64 = 50; // Small scale for integration tests
const BATCH_SIZE: usize = 10;
/// Sync timeout in seconds
const SYNC_TIMEOUT_SECS: i64 = 10;

/// Test the full populate -> sync -> verify workflow with Kafka.
///
/// This test uses the KafkaPopulator to generate protobuf-encoded messages
/// from the loadtest schema, then syncs them to SurrealDB.
#[tokio::test]
async fn test_kafka_loadtest_small_scale() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info,loadtest=info,loadtest_populate_kafka=info")
        .try_init()
        .ok();

    // Load schema from fixture file
    let schema = Schema::from_file("tests/fixtures/loadtest_schema.yaml")
        .expect("Failed to load test schema");

    let test_id = generate_test_id();
    // Get table names from schema (users, orders, products)
    let table_names: Vec<&str> = schema.table_names();

    tracing::info!(
        "Starting Kafka loadtest with {} tables, {} rows each (seed={})",
        table_names.len(),
        ROW_COUNT,
        SEED
    );

    // Setup SurrealDB connection
    let surreal_config = TestConfig::new(test_id, "kafka-loadtest");
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

    // === PHASE 1: POPULATE Kafka topics with deterministic test data ===
    // For each table, we:
    // 1. Generate a unique topic name to avoid test conflicts
    // 2. Create a fresh populator (resets generator index to 0)
    // 3. Prepare table (generates .proto file)
    // 4. Create topic and populate with messages

    // Create SurrealDB v2 sink
    let sink = Arc::new(surreal2_sink::Surreal2Sink::new(surreal.clone()));

    // Store topic info for sync phase
    // Note: We keep the populators alive because their TempDir holds the .proto files
    let mut topic_info: Vec<(String, String, std::path::PathBuf)> = Vec::new();
    let mut _populators: Vec<KafkaPopulator> = Vec::new(); // Keep alive to preserve proto files

    for table_name in &table_names {
        // Use unique topic name to avoid conflicts with other test runs
        let topic_name = format!("loadtest-{table_name}-{test_id}");

        tracing::info!(
            "Populating table '{}' -> topic '{}' (seed={}, {} rows)",
            table_name,
            topic_name,
            SEED,
            ROW_COUNT
        );

        // Create a fresh populator for each table to reset the generator index
        let mut populator = KafkaPopulator::new(KAFKA_BROKER, schema.clone(), SEED)
            .await?
            .with_batch_size(BATCH_SIZE);

        // Prepare table (generates .proto file)
        let proto_path = populator.prepare_table(table_name)?;

        // Create topic
        populator.create_topic(&topic_name).await?;

        // Wait for topic to be ready
        sleep(Duration::from_millis(200)).await;

        // Populate to the unique topic name
        let metrics = populator
            .populate_to_topic(table_name, &topic_name, ROW_COUNT)
            .await?;

        // Store the topic info for sync and verification
        topic_info.push((table_name.to_string(), topic_name.clone(), proto_path));

        tracing::info!(
            "Populated '{}': {} messages in {:?} ({:.2} msg/sec)",
            table_name,
            metrics.messages_published,
            metrics.total_duration,
            metrics.messages_per_second()
        );

        // Keep populator alive to preserve the TempDir with .proto files
        _populators.push(populator);
    }

    // Give messages time to commit
    sleep(Duration::from_millis(500)).await;

    // === PHASE 2: RUN SYNC from Kafka to SurrealDB ===
    tracing::info!("Running Kafka streaming sync to SurrealDB");

    for (table_name, topic_name, proto_path) in &topic_info {
        // Capitalize first letter for message type name (e.g., "users" -> "Users")
        let message_type = capitalize(table_name);

        tracing::info!(
            "Syncing table '{}' from topic '{}' (message type: {})",
            table_name,
            topic_name,
            message_type
        );

        let config = KafkaConfig {
            proto_path: proto_path.to_string_lossy().to_string(),
            brokers: vec![KAFKA_BROKER.to_string()],
            group_id: format!("loadtest-{table_name}-{test_id}"),
            topic: topic_name.clone(),
            message_type,
            buffer_size: 1000,
            session_timeout_ms: "6000".to_string(),
            num_consumers: 1,
            kafka_batch_size: BATCH_SIZE,
            table_name: Some(table_name.clone()),
            use_message_key_as_id: false,
            id_field: "id".to_string(),
            max_messages: None,
        };

        // Run sync with a deadline
        let deadline = Utc::now() + chrono::Duration::seconds(SYNC_TIMEOUT_SECS);

        // Get the table schema for schema-aware conversion
        // Convert from GeneratorTableDefinition to base TableDefinition
        let table_schema = schema
            .get_table(table_name)
            .map(|t| t.to_table_definition());

        let sync_handle = tokio::spawn({
            let sink_clone = sink.clone();
            async move {
                surreal_sync_kafka_source::run_incremental_sync(
                    sink_clone,
                    config,
                    deadline,
                    table_schema,
                )
                .await
            }
        });

        // Wait for sync to complete or timeout
        let sync_result = tokio::time::timeout(
            Duration::from_secs((SYNC_TIMEOUT_SECS + 5) as u64),
            sync_handle,
        )
        .await;

        match sync_result {
            Ok(Ok(Ok(()))) => tracing::info!("Sync for '{}' completed successfully", table_name),
            Ok(Ok(Err(e))) => {
                tracing::warn!("Sync for '{}' error (may be expected): {}", table_name, e)
            }
            Ok(Err(e)) => tracing::warn!("Sync for '{}' task error: {}", table_name, e),
            Err(_) => tracing::info!("Sync for '{}' timeout (expected)", table_name),
        }
    }

    // === PHASE 3: VERIFY synced data matches expected values ===
    tracing::info!(
        "Verifying synced data ({} rows per table, seed={})",
        ROW_COUNT,
        SEED
    );

    for table_name in &table_names {
        // Create verifier for Kafka.
        // Schema-aware conversion in the Kafka source now handles:
        // - JSON/Object fields: parsed from protobuf strings to native SurrealDB Objects
        // - Missing array fields: explicitly added as empty arrays based on schema
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

        // Debug: print mismatch details
        if report.mismatched > 0 {
            tracing::warn!("Mismatched rows for '{}':", table_name);
            for mismatch in &report.mismatched_rows {
                tracing::warn!(
                    "  Row {}: {} - mismatches: {:?}",
                    mismatch.index,
                    mismatch.record_id,
                    mismatch.field_mismatches
                );
            }
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

    tracing::info!("Kafka loadtest completed successfully!");
    Ok(())
}

/// Capitalize the first letter of a string.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
    }
}
