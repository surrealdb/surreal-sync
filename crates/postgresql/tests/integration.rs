//! Integration tests for PostgreSQL logical replication with wal2json

use anyhow::{bail, Context, Result};
use surreal_sync_postgresql_replication::{testing::container::PostgresContainer, Action, Client};
use tokio_postgres::NoTls;
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

/// Test port that doesn't conflict with standard PostgreSQL port
const TEST_PORT: u16 = 15432;

/// Initialize logging for tests
fn init_logging() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}

/// Creates a comprehensive test table with various PostgreSQL data types
async fn create_test_table(client: &tokio_postgres::Client) -> Result<()> {
    let create_table_sql = r#"
        CREATE TABLE IF NOT EXISTS test_data (
            -- Numeric types
            id SERIAL PRIMARY KEY,
            small_int SMALLINT,
            normal_int INTEGER,
            big_int BIGINT,
            real_num REAL,
            double_num DOUBLE PRECISION,

            -- String types
            varchar_field VARCHAR(255),
            text_field TEXT,

            -- Boolean
            bool_field BOOLEAN,

            -- Binary
            bytea_field BYTEA,

            -- UUID
            uuid_field UUID,

            -- JSON types
            json_field JSON,
            jsonb_field JSONB,

            -- Arrays
            int_array INTEGER[],
            text_array TEXT[]
        )
    "#;

    client
        .execute(create_table_sql, &[])
        .await
        .context("Failed to create test table")?;

    info!("Created test table with comprehensive data types");
    Ok(())
}

/// Inserts test data with various PostgreSQL types
async fn insert_test_data(client: &tokio_postgres::Client) -> Result<()> {
    let insert_sql = r#"
        INSERT INTO test_data (
            small_int, normal_int, big_int, real_num, double_num,
            varchar_field, text_field,
            bool_field,
            bytea_field,
            uuid_field,
            json_field, jsonb_field,
            int_array, text_array
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7,
            $8,
            $9,
            $10,
            $11, $12,
            $13, $14
        )
    "#;

    // Prepare test data
    let test_uuid = Uuid::new_v4();
    let json_data = serde_json::json!({"key": "value", "number": 42});
    let jsonb_data = serde_json::json!({"nested": {"data": [1, 2, 3]}});

    client
        .execute(
            insert_sql,
            &[
                // Numeric types
                &32767i16,               // small_int
                &2147483647i32,          // normal_int
                &9223372036854775807i64, // big_int
                &3.14159f32,             // real_num
                &2.718281828459045f64,   // double_num
                // String types
                &"Test varchar", // varchar_field
                &"This is a longer text field with multiple words and sentences.", // text_field
                // Boolean
                &true, // bool_field
                // Binary
                &vec![1u8, 2, 3, 4, 5], // bytea_field
                // UUID
                &test_uuid, // uuid_field
                // JSON types
                &json_data,  // json_field
                &jsonb_data, // jsonb_field
                // Arrays
                &vec![1i32, 2, 3, 4, 5],            // int_array
                &vec!["apple", "banana", "cherry"], // text_array
            ],
        )
        .await
        .context("Failed to insert test data")?;

    info!("Inserted test data with comprehensive types");
    Ok(())
}

/// Verifies that the change contains expected data
fn verify_change(change: &Action) -> Result<()> {
    // Debug: print the actual structure
    info!("Actual change structure: {:#?}", change);

    // Verify it's an Insert action
    match change {
        Action::Insert(row) => {
            // Check schema and table
            assert_eq!(row.schema, "public", "Schema should be 'public'");
            assert_eq!(row.table, "test_data", "Table should be 'test_data'");

            // Verify we have all the columns (should match our insert)
            // +1 for id which is SERIAL
            assert!(
                row.columns.len() >= 14,
                "Should have all columns from our test table, got {}",
                row.columns.len()
            );

            info!("Change verification successful");
            Ok(())
        }
        _ => bail!("Expected Insert action, got {}", change),
    }
}

#[tokio::test]
async fn test_postgresql_replication_with_all_types() -> Result<()> {
    init_logging();
    info!("Starting PostgreSQL replication integration test");

    // Create container configuration
    let container = PostgresContainer::new("test-replication", TEST_PORT);

    // Build the Docker image
    container
        .build_image()
        .context("Failed to build Docker image")?;

    // Start the container
    container
        .start()
        .context("Failed to start PostgreSQL container")?;

    // Wait for PostgreSQL to be ready
    container
        .wait_until_ready(30)
        .await
        .context("PostgreSQL did not become ready in time")?;

    // Connect to PostgreSQL
    let (pg_client, connection) = tokio_postgres::connect(&container.connection_string, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Create the test table
    create_test_table(&pg_client).await?;

    // Create replication client (Client::new takes ownership of the client)
    // We need to create a new connection for the replication client
    let (repl_pg_client, repl_connection) =
        tokio_postgres::connect(&container.connection_string, NoTls)
            .await
            .context("Failed to connect to PostgreSQL for replication")?;

    tokio::spawn(async move {
        if let Err(e) = repl_connection.await {
            eprintln!("Replication connection error: {e}");
        }
    });

    let repl_client = Client::new(repl_pg_client, vec![]);

    // Create the replication slot
    let slot_name = "test_slot";
    repl_client
        .create_slot(slot_name)
        .await
        .context("Failed to create replication slot")?;

    // Start replication
    let slot = repl_client
        .start_replication(Some(slot_name))
        .await
        .context("Failed to start replication")?;

    // Insert test data AFTER slot is created and replication started
    insert_test_data(&pg_client).await?;

    // Give a moment for the changes to be available
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek at changes
    let (changes, nextlsn) = slot.peek().await.context("Failed to peek changes")?;

    // Debug: Print how many changes we got
    info!("Received {} changes from replication slot", changes.len());
    info!("Next LSN for advancement: {}", nextlsn);

    // Verify we got changes
    assert!(
        !changes.is_empty(),
        "Should have received at least one change"
    );
    assert!(!nextlsn.is_empty(), "Should have received a nextlsn");

    // Verify the first change
    let first_change = &changes[0];
    verify_change(first_change)?;

    // Advance to the nextlsn
    info!("Advancing slot to nextlsn: {}", nextlsn);
    slot.advance(&nextlsn)
        .await
        .context("Failed to advance slot")?;

    // Peek again to verify no changes remain
    let (changes_after, nextlsn_after) = slot
        .peek()
        .await
        .context("Failed to peek changes after advance")?;

    info!("After advance: {} changes remain", changes_after.len());

    // Should have no changes after advancing
    assert!(
        changes_after.is_empty(),
        "Should have no changes after advancing"
    );
    assert!(
        nextlsn_after.is_empty(),
        "Should have no nextlsn when no changes"
    );
    info!("Verified advance with nextlsn works correctly");

    // Clean up: drop the replication slot
    repl_client
        .drop_slot(slot_name)
        .await
        .context("Failed to drop replication slot")?;

    // Stop the container
    container.stop().context("Failed to stop container")?;

    info!("Integration test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_multiple_inserts_and_batch_advance() -> Result<()> {
    init_logging();
    info!("Starting batch processing test");

    // Create container configuration
    let container = PostgresContainer::new("test-batch", TEST_PORT + 1);

    // Build and start container
    container.build_image()?;
    container.start()?;
    container.wait_until_ready(30).await?;

    // Connect to PostgreSQL
    let (pg_client, connection) = tokio_postgres::connect(&container.connection_string, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Create simple test table
    pg_client
        .execute(
            "CREATE TABLE batch_test (id SERIAL PRIMARY KEY, value TEXT)",
            &[],
        )
        .await?;

    // Create replication (need separate connection)
    let (repl_pg_client, repl_connection) =
        tokio_postgres::connect(&container.connection_string, NoTls)
            .await
            .context("Failed to connect to PostgreSQL for replication")?;

    tokio::spawn(async move {
        if let Err(e) = repl_connection.await {
            eprintln!("Replication connection error: {e}");
        }
    });

    let repl_client = Client::new(repl_pg_client, vec![]);
    let slot_name = "batch_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert multiple rows AFTER slot is created and replication started
    for i in 1..=5 {
        pg_client
            .execute(
                "INSERT INTO batch_test (value) VALUES ($1)",
                &[&format!("value_{i}")],
            )
            .await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek should return all 5 changes
    let (changes, nextlsn) = slot.peek().await?;
    assert_eq!(changes.len(), 5, "Should have 5 changes");
    assert!(!nextlsn.is_empty(), "Should have a nextlsn for batch");

    info!(
        "Batch test: {} changes with nextlsn: {}",
        changes.len(),
        nextlsn
    );

    // Process all changes (simulated)
    for (i, change) in changes.iter().enumerate() {
        debug!("Processing change {}: {:#?}", i + 1, change);
    }

    // Advance using the nextlsn from the batch
    info!("Advancing batch to nextlsn: {nextlsn}");
    slot.advance(&nextlsn).await?;

    // Verify all changes are consumed
    let (changes_after, nextlsn_after) = slot.peek().await?;
    assert!(
        changes_after.is_empty(),
        "Should have no changes after batch advance"
    );
    assert!(
        nextlsn_after.is_empty(),
        "Should have no nextlsn when no changes"
    );
    info!("Verified batch processing with nextlsn works correctly");

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("Batch processing test completed successfully");
    Ok(())
}

#[tokio::test]
async fn test_get_current_wal_lsn() -> Result<()> {
    init_logging();
    info!("Starting get_current_wal_lsn test");

    // Create container configuration
    let container = PostgresContainer::new("test-wal-lsn", TEST_PORT + 2);

    // Build and start container
    container.build_image()?;
    container.start()?;
    container.wait_until_ready(30).await?;

    // Connect to PostgreSQL
    let (pg_client, connection) = tokio_postgres::connect(&container.connection_string, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Create the replication client
    let repl_client = Client::new(pg_client, vec![]);

    // Get the current WAL LSN before any operations
    let lsn_before = repl_client
        .get_current_wal_lsn()
        .await
        .context("Failed to get initial WAL LSN")?;
    info!("Initial WAL LSN: {}", lsn_before);

    // Verify it's in the expected format (e.g., "0/1949850")
    assert!(
        lsn_before.contains('/'),
        "LSN should be in format like '0/1949850'"
    );

    // Create a table and insert data to advance the WAL
    let (pg_client2, connection2) = tokio_postgres::connect(&container.connection_string, NoTls)
        .await
        .context("Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection2.await {
            eprintln!("Connection error: {e}");
        }
    });

    pg_client2
        .execute(
            "CREATE TABLE lsn_test (id SERIAL PRIMARY KEY, data TEXT)",
            &[],
        )
        .await?;
    pg_client2
        .execute("INSERT INTO lsn_test (data) VALUES ('test1')", &[])
        .await?;
    pg_client2
        .execute("INSERT INTO lsn_test (data) VALUES ('test2')", &[])
        .await?;

    // Get the WAL LSN after operations
    let lsn_after = repl_client
        .get_current_wal_lsn()
        .await
        .context("Failed to get WAL LSN after operations")?;
    info!("WAL LSN after operations: {}", lsn_after);

    // The LSN should have advanced
    assert_ne!(
        lsn_before, lsn_after,
        "LSN should advance after database operations"
    );

    // Parse and compare LSNs to ensure lsn_after is greater
    let parse_lsn = |lsn: &str| -> Result<(u64, u64)> {
        let parts: Vec<&str> = lsn.split('/').collect();
        if parts.len() != 2 {
            bail!("Invalid LSN format: {lsn}");
        }
        let high = u64::from_str_radix(parts[0], 16).context("Failed to parse high part of LSN")?;
        let low = u64::from_str_radix(parts[1], 16).context("Failed to parse low part of LSN")?;
        Ok((high, low))
    };

    let (before_high, before_low) = parse_lsn(&lsn_before)?;
    let (after_high, after_low) = parse_lsn(&lsn_after)?;

    assert!(
        (after_high > before_high) || (after_high == before_high && after_low > before_low),
        "LSN should increase: {lsn_before} -> {lsn_after}"
    );

    info!("Successfully verified WAL LSN advancement");

    // Cleanup
    container.stop()?;

    info!("get_current_wal_lsn test completed successfully");
    Ok(())
}
