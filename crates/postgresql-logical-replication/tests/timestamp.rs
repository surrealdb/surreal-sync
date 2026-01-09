//! Integration tests for TIMESTAMP replication with various timestamp formats

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use surreal_sync_postgresql_logical_replication::{
    testing::container::PostgresContainer, Action, Client, Value,
};
use tokio_postgres::NoTls;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Test port that doesn't conflict with standard PostgreSQL port
const TEST_PORT: u16 = 15437;

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

#[tokio::test]
async fn test_timestamp_replication_formats() -> Result<()> {
    init_logging();
    info!("Starting TIMESTAMP replication format test");

    // Create container configuration
    let container = PostgresContainer::new("test-timestamp", TEST_PORT);

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

    // Create a test table with id and timestamp field (without timezone)
    pg_client
        .execute(
            "CREATE TABLE timestamp_test (
                id INTEGER PRIMARY KEY,
                event_time TIMESTAMP
            )",
            &[],
        )
        .await?;

    info!("Created timestamp_test table");

    // Create replication client
    let (repl_pg_client, repl_connection) =
        tokio_postgres::connect(&container.connection_string, NoTls)
            .await
            .context("Failed to connect for replication")?;

    tokio::spawn(async move {
        if let Err(e) = repl_connection.await {
            eprintln!("Replication connection error: {e}");
        }
    });

    let repl_client = Client::new(repl_pg_client, vec![]);
    let slot_name = "timestamp_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert test data with various timestamp formats
    // Note: TIMESTAMP (without timezone) stores timestamps without timezone information
    // PostgreSQL will interpret them in the server's timezone context

    // 1. ISO 8601 format
    pg_client
        .execute(
            "INSERT INTO timestamp_test (id, event_time) VALUES ($1, TIMESTAMP '2024-01-15T10:30:00')",
            &[&1i32],
        )
        .await?;
    info!("Inserted ISO 8601 timestamp");

    // 2. ISO 8601 with microseconds
    pg_client
        .execute(
            "INSERT INTO timestamp_test (id, event_time) VALUES ($1, TIMESTAMP '1997-12-17T15:37:16.123456')",
            &[&2i32],
        )
        .await?;
    info!("Inserted ISO 8601 with microseconds timestamp");

    // 3. Traditional SQL format
    pg_client
        .execute(
            "INSERT INTO timestamp_test (id, event_time) VALUES ($1, TIMESTAMP '12/17/1997 07:37:16.00')",
            &[&3i32],
        )
        .await?;
    info!("Inserted traditional SQL format timestamp");

    // 4. PostgreSQL date + time format
    pg_client
        .execute(
            "INSERT INTO timestamp_test (id, event_time) VALUES ($1, TIMESTAMP '1997-12-17 15:37:16')",
            &[&4i32],
        )
        .await?;
    info!("Inserted PostgreSQL date+time format timestamp");

    // 5. Traditional PostgreSQL format
    pg_client
        .execute(
            "INSERT INTO timestamp_test (id, event_time) VALUES ($1, TIMESTAMP 'Wed Dec 17 07:37:16 1997')",
            &[&5i32],
        )
        .await?;
    info!("Inserted traditional PostgreSQL format timestamp");

    // Wait for replication
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek at changes
    let (changes, _nextlsn) = slot.peek().await?;
    info!("Received {} changes from replication", changes.len());

    // We should have 5 INSERT actions
    assert_eq!(changes.len(), 5, "Should have 5 INSERT changes");

    // Verify each insert can be converted to DateTime<Utc>
    for (idx, change) in changes.iter().enumerate() {
        match change {
            Action::Insert(row) => {
                info!("Verifying change {} for id={}", idx + 1, idx + 1);

                assert_eq!(row.table, "timestamp_test");
                assert_eq!(row.schema, "public");

                // Extract the event_time field
                let event_time = row
                    .columns
                    .get("event_time")
                    .context("Should have event_time column")?;

                // Verify it's a Timestamp value
                match event_time {
                    Value::Timestamp(ts) => {
                        info!("Raw timestamp value: {}", ts.0);

                        // Convert to DateTime<Utc>
                        let dt = ts.to_chrono_datetime_utc().map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to convert timestamp to DateTime<Utc>: {} - {}",
                                ts.0,
                                e
                            )
                        })?;

                        info!(
                            "Successfully converted to DateTime<Utc>: {}",
                            dt.to_rfc3339()
                        );

                        // Verify the timestamp makes sense (not in the future, not before 1990)
                        let now = Utc::now();
                        assert!(
                            dt < now,
                            "Timestamp should be in the past: {}",
                            dt.to_rfc3339()
                        );

                        let min_time =
                            DateTime::parse_from_rfc3339("1990-01-01T00:00:00Z").unwrap();
                        assert!(
                            dt > min_time.with_timezone(&Utc),
                            "Timestamp should be after 1990: {}",
                            dt.to_rfc3339()
                        );

                        // For known timestamps (IDs 1-5), verify they were parsed successfully
                        // Note: TIMESTAMP without timezone doesn't preserve timezone info,
                        // so we just verify that parsing succeeded
                        let id = match row.primary_key {
                            Value::Integer(i) => i,
                            _ => panic!("Expected integer primary key"),
                        };

                        info!(
                            "Successfully verified timestamp for id={}: {}",
                            id,
                            dt.to_rfc3339()
                        );
                    }
                    _ => panic!("Expected Timestamp value, got {event_time:?}"),
                }
            }
            _ => panic!("Expected Insert action, got {change}"),
        }
    }

    info!("All INSERT changes verified successfully");

    // Now test UPDATE to ensure it also works
    pg_client
        .execute(
            "UPDATE timestamp_test SET event_time = TIMESTAMP '2025-06-01 12:00:00' WHERE id = $1",
            &[&1i32],
        )
        .await?;

    info!("Updated id=1 with new timestamp");

    // Wait and peek again
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let (update_changes, _) = slot.peek().await?;

    // Find the UPDATE action
    let update_action = update_changes
        .iter()
        .find(|c| matches!(c, Action::Update(_)))
        .context("Should find UPDATE action")?;

    match update_action {
        Action::Update(row) => {
            info!("Verifying UPDATE action");

            let event_time = row
                .columns
                .get("event_time")
                .context("Should have event_time column in UPDATE")?;

            match event_time {
                Value::Timestamp(ts) => {
                    let dt = ts.to_chrono_datetime_utc().map_err(|e| {
                        anyhow::anyhow!("Failed to convert updated timestamp: {} - {}", ts.0, e)
                    })?;

                    info!(
                        "UPDATE timestamp successfully converted: {}",
                        dt.to_rfc3339()
                    );

                    // Verify it's a valid timestamp
                    assert!(dt > Utc::now() - chrono::Duration::days(365 * 2));
                }
                _ => panic!("Expected Timestamp value in UPDATE"),
            }
        }
        _ => panic!("Expected Update action"),
    }

    info!("UPDATE change verified successfully");

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("TIMESTAMP replication format test completed successfully");
    Ok(())
}
