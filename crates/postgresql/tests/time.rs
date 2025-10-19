//! Integration tests for TIME replication with various time formats

use anyhow::{Context, Result};
use surreal_sync_postgresql_replication::{
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
async fn test_time_replication_formats() -> Result<()> {
    init_logging();
    info!("Starting TIME replication format test");

    // Create container configuration
    let container = PostgresContainer::new("test-time", TEST_PORT);

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

    // Create a test table with id and time field
    pg_client
        .execute(
            "CREATE TABLE time_test (
                id INTEGER PRIMARY KEY,
                event_time TIME
            )",
            &[],
        )
        .await?;

    info!("Created time_test table");

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
    let slot_name = "time_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert test data with various time formats
    // PostgreSQL will normalize these to TIME internally

    // 1. ISO 8601 format
    pg_client
        .execute(
            "INSERT INTO time_test (id, event_time) VALUES ($1, TIME '04:05:06')",
            &[&1i32],
        )
        .await?;
    info!("Inserted ISO 8601 time format");

    // 2. Time with microseconds
    pg_client
        .execute(
            "INSERT INTO time_test (id, event_time) VALUES ($1, TIME '04:05:06.789012')",
            &[&2i32],
        )
        .await?;
    info!("Inserted time with microseconds");

    // 3. Time without seconds
    pg_client
        .execute(
            "INSERT INTO time_test (id, event_time) VALUES ($1, TIME '04:05')",
            &[&3i32],
        )
        .await?;
    info!("Inserted time without seconds");

    // 4. Midnight
    pg_client
        .execute(
            "INSERT INTO time_test (id, event_time) VALUES ($1, TIME '00:00:00')",
            &[&4i32],
        )
        .await?;
    info!("Inserted midnight time");

    // 5. End of day (just before midnight)
    pg_client
        .execute(
            "INSERT INTO time_test (id, event_time) VALUES ($1, TIME '23:59:59.999999')",
            &[&5i32],
        )
        .await?;
    info!("Inserted end of day time");

    // Wait for replication
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek at changes
    let (changes, _nextlsn) = slot.peek().await?;
    info!("Received {} changes from replication", changes.len());

    // We should have 5 INSERT actions
    assert_eq!(changes.len(), 5, "Should have 5 INSERT changes");

    // Verify each insert
    for (idx, change) in changes.iter().enumerate() {
        match change {
            Action::Insert(row) => {
                info!("Verifying change {} for id={}", idx + 1, idx + 1);

                assert_eq!(row.table, "time_test");
                assert_eq!(row.schema, "public");

                // Extract the event_time field
                let event_time = row
                    .columns
                    .get("event_time")
                    .context("Should have event_time column")?;

                // Verify it's a Time value
                match event_time {
                    Value::Time(time_val) => {
                        info!("Raw time value: {}", time_val.0);

                        // Convert to DateTime<Utc> (uses epoch date 1970-01-01)
                        let dt = time_val.to_chrono_datetime_utc().map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to convert time to DateTime<Utc>: {} - {}",
                                time_val.0,
                                e
                            )
                        })?;

                        info!(
                            "Successfully converted to DateTime<Utc>: {}",
                            dt.to_rfc3339()
                        );

                        // Verify specific values based on ID
                        let id = match row.primary_key {
                            Value::Integer(i) => i,
                            _ => panic!("Expected integer primary key"),
                        };

                        match id {
                            1 => {
                                // 04:05:06 on epoch date
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T04:05:06+00:00");
                            }
                            2 => {
                                // 04:05:06.789012 on epoch date
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T04:05:06.789012+00:00");
                            }
                            3 => {
                                // 04:05:00 (seconds default to 0) on epoch date
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T04:05:00+00:00");
                            }
                            4 => {
                                // 00:00:00 (midnight) on epoch date
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T00:00:00+00:00");
                            }
                            5 => {
                                // 23:59:59.999999 on epoch date
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T23:59:59.999999+00:00");
                            }
                            _ => panic!("Unexpected id: {id}"),
                        }
                    }
                    _ => panic!("Expected Time value, got {event_time:?}"),
                }
            }
            _ => panic!("Expected Insert action, got {change}"),
        }
    }

    info!("All INSERT changes verified successfully");

    // Now test UPDATE to ensure it also works
    pg_client
        .execute(
            "UPDATE time_test SET event_time = TIME '12:30:45' WHERE id = $1",
            &[&1i32],
        )
        .await?;

    info!("Updated id=1 with new time");

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
                Value::Time(time_val) => {
                    let dt = time_val.to_chrono_datetime_utc().map_err(|e| {
                        anyhow::anyhow!("Failed to convert updated time: {} - {}", time_val.0, e)
                    })?;

                    info!("UPDATE time successfully converted: {}", dt.to_rfc3339());

                    // Verify the updated value (12:30:45 on epoch date)
                    assert_eq!(dt.to_rfc3339(), "1970-01-01T12:30:45+00:00");
                }
                _ => panic!("Expected Time value in UPDATE"),
            }
        }
        _ => panic!("Expected Update action"),
    }

    info!("UPDATE change verified successfully");

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("TIME replication format test completed successfully");
    Ok(())
}
