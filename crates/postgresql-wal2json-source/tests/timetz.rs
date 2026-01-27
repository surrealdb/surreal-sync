//! Integration tests for TIMETZ replication with various time with timezone formats
#![allow(clippy::uninlined_format_args)]

use anyhow::{Context, Result};
use surreal_sync_postgresql_wal2json_source::{
    testing::container::PostgresContainer, Action, Client,
};
use sync_core::UniversalValue;
use tokio_postgres::NoTls;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Test port that doesn't conflict with standard PostgreSQL port
const TEST_PORT: u16 = 15438;

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
async fn test_timetz_replication_formats() -> Result<()> {
    init_logging();
    info!("Starting TIMETZ replication format test");

    // Create container configuration
    let container = PostgresContainer::new("test-timetz", TEST_PORT);

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

    // Create a test table with id and timetz field
    pg_client
        .execute(
            "CREATE TABLE timetz_test (
                id INTEGER PRIMARY KEY,
                event_time TIMETZ
            )",
            &[],
        )
        .await?;

    info!("Created timetz_test table");

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
    let slot_name = "timetz_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert test data with various timetz formats
    // PostgreSQL will normalize these to TIMETZ internally

    // 1. Time with UTC offset +00:00
    pg_client
        .execute(
            "INSERT INTO timetz_test (id, event_time) VALUES ($1, TIMETZ '04:05:06+00:00')",
            &[&1i32],
        )
        .await?;
    info!("Inserted time with UTC offset");

    // 2. Time with positive offset (e.g., +05:30 for IST)
    pg_client
        .execute(
            "INSERT INTO timetz_test (id, event_time) VALUES ($1, TIMETZ '04:05:06+05:30')",
            &[&2i32],
        )
        .await?;
    info!("Inserted time with positive offset");

    // 3. Time with negative offset (e.g., -08:00 for PST)
    pg_client
        .execute(
            "INSERT INTO timetz_test (id, event_time) VALUES ($1, TIMETZ '15:37:16-08:00')",
            &[&3i32],
        )
        .await?;
    info!("Inserted time with negative offset");

    // 4. Time with Z timezone (equivalent to +00:00)
    pg_client
        .execute(
            "INSERT INTO timetz_test (id, event_time) VALUES ($1, TIMETZ '12:00:00Z')",
            &[&4i32],
        )
        .await?;
    info!("Inserted time with Z timezone");

    // 5. Time with microseconds and timezone
    pg_client
        .execute(
            "INSERT INTO timetz_test (id, event_time) VALUES ($1, TIMETZ '23:59:59.999999+00:00')",
            &[&5i32],
        )
        .await?;
    info!("Inserted time with microseconds and timezone");

    // Wait for replication
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek at changes
    let (changes, _nextlsn) = slot.peek().await?;
    info!("Received {} changes from replication", changes.len());

    // We should have 5 INSERT actions
    assert_eq!(changes.len(), 5, "Should have 5 INSERT changes");

    // Verify each insert returns a ZonedDateTime value (timetz values are converted to DateTime)
    for (idx, change) in changes.iter().enumerate() {
        match change {
            Action::Insert(row) => {
                info!("Verifying change {} for id={}", idx + 1, idx + 1);

                assert_eq!(row.table, "timetz_test");
                assert_eq!(row.schema, "public");

                // Extract the event_time field
                let event_time = row
                    .columns
                    .get("event_time")
                    .context("Should have event_time column")?;

                // Verify it's a TimeTz value (string) - preserving original format
                // Note: TIMETZ must be stored as String, NOT DateTime.
                // Time and datetime are fundamentally different types - datetime implies a specific
                // point in time, while TIMETZ represents a daily recurring time in a specific timezone.
                match event_time {
                    UniversalValue::TimeTz(time_str) => {
                        info!("TimeTz value: {}", time_str);

                        // Get the ID to verify specific expected values
                        let id = match row.primary_key {
                            UniversalValue::Int32(i) => i,
                            _ => panic!("Expected Int32 primary key, got {:?}", row.primary_key),
                        };

                        // Verify timetz values preserve the original format
                        match id {
                            1 => {
                                assert!(
                                    time_str.contains("04:05:06"),
                                    "TimeTz for id=1 should contain 04:05:06, got {}",
                                    time_str
                                );
                            }
                            2 => {
                                // Original format is preserved: 04:05:06+05:30
                                assert!(
                                    time_str.contains("04:05:06"),
                                    "TimeTz for id=2 should contain 04:05:06, got {}",
                                    time_str
                                );
                            }
                            3 => {
                                // Original format is preserved: 15:37:16-08:00
                                assert!(
                                    time_str.contains("15:37:16"),
                                    "TimeTz for id=3 should contain 15:37:16, got {}",
                                    time_str
                                );
                            }
                            4 => {
                                assert!(
                                    time_str.contains("12:00:00"),
                                    "TimeTz for id=4 should contain 12:00:00, got {}",
                                    time_str
                                );
                            }
                            5 => {
                                assert!(
                                    time_str.contains("23:59:59"),
                                    "TimeTz for id=5 should contain 23:59:59, got {}",
                                    time_str
                                );
                            }
                            _ => panic!("Unexpected id: {id}"),
                        }
                    }
                    _ => panic!(
                        "Expected TimeTz (String) value, NOT ZonedDateTime. Got {:?}",
                        event_time
                    ),
                }
            }
            _ => panic!("Expected Insert action, got {change}"),
        }
    }

    info!("All INSERT changes verified successfully");

    // Now test UPDATE to ensure it also works
    pg_client
        .execute(
            "UPDATE timetz_test SET event_time = TIMETZ '18:30:00-05:00' WHERE id = $1",
            &[&1i32],
        )
        .await?;

    info!("Updated id=1 with new timetz");

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

            // Note: TIMETZ must be stored as String, NOT DateTime.
            // Time and datetime are fundamentally different types - datetime implies a specific
            // point in time, while TIMETZ represents a daily recurring time in a specific timezone.
            match event_time {
                UniversalValue::TimeTz(time_str) => {
                    info!("UPDATE TimeTz value: {}", time_str);
                    // Original format preserved: 18:30:00-05:00
                    assert!(
                        time_str.contains("18:30:00"),
                        "Updated timetz should contain 18:30:00, got {}",
                        time_str
                    );
                }
                _ => panic!(
                    "Expected TimeTz (String) value in UPDATE, NOT ZonedDateTime. Got {:?}",
                    event_time
                ),
            }
        }
        _ => panic!("Expected Update action"),
    }

    info!("UPDATE change verified successfully");

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("TIMETZ replication format test completed successfully");
    Ok(())
}
