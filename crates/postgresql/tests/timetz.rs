//! Integration tests for TIMETZ replication with various time with timezone formats

use anyhow::{Context, Result};
use surreal_sync_postgresql_replication::{
    testing::container::PostgresContainer, Action, Client, Value,
};
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

    // Verify each insert can be converted to DateTime<Utc>
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

                // Verify it's a TimeTz value
                match event_time {
                    Value::TimeTz(timetz_val) => {
                        info!("Raw timetz value: {}", timetz_val.0);

                        // Convert to DateTime<Utc> (uses epoch date 1970-01-01)
                        let dt = timetz_val.to_chrono_datetime_utc().map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to convert timetz to DateTime<Utc>: {} - {}",
                                timetz_val.0,
                                e
                            )
                        })?;

                        info!(
                            "Successfully converted to DateTime<Utc>: {}",
                            dt.to_rfc3339()
                        );

                        // Verify specific values based on ID
                        // All times are normalized to UTC on epoch date
                        let id = match row.primary_key {
                            Value::Integer(i) => i,
                            _ => panic!("Expected integer primary key"),
                        };

                        match id {
                            1 => {
                                // 04:05:06+00:00 = 04:05:06 UTC
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T04:05:06+00:00");
                            }
                            2 => {
                                // 04:05:06+05:30 = 1969-12-31T22:35:06 UTC (previous day!)
                                assert_eq!(dt.to_rfc3339(), "1969-12-31T22:35:06+00:00");
                            }
                            3 => {
                                // 15:37:16-08:00 = 23:37:16 UTC (same day)
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T23:37:16+00:00");
                            }
                            4 => {
                                // 12:00:00Z = 12:00:00 UTC
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T12:00:00+00:00");
                            }
                            5 => {
                                // 23:59:59.999999+00:00 = 23:59:59.999999 UTC
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T23:59:59.999999+00:00");
                            }
                            _ => panic!("Unexpected id: {id}"),
                        }
                    }
                    _ => panic!("Expected TimeTz value, got {event_time:?}"),
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

            match event_time {
                Value::TimeTz(timetz_val) => {
                    let dt = timetz_val.to_chrono_datetime_utc().map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to convert updated timetz: {} - {}",
                            timetz_val.0,
                            e
                        )
                    })?;

                    info!("UPDATE timetz successfully converted: {}", dt.to_rfc3339());

                    // Verify the updated value
                    // 18:30:00-05:00 = 23:30:00 UTC (same day on epoch date)
                    assert_eq!(dt.to_rfc3339(), "1970-01-01T23:30:00+00:00");
                }
                _ => panic!("Expected TimeTz value in UPDATE"),
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
