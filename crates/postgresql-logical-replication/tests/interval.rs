//! Integration tests for INTERVAL replication with various interval formats

use anyhow::{Context, Result};
use std::time::Duration;
use surreal_sync_postgresql_logical_replication::{
    testing::container::PostgresContainer, Action, Client, Value,
};
use tokio_postgres::NoTls;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Test port that doesn't conflict with standard PostgreSQL port
const TEST_PORT: u16 = 15440;

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
async fn test_interval_replication_formats() -> Result<()> {
    init_logging();
    info!("Starting INTERVAL replication format test");

    // Create container configuration
    let container = PostgresContainer::new("test-interval", TEST_PORT);

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

    // Create a test table with id and interval field
    pg_client
        .execute(
            "CREATE TABLE interval_test (
                id INTEGER PRIMARY KEY,
                duration INTERVAL
            )",
            &[],
        )
        .await?;

    info!("Created interval_test table");

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
    let slot_name = "interval_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert test data with various interval formats
    // See: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT

    // 1. Simple seconds interval
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '30 seconds')",
            &[&1i32],
        )
        .await?;
    info!("Inserted simple seconds interval");

    // 2. Minutes interval
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '5 minutes')",
            &[&2i32],
        )
        .await?;
    info!("Inserted minutes interval");

    // 3. Hours interval
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '2 hours')",
            &[&3i32],
        )
        .await?;
    info!("Inserted hours interval");

    // 4. Days interval
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '3 days')",
            &[&4i32],
        )
        .await?;
    info!("Inserted days interval");

    // 5. Weeks interval
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '2 weeks')",
            &[&5i32],
        )
        .await?;
    info!("Inserted weeks interval");

    // 6. Months interval (approximated to 30 days in conversion)
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '1 month')",
            &[&6i32],
        )
        .await?;
    info!("Inserted months interval");

    // 7. Years interval (approximated to 360 days in conversion)
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '1 year')",
            &[&7i32],
        )
        .await?;
    info!("Inserted years interval");

    // 8. Complex interval with multiple components
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL '1 day 2 hours 30 minutes')",
            &[&8i32],
        )
        .await?;
    info!("Inserted complex interval");

    // 9. ISO 8601 format interval (e.g., P1DT2H30M)
    pg_client
        .execute(
            "INSERT INTO interval_test (id, duration) VALUES ($1, INTERVAL 'P1DT2H30M')",
            &[&9i32],
        )
        .await?;
    info!("Inserted ISO 8601 interval");

    // Wait for replication
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek at changes
    let (changes, _nextlsn) = slot.peek().await?;
    info!("Received {} changes from replication", changes.len());

    // We should have 9 INSERT actions
    assert_eq!(changes.len(), 9, "Should have 9 INSERT changes");

    // Verify each insert can be converted to Duration
    for (idx, change) in changes.iter().enumerate() {
        match change {
            Action::Insert(row) => {
                info!("Verifying change {} for id={}", idx + 1, idx + 1);

                assert_eq!(row.table, "interval_test");
                assert_eq!(row.schema, "public");

                // Extract the duration field
                let duration_field = row
                    .columns
                    .get("duration")
                    .context("Should have duration column")?;

                // Verify it's an Interval value
                match duration_field {
                    Value::Interval(interval_val) => {
                        info!("Raw interval value: {}", interval_val.0);

                        // Convert to Duration
                        let duration = interval_val.to_duration().map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to convert interval to Duration: {} - {}",
                                interval_val.0,
                                e
                            )
                        })?;

                        info!(
                            "Successfully converted to Duration: {:?} ({} seconds)",
                            duration,
                            duration.as_secs()
                        );

                        // Verify specific values based on ID
                        let id = match row.primary_key {
                            Value::Integer(i) => i,
                            _ => panic!("Expected integer primary key"),
                        };

                        match id {
                            1 => {
                                // 30 seconds
                                assert_eq!(duration, Duration::from_secs(30));
                            }
                            2 => {
                                // 5 minutes = 300 seconds
                                assert_eq!(duration, Duration::from_secs(300));
                            }
                            3 => {
                                // 2 hours = 7200 seconds
                                assert_eq!(duration, Duration::from_secs(7200));
                            }
                            4 => {
                                // 3 days = 259200 seconds
                                assert_eq!(duration, Duration::from_secs(259200));
                            }
                            5 => {
                                // 2 weeks = 1209600 seconds
                                assert_eq!(duration, Duration::from_secs(1209600));
                            }
                            6 => {
                                // 1 month (approximated to 30 days) = 2592000 seconds
                                assert_eq!(duration, Duration::from_secs(2592000));
                            }
                            7 => {
                                // 1 year (approximated to 360 days) = 31104000 seconds
                                assert_eq!(duration, Duration::from_secs(31104000));
                            }
                            8 | 9 => {
                                // 1 day 2 hours 30 minutes = 95400 seconds
                                // Both the traditional and ISO 8601 formats should produce the same result
                                assert_eq!(duration, Duration::from_secs(95400));
                            }
                            _ => panic!("Unexpected id: {id}"),
                        }
                    }
                    _ => panic!("Expected Interval value, got {duration_field:?}"),
                }
            }
            _ => panic!("Expected Insert action, got {change}"),
        }
    }

    info!("All INSERT changes verified successfully");

    // Now test UPDATE to ensure it also works
    pg_client
        .execute(
            "UPDATE interval_test SET duration = INTERVAL '5 hours 30 minutes' WHERE id = $1",
            &[&1i32],
        )
        .await?;

    info!("Updated id=1 with new interval");

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

            let duration_field = row
                .columns
                .get("duration")
                .context("Should have duration column in UPDATE")?;

            match duration_field {
                Value::Interval(interval_val) => {
                    let duration = interval_val.to_duration().map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to convert updated interval: {} - {}",
                            interval_val.0,
                            e
                        )
                    })?;

                    info!(
                        "UPDATE interval successfully converted: {:?} ({} seconds)",
                        duration,
                        duration.as_secs()
                    );

                    // Verify the updated value
                    // 5 hours 30 minutes = 19800 seconds
                    assert_eq!(duration, Duration::from_secs(19800));
                }
                _ => panic!("Expected Interval value in UPDATE"),
            }
        }
        _ => panic!("Expected Update action"),
    }

    info!("UPDATE change verified successfully");

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("INTERVAL replication format test completed successfully");
    Ok(())
}
