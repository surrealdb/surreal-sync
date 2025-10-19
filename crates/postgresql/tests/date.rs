//! Integration tests for DATE replication with various date formats

use anyhow::{Context, Result};
use chrono::{DateTime, Timelike, Utc};
use surreal_sync_postgresql_replication::{
    testing::container::PostgresContainer, Action, Client, Value,
};
use tokio_postgres::NoTls;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Test port that doesn't conflict with standard PostgreSQL port
const TEST_PORT: u16 = 15439;

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
async fn test_date_replication_formats() -> Result<()> {
    init_logging();
    info!("Starting DATE replication format test");

    // Create container configuration
    let container = PostgresContainer::new("test-date", TEST_PORT);

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

    // Create a test table with id and date field
    pg_client
        .execute(
            "CREATE TABLE date_test (
                id INTEGER PRIMARY KEY,
                event_date DATE
            )",
            &[],
        )
        .await?;

    info!("Created date_test table");

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
    let slot_name = "date_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert test data with various date formats supported by PostgreSQL
    // See: https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-INPUT-DATES

    // 1. ISO 8601 format (YYYY-MM-DD)
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE '1999-01-08')",
            &[&1i32],
        )
        .await?;
    info!("Inserted ISO 8601 date");

    // 2. PostgreSQL traditional format (Month DD, YYYY)
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE 'January 8, 1999')",
            &[&2i32],
        )
        .await?;
    info!("Inserted traditional format date");

    // 3. PostgreSQL format with abbreviated month
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE '8 Jan 1999')",
            &[&3i32],
        )
        .await?;
    info!("Inserted abbreviated month format");

    // 4. US format (MM/DD/YYYY) - requires DateStyle setting
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE '01/08/1999')",
            &[&4i32],
        )
        .await?;
    info!("Inserted US format date");

    // 5. SQL format with dashes (DD-MM-YYYY)
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE '08-Jan-1999')",
            &[&5i32],
        )
        .await?;
    info!("Inserted day-month-year format date");

    // 6. Epoch (special date)
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE 'epoch')",
            &[&6i32],
        )
        .await?;
    info!("Inserted epoch date");

    // 7. Recent date (2024)
    pg_client
        .execute(
            "INSERT INTO date_test (id, event_date) VALUES ($1, DATE '2024-03-15')",
            &[&7i32],
        )
        .await?;
    info!("Inserted recent date");

    // Wait for replication
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Peek at changes
    let (changes, _nextlsn) = slot.peek().await?;
    info!("Received {} changes from replication", changes.len());

    // We should have 7 INSERT actions
    assert_eq!(changes.len(), 7, "Should have 7 INSERT changes");

    // Verify each insert can be converted to DateTime<Utc>
    for (idx, change) in changes.iter().enumerate() {
        match change {
            Action::Insert(row) => {
                info!("Verifying change {} for id={}", idx + 1, idx + 1);

                assert_eq!(row.table, "date_test");
                assert_eq!(row.schema, "public");

                // Extract the event_date field
                let event_date = row
                    .columns
                    .get("event_date")
                    .context("Should have event_date column")?;

                // Verify it's a Date value
                match event_date {
                    Value::Date(date_val) => {
                        info!("Raw date value: {}", date_val.0);

                        // Convert to DateTime<Utc> (at midnight UTC)
                        let dt = date_val.to_chrono_datetime_utc().map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to convert date to DateTime<Utc>: {} - {}",
                                date_val.0,
                                e
                            )
                        })?;

                        info!(
                            "Successfully converted to DateTime<Utc>: {}",
                            dt.to_rfc3339()
                        );

                        // Verify the timestamp is at midnight UTC
                        assert_eq!(dt.hour(), 0);
                        assert_eq!(dt.minute(), 0);
                        assert_eq!(dt.second(), 0);

                        // Verify specific values based on ID
                        let id = match row.primary_key {
                            Value::Integer(i) => i,
                            _ => panic!("Expected integer primary key"),
                        };

                        match id {
                            1 | 2 | 3 => {
                                // All these should normalize to 1999-01-08
                                assert_eq!(dt.to_rfc3339(), "1999-01-08T00:00:00+00:00");
                            }
                            4 | 5 => {
                                // US format and month name format
                                // 01/08/1999 (US) = January 8, 1999
                                // 08-Jan-1999 = January 8, 1999
                                // Both should parse to the same date
                                assert_eq!(dt.to_rfc3339(), "1999-01-08T00:00:00+00:00");
                            }
                            6 => {
                                // Epoch = 1970-01-01
                                assert_eq!(dt.to_rfc3339(), "1970-01-01T00:00:00+00:00");
                            }
                            7 => {
                                // 2024-03-15
                                assert_eq!(dt.to_rfc3339(), "2024-03-15T00:00:00+00:00");
                            }
                            _ => panic!("Unexpected id: {id}"),
                        }

                        // Verify it's in the past or reasonable range
                        let now = Utc::now();
                        let min_time =
                            DateTime::parse_from_rfc3339("1960-01-01T00:00:00Z").unwrap();
                        assert!(
                            dt > min_time.with_timezone(&Utc),
                            "Date should be after 1960: {}",
                            dt.to_rfc3339()
                        );
                        assert!(dt < now, "Date should be in the past: {}", dt.to_rfc3339());
                    }
                    _ => panic!("Expected Date value, got {event_date:?}"),
                }
            }
            _ => panic!("Expected Insert action, got {change}"),
        }
    }

    info!("All INSERT changes verified successfully");

    // Now test UPDATE to ensure it also works
    pg_client
        .execute(
            "UPDATE date_test SET event_date = DATE '2025-12-25' WHERE id = $1",
            &[&1i32],
        )
        .await?;

    info!("Updated id=1 with new date");

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

            let event_date = row
                .columns
                .get("event_date")
                .context("Should have event_date column in UPDATE")?;

            match event_date {
                Value::Date(date_val) => {
                    let dt = date_val.to_chrono_datetime_utc().map_err(|e| {
                        anyhow::anyhow!("Failed to convert updated date: {} - {}", date_val.0, e)
                    })?;

                    info!("UPDATE date successfully converted: {}", dt.to_rfc3339());

                    // Verify the updated value
                    assert_eq!(dt.to_rfc3339(), "2025-12-25T00:00:00+00:00");
                }
                _ => panic!("Expected Date value in UPDATE"),
            }
        }
        _ => panic!("Expected Update action"),
    }

    info!("UPDATE change verified successfully");

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("DATE replication format test completed successfully");
    Ok(())
}
