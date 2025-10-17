//! Tests for wal2json to PostgreSQL type conversion

use anyhow::{Context, Result};
use surreal_sync_postgresql_replication::{
    testing::container::PostgresContainer, wal2json_to_psql, Action, Client, Value,
};
use tokio_postgres::NoTls;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Test port that doesn't conflict with standard PostgreSQL port
const TEST_PORT: u16 = 15435;

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
async fn test_wal2json_to_psql_conversion() -> Result<()> {
    init_logging();
    info!("Starting wal2json_to_psql conversion test");

    // Create container configuration
    let container = PostgresContainer::new("test-conversion", TEST_PORT);

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

    // Create a test table with various data types
    pg_client
        .execute(
            "CREATE TABLE conversion_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                active BOOLEAN,
                score REAL,
                data JSON,
                tags TEXT[]
            )",
            &[],
        )
        .await?;

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
    let slot_name = "conversion_slot";
    repl_client.create_slot(slot_name).await?;
    let slot = repl_client.start_replication(Some(slot_name)).await?;

    // Insert test data
    let json_data = serde_json::json!({"key": "value", "nested": {"count": 42}});
    pg_client
        .execute(
            "INSERT INTO conversion_test (id, name, active, score, data, tags)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &123i32,
                &"Test Name",
                &true,
                &3.14f32,
                &json_data,
                &vec!["tag1", "tag2", "tag3"],
            ],
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Peek at changes
    let (changes, _nextlsn) = slot.peek().await?;
    assert!(!changes.is_empty(), "Should have captured INSERT change");

    // Find the INSERT action (skip Begin/Commit)
    let insert_change = changes
        .iter()
        .find(|c| c.get("action").and_then(|a| a.as_str()) == Some("I"))
        .context("Should find INSERT action")?;

    info!(
        "Raw wal2json INSERT: {}",
        serde_json::to_string_pretty(insert_change)?
    );

    // Convert using our new function
    let action = wal2json_to_psql(insert_change)?;

    match action {
        Action::Insert(row) => {
            info!("Converted to Insert action");
            assert_eq!(row.table, "conversion_test");
            assert_eq!(row.schema, "public");

            // Check primary key
            assert_eq!(row.primary_key, Value::Integer(123));

            // Check columns
            assert_eq!(
                row.columns.get("name"),
                Some(&Value::Text("Test Name".to_string()))
            );
            assert_eq!(row.columns.get("active"), Some(&Value::Boolean(true)));

            // Check float
            if let Some(Value::Real(score)) = row.columns.get("score") {
                assert!((score - 3.14).abs() < 0.001);
            } else {
                panic!("Expected Real value for score");
            }

            // Check JSON
            if let Some(Value::Json(json_val)) = row.columns.get("data") {
                assert_eq!(json_val.get("key").and_then(|v| v.as_str()), Some("value"));
                assert_eq!(
                    json_val
                        .get("nested")
                        .and_then(|n| n.get("count"))
                        .and_then(|c| c.as_i64()),
                    Some(42)
                );
            } else {
                panic!("Expected Json value for data");
            }

            // Check array
            if let Some(Value::Array(tags)) = row.columns.get("tags") {
                assert_eq!(tags.len(), 3);
                assert_eq!(tags[0], Value::Text("tag1".to_string()));
                assert_eq!(tags[1], Value::Text("tag2".to_string()));
                assert_eq!(tags[2], Value::Text("tag3".to_string()));
            } else {
                panic!("Expected Array value for tags");
            }

            info!("All assertions passed for INSERT conversion");
        }
        _ => panic!("Expected Insert action"),
    }

    // Test UPDATE
    pg_client
        .execute(
            "UPDATE conversion_test SET name = $1, active = $2 WHERE id = $3",
            &[&"Updated Name", &false, &123i32],
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let (changes2, _) = slot.peek().await?;
    let update_change = changes2
        .iter()
        .find(|c| c.get("action").and_then(|a| a.as_str()) == Some("U"))
        .context("Should find UPDATE action")?;

    let update_action = wal2json_to_psql(update_change)?;

    match update_action {
        Action::Update(row) => {
            info!("Converted to Update action");
            assert_eq!(row.primary_key, Value::Integer(123));
            assert_eq!(
                row.columns.get("name"),
                Some(&Value::Text("Updated Name".to_string()))
            );
            assert_eq!(row.columns.get("active"), Some(&Value::Boolean(false)));
            info!("All assertions passed for UPDATE conversion");
        }
        _ => panic!("Expected Update action"),
    }

    // Test DELETE
    pg_client
        .execute("DELETE FROM conversion_test WHERE id = $1", &[&123i32])
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let (changes3, _) = slot.peek().await?;
    let delete_change = changes3
        .iter()
        .find(|c| c.get("action").and_then(|a| a.as_str()) == Some("D"))
        .context("Should find DELETE action")?;

    let delete_action = wal2json_to_psql(delete_change)?;

    match delete_action {
        Action::Delete(row) => {
            info!("Converted to Delete action");
            assert_eq!(row.primary_key, Value::Integer(123));
            info!("All assertions passed for DELETE conversion");
        }
        _ => panic!("Expected Delete action"),
    }

    // Cleanup
    repl_client.drop_slot(slot_name).await?;
    container.stop()?;

    info!("wal2json_to_psql conversion test completed successfully");
    Ok(())
}
