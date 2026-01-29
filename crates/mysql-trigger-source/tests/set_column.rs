//! Integration tests for MySQL SET column type conversion
//!
//! This test verifies how MySQL SET columns are converted to UniversalValue types.
//! SET columns store comma-separated values and should be converted to arrays.
#![allow(clippy::uninlined_format_args)]

use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_types::{row_to_typed_values, row_to_typed_values_with_config, RowConversionConfig};
use surreal_sync_mysql_trigger_source::testing::MySQLContainer;
use sync_core::UniversalValue;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Test port that doesn't conflict with standard MySQL port
const TEST_PORT: u16 = 13309;

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

/// Test that demonstrates mysql_async reports SET columns as MYSQL_TYPE_STRING (254).
/// Without schema-based detection, SET columns are returned as Text/Char values,
/// NOT as Set arrays. This test documents this behavior.
///
/// For proper SET column handling, use `row_to_typed_values_with_config` with
/// schema-based detection (see `test_set_column_with_schema_detection`).
#[tokio::test]
async fn test_set_column_without_schema_returns_string() -> Result<()> {
    init_logging();
    info!("Testing that SET columns without schema detection return as strings");

    // Create and start container
    let container = MySQLContainer::new("test-set-column", TEST_PORT);
    container.start()?;
    container.wait_until_ready(60).await?;

    // Get connection pool
    let pool = container.get_pool()?;
    let mut conn = pool.get_conn().await?;

    // Create test table with SET column
    conn.query_drop(
        "CREATE TABLE set_test (
            id INT PRIMARY KEY,
            tags SET('technology', 'tutorial', 'news', 'opinion')
        )",
    )
    .await
    .context("Failed to create set_test table")?;

    info!("Created set_test table with SET columns");

    // Insert test data
    conn.query_drop("INSERT INTO set_test (id, tags) VALUES (1, 'technology,tutorial')")
        .await?;
    conn.query_drop("INSERT INTO set_test (id, tags) VALUES (2, NULL)")
        .await?;

    info!("Inserted test data");

    // Query the data
    let rows: Vec<mysql_async::Row> = conn.query("SELECT * FROM set_test ORDER BY id").await?;
    assert_eq!(rows.len(), 2, "Should have 2 test rows");

    // Check column type reported by mysql_async
    // KEY FINDING: mysql_async reports SET as MYSQL_TYPE_STRING (254), not MYSQL_TYPE_SET
    let tags_col = &rows[0].columns()[1];
    let col_type = tags_col.column_type();
    info!("tags column type: {:?} (code={})", col_type, col_type as u8);

    // Verify mysql_async reports STRING type, not SET type
    use mysql_async::consts::ColumnType;
    assert_eq!(
        col_type,
        ColumnType::MYSQL_TYPE_STRING,
        "mysql_async reports SET columns as MYSQL_TYPE_STRING, not MYSQL_TYPE_SET"
    );

    // Convert row WITHOUT schema detection
    let typed_values = row_to_typed_values(&rows[0], None, None)?;
    let tags = typed_values.get("tags").context("Should have tags field")?;

    info!(
        "Without schema detection - tags type: {:?}, value: {:?}",
        tags.sync_type, tags.value
    );

    // Verify it comes back as a string (Char), NOT as a Set
    match &tags.value {
        UniversalValue::Char { value, .. } => {
            assert_eq!(value, "technology,tutorial");
            info!("✓ Confirmed: SET column returns as Char without schema detection");
        }
        other => {
            panic!(
                "Expected Char value without schema detection, got {:?}",
                other
            );
        }
    }

    // NULL values should still work correctly
    let typed_values_null = row_to_typed_values(&rows[1], None, None)?;
    let tags_null = typed_values_null
        .get("tags")
        .context("Should have tags field")?;
    assert!(
        matches!(tags_null.value, UniversalValue::Null),
        "NULL SET should be Null, got {:?}",
        tags_null.value
    );

    // Cleanup
    drop(conn);
    pool.disconnect().await?;
    container.stop()?;

    info!("Test completed: SET columns without schema return as strings");
    Ok(())
}

/// Test SET column conversion WITH schema-based detection
/// This shows the correct approach: query INFORMATION_SCHEMA to detect SET columns
#[tokio::test]
async fn test_set_column_with_schema_detection() -> Result<()> {
    init_logging();
    info!("Starting SET column test WITH schema-based detection");

    let container = MySQLContainer::new("test-set-schema", TEST_PORT + 2);
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = container.get_pool()?;
    let mut conn = pool.get_conn().await?;

    // Create test table
    conn.query_drop(
        "CREATE TABLE set_schema_test (
            id INT PRIMARY KEY,
            tags SET('tech', 'news', 'sports')
        )",
    )
    .await?;

    conn.query_drop("INSERT INTO set_schema_test VALUES (1, 'tech,news')")
        .await?;

    // STEP 1: Query INFORMATION_SCHEMA to detect SET columns
    let set_columns: Vec<String> = conn
        .query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = 'testdb'
             AND TABLE_NAME = 'set_schema_test'
             AND DATA_TYPE = 'set'",
        )
        .await?;

    info!("Detected SET columns from schema: {:?}", set_columns);
    assert_eq!(
        set_columns,
        vec!["tags"],
        "Should detect 'tags' as SET column"
    );

    // STEP 2: Query data
    let rows: Vec<mysql_async::Row> = conn.query("SELECT * FROM set_schema_test").await?;
    let row = &rows[0];

    // STEP 3: Convert WITH set_columns configuration
    let config = RowConversionConfig {
        boolean_columns: vec![],
        set_columns: set_columns.clone(),
        json_config: None,
    };

    let typed_values = row_to_typed_values_with_config(row, &config)?;

    let tags = typed_values.get("tags").context("Should have tags field")?;
    info!(
        "With schema detection - tags type: {:?}, value: {:?}",
        tags.sync_type, tags.value
    );

    // Verify it's now a proper Set
    match &tags.value {
        UniversalValue::Set { elements, .. } => {
            assert_eq!(elements, &vec!["tech".to_string(), "news".to_string()]);
            info!("✓ SET column correctly converted to Set type with schema detection");
        }
        other => {
            panic!("Expected Set value with schema detection, got {:?}", other);
        }
    }

    drop(conn);
    pool.disconnect().await?;
    container.stop()?;

    info!("Schema-based SET detection test completed successfully");
    Ok(())
}

/// Test that verifies what ColumnType mysql_async reports for SET columns
#[tokio::test]
async fn test_mysql_async_set_column_type() -> Result<()> {
    init_logging();
    info!("Testing mysql_async ColumnType for SET columns");

    let container = MySQLContainer::new("test-set-coltype", TEST_PORT + 1);
    container.start()?;
    container.wait_until_ready(60).await?;

    let pool = container.get_pool()?;
    let mut conn = pool.get_conn().await?;

    // Create test table
    conn.query_drop("CREATE TABLE coltype_test (id INT, mysets SET('a','b','c'))")
        .await?;
    conn.query_drop("INSERT INTO coltype_test VALUES (1, 'a,b')")
        .await?;

    // Query and check column type
    let rows: Vec<mysql_async::Row> = conn.query("SELECT * FROM coltype_test").await?;
    let row = &rows[0];

    // Find the mysets column
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name_str();
        let col_type = col.column_type();

        info!(
            "Column '{}': ColumnType = {:?} (value = {})",
            name, col_type, col_type as u8
        );

        if name == "mysets" {
            // Check if mysql_async reports this as MYSQL_TYPE_SET
            use mysql_async::consts::ColumnType;
            if col_type == ColumnType::MYSQL_TYPE_SET {
                info!("✓ mysql_async correctly identifies SET column as MYSQL_TYPE_SET");
            } else {
                info!(
                    "✗ mysql_async reports SET column as {:?}, not MYSQL_TYPE_SET",
                    col_type
                );
            }

            // Also get the raw value to see what we receive
            let raw_val: Option<String> = row.get(i);
            info!("Raw value for mysets: {:?}", raw_val);
        }
    }

    drop(conn);
    pool.disconnect().await?;
    container.stop()?;

    info!("ColumnType test completed");
    Ok(())
}
