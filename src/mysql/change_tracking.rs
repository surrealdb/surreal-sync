//! MySQL trigger-based incremental sync infrastructure
//!
//! This module contains shared functionality for setting up MySQL triggers and audit tables
//! for trigger-based incremental synchronization. This infrastructure is used by both
//! full sync (to set up change tracking) and incremental sync (to read changes).

use anyhow::{anyhow, Result};
use mysql_async::{prelude::*, Row};
use tracing::{info, warn};

/// Set up MySQL triggers and audit table for capturing changes during full sync
pub async fn setup_mysql_change_tracking(
    conn: &mut mysql_async::Conn,
    database_name: &str,
) -> Result<()> {
    // Create audit table for tracking changes
    let create_table = "CREATE TABLE IF NOT EXISTS surreal_sync_changes (
        sequence_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        operation VARCHAR(10) NOT NULL,
        row_id VARCHAR(255) NOT NULL,
        old_data JSON,
        new_data JSON,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_sequence (sequence_id)
    )";

    conn.query_drop(create_table).await?;

    // Get list of tables to set up triggers for
    let tables_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                       WHERE TABLE_SCHEMA = ?
                       AND TABLE_TYPE = 'BASE TABLE'
                       AND TABLE_NAME != 'surreal_sync_changes'";

    let table_rows: Vec<Row> = conn.exec(tables_query, (database_name,)).await?;

    for row in table_rows {
        let table_name: String = row.get(0).ok_or_else(|| anyhow!("Missing table name"))?;

        // Skip system tables
        if table_name.starts_with("mysql")
            || table_name.starts_with("information_schema")
            || table_name.starts_with("performance_schema")
        {
            continue;
        }

        // Get table columns for building JSON objects
        let columns_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
                            ORDER BY ORDINAL_POSITION";

        let column_rows: Vec<Row> = conn
            .exec(columns_query, (&table_name, database_name))
            .await?;
        let columns: Vec<String> = column_rows
            .into_iter()
            .filter_map(|row| row.get::<String, _>(0))
            .collect();

        if columns.is_empty() {
            warn!("No columns found for table: {table_name}");
            continue;
        }

        create_triggers_for_table(conn, &table_name, &columns).await?;
    }

    info!("MySQL trigger-based change tracking setup completed");
    Ok(())
}

/// Create INSERT, UPDATE, DELETE triggers for a specific table
pub async fn create_triggers_for_table(
    conn: &mut mysql_async::Conn,
    table_name: &str,
    columns: &[String],
) -> Result<()> {
    // Build column lists for JSON_OBJECT()
    let new_columns = columns
        .iter()
        .map(|c| format!("'{c}', NEW.{c}"))
        .collect::<Vec<_>>()
        .join(", ");

    let old_columns = columns
        .iter()
        .map(|c| format!("'{c}', OLD.{c}"))
        .collect::<Vec<_>>()
        .join(", ");

    // Create INSERT trigger
    let insert_trigger = format!(
        "CREATE TRIGGER surreal_sync_insert_{table_name}
         AFTER INSERT ON {table_name}
         FOR EACH ROW
         INSERT INTO surreal_sync_changes (table_name, operation, row_id, new_data)
         VALUES ('{table_name}', 'INSERT', NEW.id, JSON_OBJECT({new_columns}))"
    );

    // Create UPDATE trigger
    let update_trigger = format!(
        "CREATE TRIGGER surreal_sync_update_{table_name}
         AFTER UPDATE ON {table_name}
         FOR EACH ROW
         INSERT INTO surreal_sync_changes (table_name, operation, row_id, old_data, new_data)
         VALUES ('{table_name}', 'UPDATE', NEW.id, JSON_OBJECT({old_columns}), JSON_OBJECT({new_columns}))"
    );

    // Create DELETE trigger
    let delete_trigger = format!(
        "CREATE TRIGGER surreal_sync_delete_{table_name}
         AFTER DELETE ON {table_name}
         FOR EACH ROW
         INSERT INTO surreal_sync_changes (table_name, operation, row_id, old_data)
         VALUES ('{table_name}', 'DELETE', OLD.id, JSON_OBJECT({old_columns}))"
    );

    // Execute triggers, ignoring "already exists" errors
    for (trigger_type, trigger_sql) in [
        ("INSERT", insert_trigger),
        ("UPDATE", update_trigger),
        ("DELETE", delete_trigger),
    ] {
        match conn.query_drop(&trigger_sql).await {
            Ok(_) => info!("Created {trigger_type} trigger for table: {table_name}"),
            Err(e) if e.to_string().contains("already exists") => {
                info!("{trigger_type} trigger already exists for table: {table_name}");
            }
            Err(e) => warn!("Failed to create {trigger_type} trigger for {table_name}: {e}"),
        }
    }

    Ok(())
}
