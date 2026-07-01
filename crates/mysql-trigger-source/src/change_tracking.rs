//! MySQL trigger-based incremental sync infrastructure
//!
//! This module contains shared functionality for setting up MySQL triggers and audit tables
//! for trigger-based incremental synchronization. This infrastructure is used by both
//! full sync (to set up change tracking) and incremental sync (to read changes).

use crate::json_columns::{get_json_columns, json_object_value_expr};
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

    // Detect JSON columns up front so triggers can nest them as real JSON
    // (needed for MariaDB's LONGTEXT-backed JSON; a no-op for native MySQL JSON).
    let json_columns_by_table = get_json_columns(conn, database_name).await?;

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

        // Discover the actual primary key column(s) so triggers record the real
        // row identity rather than assuming an `id` column.
        let pk_columns = get_primary_key_columns(conn, database_name, &table_name).await?;
        if pk_columns.is_empty() {
            warn!(
                "Table '{table_name}' has no primary key; skipping change-tracking triggers \
                 (a primary key is required for change capture)"
            );
            continue;
        }

        let json_columns = json_columns_by_table
            .get(&table_name)
            .cloned()
            .unwrap_or_default();

        create_triggers_for_table(conn, &table_name, &columns, &pk_columns, &json_columns).await?;
    }

    info!("MySQL trigger-based change tracking setup completed");
    Ok(())
}

/// Query the primary key column names for a table, in key ordinal order.
/// Returns an empty vec when the table has no primary key.
async fn get_primary_key_columns(
    conn: &mut mysql_async::Conn,
    database_name: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                 AND CONSTRAINT_NAME = 'PRIMARY' \
                 ORDER BY ORDINAL_POSITION";

    let rows: Vec<Row> = conn.exec(query, (database_name, table_name)).await?;

    let pk_columns: Vec<String> = rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>(0))
        .collect();

    Ok(pk_columns)
}

/// Build the SQL expression that yields a row's identity for the audit table's
/// `row_id` column, using the actual primary key column(s).
///
/// A single primary-key column is recorded as its raw value (e.g. `NEW.user_id`);
/// a composite primary key is recorded as a JSON array (e.g.
/// `JSON_ARRAY(NEW.a, NEW.b)`) so all key parts are preserved.
fn build_row_id_expr(row_alias: &str, pk_columns: &[String]) -> String {
    if pk_columns.len() == 1 {
        format!("{row_alias}.{}", pk_columns[0])
    } else {
        let parts = pk_columns
            .iter()
            .map(|c| format!("{row_alias}.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("JSON_ARRAY({parts})")
    }
}

/// Create INSERT, UPDATE, DELETE triggers for a specific table.
///
/// `pk_columns` are the table's actual primary key column(s); they determine
/// what is written to the audit table's `row_id` column. `json_columns` are the
/// table's JSON columns, which are wrapped in `JSON_EXTRACT(..., '$')` so they
/// nest as real JSON in the audit payload on both MySQL and MariaDB.
pub async fn create_triggers_for_table(
    conn: &mut mysql_async::Conn,
    table_name: &str,
    columns: &[String],
    pk_columns: &[String],
    json_columns: &[String],
) -> Result<()> {
    // Build column lists for JSON_OBJECT()
    let new_columns = columns
        .iter()
        .map(|c| format!("'{c}', {}", json_object_value_expr("NEW", c, json_columns)))
        .collect::<Vec<_>>()
        .join(", ");

    let old_columns = columns
        .iter()
        .map(|c| format!("'{c}', {}", json_object_value_expr("OLD", c, json_columns)))
        .collect::<Vec<_>>()
        .join(", ");

    // Record the real primary key value(s) in row_id rather than assuming `id`.
    let new_row_id = build_row_id_expr("NEW", pk_columns);
    let old_row_id = build_row_id_expr("OLD", pk_columns);

    // Create INSERT trigger
    let insert_trigger = format!(
        "CREATE TRIGGER surreal_sync_insert_{table_name}
         AFTER INSERT ON {table_name}
         FOR EACH ROW
         INSERT INTO surreal_sync_changes (table_name, operation, row_id, new_data)
         VALUES ('{table_name}', 'INSERT', {new_row_id}, JSON_OBJECT({new_columns}))"
    );

    // Create UPDATE trigger
    let update_trigger = format!(
        "CREATE TRIGGER surreal_sync_update_{table_name}
         AFTER UPDATE ON {table_name}
         FOR EACH ROW
         INSERT INTO surreal_sync_changes (table_name, operation, row_id, old_data, new_data)
         VALUES ('{table_name}', 'UPDATE', {new_row_id}, JSON_OBJECT({old_columns}), JSON_OBJECT({new_columns}))"
    );

    // Create DELETE trigger
    let delete_trigger = format!(
        "CREATE TRIGGER surreal_sync_delete_{table_name}
         AFTER DELETE ON {table_name}
         FOR EACH ROW
         INSERT INTO surreal_sync_changes (table_name, operation, row_id, old_data)
         VALUES ('{table_name}', 'DELETE', {old_row_id}, JSON_OBJECT({old_columns}))"
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
