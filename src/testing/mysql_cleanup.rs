//! MySQL test cleanup utilities

use anyhow::Result;
use mysql_async::prelude::Queryable;

/// Clean up MySQL triggers and audit table created by incremental sync
///
/// This function removes all surreal_sync triggers and the surreal_sync_changes audit table.
/// It should be called at both the beginning and end of tests to ensure a clean state.
pub async fn cleanup_triggers_and_audit(conn: &mut mysql_async::Conn) -> Result<()> {
    // Get all tables to find their triggers
    let existing_tables: Vec<String> = conn
        .query("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'")
        .await?;

    for table_name in existing_tables {
        // Drop triggers for each table (ignore errors if they don't exist)
        let _ = conn
            .query_drop(format!(
                "DROP TRIGGER IF EXISTS surreal_sync_insert_{table_name}"
            ))
            .await;
        let _ = conn
            .query_drop(format!(
                "DROP TRIGGER IF EXISTS surreal_sync_update_{table_name}"
            ))
            .await;
        let _ = conn
            .query_drop(format!(
                "DROP TRIGGER IF EXISTS surreal_sync_delete_{table_name}"
            ))
            .await;
    }

    // Drop the audit table if it exists
    let _ = conn
        .query_drop("DROP TABLE IF EXISTS surreal_sync_changes")
        .await;

    Ok(())
}

/// Clean up specific test tables
///
/// Drops the specified tables if they exist.
pub async fn cleanup_test_tables(conn: &mut mysql_async::Conn, tables: &[&str]) -> Result<()> {
    for table in tables {
        let _ = conn
            .query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .await;
    }
    Ok(())
}

/// Complete cleanup for MySQL tests
///
/// This performs a full cleanup:
/// 1. Drops all triggers
/// 2. Drops the audit table
/// 3. Drops specified test tables
pub async fn full_cleanup(conn: &mut mysql_async::Conn, test_tables: &[&str]) -> Result<()> {
    // Disable foreign key checks temporarily for cleanup
    let _ = conn.query_drop("SET FOREIGN_KEY_CHECKS = 0").await;

    // First clean up triggers and audit table
    cleanup_triggers_and_audit(conn).await?;

    // Then clean up test tables
    cleanup_test_tables(conn, test_tables).await?;

    // Re-enable foreign key checks
    let _ = conn.query_drop("SET FOREIGN_KEY_CHECKS = 1").await;

    Ok(())
}
