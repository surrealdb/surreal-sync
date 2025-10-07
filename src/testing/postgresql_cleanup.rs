//! PostgreSQL test cleanup utilities

use anyhow::Result;
use tokio_postgres::Client;

/// Clean up PostgreSQL triggers and audit table created by incremental sync
///
/// This function removes all surreal_sync triggers, the tracking function,
/// and the surreal_sync_changes audit table.
/// It should be called at both the beginning and end of tests to ensure a clean state.
pub async fn cleanup_triggers_and_audit(client: &Client) -> Result<()> {
    // Get all tables to find their triggers
    let tables_query = "SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                        AND tablename NOT LIKE 'pg_%'";

    let rows = client.query(tables_query, &[]).await?;

    for row in rows {
        let table_name: String = row.get(0);

        // Drop trigger for each table (ignore errors if they don't exist)
        let _ = client
            .execute(
                &format!(
                    "DROP TRIGGER IF EXISTS surreal_sync_trigger_{table_name} ON {table_name}"
                ),
                &[],
            )
            .await;
    }

    // Drop the tracking function
    let _ = client
        .execute(
            "DROP FUNCTION IF EXISTS surreal_sync_track_changes() CASCADE",
            &[],
        )
        .await;

    // Drop the audit table if it exists
    let _ = client
        .execute("DROP TABLE IF EXISTS surreal_sync_changes CASCADE", &[])
        .await;

    // Also clean up any publications (for logical replication)
    let publications = client
        .query(
            "SELECT pubname FROM pg_publication WHERE pubname LIKE 'surreal_sync_%'",
            &[],
        )
        .await?;

    for pub_row in publications {
        let pub_name: String = pub_row.get(0);
        let _ = client
            .execute(&format!("DROP PUBLICATION IF EXISTS {pub_name}"), &[])
            .await;
    }

    // Clean up replication slots if any
    let slots = client
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'surreal_sync_%'",
            &[],
        )
        .await?;

    for slot_row in slots {
        let slot_name: String = slot_row.get(0);
        let _ = client
            .query(
                &format!("SELECT pg_drop_replication_slot('{slot_name}')"),
                &[],
            )
            .await;
    }

    Ok(())
}

/// Clean up specific test tables
///
/// Drops the specified tables if they exist.
pub async fn cleanup_test_tables(client: &Client, tables: &[&str]) -> Result<()> {
    for table in tables {
        client
            .execute(&format!("DROP TABLE IF EXISTS {table} CASCADE"), &[])
            .await?;
    }
    Ok(())
}

/// Clean up all tables with a given prefix
///
/// Useful for cleaning up all test-related tables that follow a naming convention.
pub async fn cleanup_tables_with_prefix(client: &Client, prefix: &str) -> Result<()> {
    let query = format!(
        "SELECT tablename FROM pg_tables
         WHERE schemaname = 'public'
         AND tablename LIKE '{prefix}%'"
    );

    let rows = client.query(&query, &[]).await?;

    for row in rows {
        let table_name: String = row.get(0);
        let _ = client
            .execute(&format!("DROP TABLE IF EXISTS {table_name} CASCADE"), &[])
            .await;
    }

    Ok(())
}

/// Complete cleanup for PostgreSQL tests
///
/// This performs a full cleanup:
/// 1. Drops all triggers and the tracking function
/// 2. Drops the audit table
/// 3. Drops specified test tables
/// 4. Cleans up publications and replication slots
pub async fn full_cleanup(client: &Client, test_tables: &[&str]) -> Result<()> {
    // First clean up triggers and audit table
    cleanup_triggers_and_audit(client).await?;

    // Then clean up test tables
    cleanup_test_tables(client, test_tables).await?;

    Ok(())
}

/// Clean up all unified test dataset tables
///
/// Specifically cleans up tables used by the unified test dataset
pub async fn cleanup_unified_dataset_tables(client: &Client) -> Result<()> {
    let unified_tables = [
        "all_types_users",
        "all_types_posts",
        "test_all_types_users",
        "test_all_types_posts",
        "users",
        "posts",
        "test_users",
        "test_products",
        "authored_by",
    ];

    cleanup_test_tables(client, &unified_tables).await?;
    cleanup_triggers_and_audit(client).await?;

    Ok(())
}
