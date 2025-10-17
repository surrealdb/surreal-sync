//! PostgreSQL logical replication client and stream
//!
//! This module provides Client and Stream structs for managing PostgreSQL
//! logical replication using regular SQL connections and wal2json.

use anyhow::{Context, Result};
use serde_json::Value;
use std::sync::Arc;
use tokio_postgres::Client as PgClient;
use tracing::{debug, info, warn};

use crate::wal2json::parse_wal2json;

/// Client for PostgreSQL logical replication
///
/// Manages the connection and configuration for logical replication
/// using regular SQL connections (not replication protocol).
pub struct Client {
    /// The PostgreSQL client connection
    pg_client: Arc<PgClient>,
    /// List of table names to track for replication
    table_names: Vec<String>,
}

impl Client {
    /// Creates a new logical replication client
    ///
    /// # Arguments
    /// * `pg_client` - A connected tokio_postgres Client
    /// * `table_names` - List of table names to track for replication
    pub fn new(pg_client: PgClient, table_names: Vec<String>) -> Self {
        Self {
            pg_client: Arc::new(pg_client),
            table_names,
        }
    }

    /// Starts logical replication and returns a Stream
    ///
    /// Creates a logical replication slot if it doesn't exist and
    /// returns a Stream for consuming changes.
    ///
    /// # Arguments
    /// * `slot_name` - Name for the replication slot (default: "test_slot")
    ///
    /// # Returns
    /// * `Result<Stream>` - A Stream for consuming replication changes
    pub async fn start_replication(&self, slot_name: Option<&str>) -> Result<Stream> {
        let slot_name = slot_name.unwrap_or("test_slot");

        info!("Starting logical replication with slot: {}", slot_name);

        // Check if the slot already exists
        let check_slot_query = "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1";
        let rows = self
            .pg_client
            .query(check_slot_query, &[&slot_name])
            .await
            .context("Failed to check for existing replication slot")?;

        if rows.is_empty() {
            // Create the logical replication slot with wal2json plugin
            info!("Creating new logical replication slot: {}", slot_name);
            let create_slot_query = format!(
                "SELECT * FROM pg_create_logical_replication_slot('{slot_name}', 'wal2json')",
            );
            self.pg_client
                .execute(&create_slot_query, &[])
                .await
                .context("Failed to create logical replication slot")?;
            info!("Successfully created replication slot: {}", slot_name);
        } else {
            info!("Replication slot already exists: {}", slot_name);
        }

        // List all replication slots for debugging
        let list_slots_query =
            "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots";
        let slots = self
            .pg_client
            .query(list_slots_query, &[])
            .await
            .context("Failed to list replication slots")?;

        for slot in &slots {
            let name: &str = slot.get(0);
            let plugin: &str = slot.get(1);
            let slot_type: &str = slot.get(2);
            let active: bool = slot.get(3);
            debug!(
                "Slot: {} | Plugin: {} | Type: {} | Active: {}",
                name, plugin, slot_type, active
            );
        }

        // Create and return a Stream
        Ok(Stream::new(
            Arc::clone(&self.pg_client),
            slot_name.to_string(),
            self.table_names.clone(),
        ))
    }
}

/// Stream for consuming logical replication changes
///
/// Provides an interface for reading changes from a PostgreSQL
/// logical replication slot using pg_logical_slot_get_changes.
pub struct Stream {
    /// The PostgreSQL client connection
    pg_client: Arc<PgClient>,
    /// Name of the replication slot
    slot_name: String,
    /// List of table names to filter (if needed)
    table_names: Vec<String>,
}

impl Stream {
    /// Creates a new Stream instance
    fn new(pg_client: Arc<PgClient>, slot_name: String, table_names: Vec<String>) -> Self {
        Self {
            pg_client,
            slot_name,
            table_names,
        }
    }

    /// Gets the next change from the replication stream
    ///
    /// Polls the logical replication slot for changes and returns
    /// the next available change as a parsed JSON object.
    ///
    /// # Returns
    /// * `Result<Option<Value>>` - The next change as JSON, or None if no changes available
    pub async fn next(&mut self) -> Result<Option<Value>> {
        // wal2json options for formatting
        //
        // 'format-version', '2' - use format version 2
        // 'include-lsn': 'true' - add nextlsn to each changeset
        // 'include-transaction': 'false' - do not emit records denoting the start and end of each transaction
        // 'include-pk': 'true' - add primary key information as pk. Column name and data type is included
        let wal2json_options = "'format-version', '2', 'include-lsn', 'true', 'include-transaction', 'false', 'include-pk', 'true'";

        // Query for changes using pg_logical_slot_get_changes
        // This function consumes changes (they won't be returned again)
        let query = format!(
            "SELECT lsn, data FROM pg_logical_slot_get_changes('{}', NULL, NULL, {wal2json_options})",
            self.slot_name
        );

        let rows = self
            .pg_client
            .query(&query, &[])
            .await
            .context("Failed to get changes from replication slot")?;

        if rows.is_empty() {
            debug!("No new changes available");
            return Ok(None);
        }

        // Process the first available change
        // In a real implementation, you might want to batch these
        for row in rows {
            let _lsn: String = row.get(0); // Log Sequence Number
            let data: String = row.get(1); // wal2json formatted data

            debug!("Received change with LSN: {}", _lsn);

            // Parse the wal2json data
            match parse_wal2json(&data) {
                Ok(parsed) => {
                    // Optional: Filter by table names if needed
                    if !self.table_names.is_empty() {
                        // Check if this change is for one of our tracked tables
                        // wal2json format includes table information in the change
                        if let Some(changes) = parsed.get("change") {
                            if let Some(change_array) = changes.as_array() {
                                for change in change_array {
                                    if let Some(table) = change.get("table") {
                                        if let Some(table_str) = table.as_str() {
                                            if self.table_names.iter().any(|t| t == table_str) {
                                                return Ok(Some(parsed));
                                            }
                                        }
                                    }
                                }
                                // Change is not for a tracked table, skip it
                                debug!("Skipping change for untracked table");
                                continue;
                            }
                        }
                    }

                    return Ok(Some(parsed));
                }
                Err(e) => {
                    warn!("Failed to parse wal2json data: {}", e);
                    warn!("Raw data: {}", data);
                    // Continue to next change instead of failing
                    continue;
                }
            }
        }

        Ok(None)
    }

    /// Peek at changes without consuming them
    ///
    /// Uses pg_logical_slot_peek_changes to look at changes without consuming them.
    /// Useful for debugging or preview purposes.
    pub async fn peek(&self) -> Result<Vec<Value>> {
        let query = format!(
            "SELECT lsn, data FROM pg_logical_slot_peek_changes('{}', NULL, NULL, 'format-version', '2', 'include-timestamp', 'true')",
            self.slot_name
        );

        let rows = self
            .pg_client
            .query(&query, &[])
            .await
            .context("Failed to peek changes from replication slot")?;

        let mut changes = Vec::new();
        for row in rows {
            let data: String = row.get(1);
            if let Ok(parsed) = parse_wal2json(&data) {
                changes.push(parsed);
            }
        }

        Ok(changes)
    }

    /// Drops the replication slot
    ///
    /// Cleans up the replication slot when it's no longer needed.
    /// This should be called when shutting down the replication stream.
    pub async fn drop_slot(&self) -> Result<()> {
        info!("Dropping replication slot: {}", self.slot_name);
        let query = format!("SELECT pg_drop_replication_slot('{}')", self.slot_name);
        self.pg_client
            .execute(&query, &[])
            .await
            .context("Failed to drop replication slot")?;
        info!("Successfully dropped replication slot");
        Ok(())
    }
}
