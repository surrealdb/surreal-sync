//! PostgreSQL logical replication client and slot
//!
//! This module provides Client and Slot structs for managing PostgreSQL
//! logical replication using regular SQL connections and wal2json.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::sync::Arc;
use tokio_postgres::Client as PgClient;
use tracing::{debug, info};

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

    /// Gets the current WAL LSN position
    ///
    /// This is useful for capturing the LSN position before taking a snapshot,
    /// so you can later replay all changes from that point onwards. This ensures
    /// consistency when the snapshot might be taken at a lower isolation level.
    ///
    /// # Returns
    /// * `Result<String>` - The current WAL LSN as a string (e.g., "0/1949850")
    ///
    /// # Example
    /// ```ignore
    /// // Capture LSN before taking snapshot
    /// let start_lsn = client.get_current_wal_lsn().await?;
    ///
    /// // Take your snapshot here
    /// take_database_snapshot().await?;
    ///
    /// // Later, replay changes from the captured LSN
    /// // using pg_logical_slot_peek_changes with the start_lsn
    /// ```
    pub async fn get_current_wal_lsn(&self) -> Result<String> {
        info!("Getting current WAL LSN position");

        let query = "SELECT pg_current_wal_lsn()::text";
        let rows = self
            .pg_client
            .query(query, &[])
            .await
            .context("Failed to get current WAL LSN")?;

        if rows.is_empty() {
            bail!("No result returned from pg_current_wal_lsn()");
        }

        let lsn: String = rows[0]
            .try_get(0)
            .context("Failed to extract LSN from query result")?;

        info!("Current WAL LSN: {}", lsn);
        Ok(lsn)
    }

    /// Creates a logical replication slot if it doesn't exist
    ///
    /// # Arguments
    /// * `slot_name` - Name for the replication slot
    ///
    /// # Returns
    /// * `Result<()>` - Ok if slot exists or was created successfully
    pub async fn create_slot(&self, slot_name: &str) -> Result<()> {
        info!("Checking logical replication slot: {}", slot_name);

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
            let create_slot_query =
                format!("SELECT pg_create_logical_replication_slot('{slot_name}', 'wal2json')",);
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

        Ok(())
    }

    /// Drops a replication slot
    ///
    /// Cleans up the replication slot when it's no longer needed.
    ///
    /// # Arguments
    /// * `slot_name` - Name of the replication slot to drop
    ///
    /// # Returns
    /// * `Result<()>` - Ok if slot was dropped successfully
    pub async fn drop_slot(&self, slot_name: &str) -> Result<()> {
        info!("Dropping replication slot: {}", slot_name);
        let query = format!("SELECT pg_drop_replication_slot('{slot_name}')");
        self.pg_client
            .execute(&query, &[])
            .await
            .context("Failed to drop replication slot")?;
        info!("Successfully dropped replication slot: {}", slot_name);
        Ok(())
    }

    /// Starts logical replication and returns a Slot
    ///
    /// Returns a Slot for consuming changes from an existing replication slot.
    /// The slot must already exist - use `create_slot()` to create one if needed,
    /// or create it manually using PostgreSQL commands.
    ///
    /// # Arguments
    /// * `slot_name` - Name for the replication slot (default: "test_slot")
    ///
    /// # Returns
    /// * `Result<Slot>` - A Slot for consuming replication changes
    ///
    /// # Example
    /// ```ignore
    /// // Option 1: Create slot using this library
    /// client.create_slot("my_slot").await?;
    /// let slot = client.start_replication(Some("my_slot")).await?;
    ///
    /// // Option 2: Assume slot was created manually via SQL
    /// // SELECT pg_create_logical_replication_slot('my_slot', 'wal2json');
    /// let slot = client.start_replication(Some("my_slot")).await?;
    /// ```
    pub async fn start_replication(&self, slot_name: Option<&str>) -> Result<Slot> {
        let slot_name = slot_name.unwrap_or("test_slot");

        info!("Starting logical replication with slot: {}", slot_name);

        // Create and return a Slot for the existing slot
        Ok(Slot::new(
            Arc::clone(&self.pg_client),
            slot_name.to_string(),
            self.table_names.clone(),
        ))
    }
}

/// Slot for consuming logical replication changes
///
/// Provides an interface for reading changes from a PostgreSQL
/// logical replication slot using pg_logical_slot_peek_changes and
/// pg_logical_slot_advance.
pub struct Slot {
    /// The PostgreSQL client connection
    pg_client: Arc<PgClient>,
    /// Name of the replication slot
    slot_name: String,
    /// List of table names to filter (if needed)
    table_names: Vec<String>,
}

impl Slot {
    /// Creates a new Slot instance
    fn new(pg_client: Arc<PgClient>, slot_name: String, table_names: Vec<String>) -> Self {
        Self {
            pg_client,
            slot_name,
            table_names,
        }
    }

    /// Advances the replication slot to the specified LSN
    ///
    /// This consumes all changes up to and including the specified LSN.
    /// Should only be called after successfully processing changes.
    ///
    /// # Arguments
    /// * `lsn` - The Log Sequence Number to advance to
    ///
    /// # Returns
    /// * `Result<()>` - Ok if advancement was successful
    ///
    /// # Example
    /// ```ignore
    /// // Peek at available changes
    /// let changes = slot.peek().await?;
    ///
    /// if !changes.is_empty() {
    ///     // Process all changes in the batch
    ///     for (lsn, change) in &changes {
    ///         handle_change(&change)?;
    ///     }
    ///
    ///     // Advance to the last LSN after all changes are processed
    ///     let last_lsn = &changes.last().unwrap().0;
    ///     slot.advance(last_lsn).await?;
    /// }
    /// ```
    pub async fn advance(&self, lsn: &str) -> Result<()> {
        info!(
            "Advancing replication slot {} to LSN: {}",
            self.slot_name, lsn
        );

        // Use pg_replication_slot_advance to move the slot forward
        // This is more efficient than consuming with pg_logical_slot_get_changes
        let query = format!(
            "SELECT pg_replication_slot_advance('{}', '{}')",
            self.slot_name, lsn
        );

        self.pg_client
            .execute(&query, &[])
            .await
            .context("Failed to advance replication slot")?;

        debug!("Successfully advanced slot to LSN: {}", lsn);
        Ok(())
    }

    /// Peek at changes without consuming them
    ///
    /// Uses pg_logical_slot_peek_changes to look at changes without consuming them.
    /// Returns a tuple containing the changes and the nextlsn to use for advancement.
    ///
    /// # Returns
    /// * `Result<(Vec<Value>, String)>` - Tuple of (changes, nextlsn)
    ///   - changes: Vector of change JSON objects (excluding transaction begin/commit)
    ///   - nextlsn: The nextlsn from the last commit action, to be used with advance()
    ///
    /// # Usage Pattern (batch processing with at-least-once delivery)
    /// ```ignore
    /// loop {
    ///     // 1. Peek at available changes
    ///     let (changes, nextlsn) = slot.peek().await?;
    ///
    ///     if changes.is_empty() {
    ///         break; // No more changes
    ///     }
    ///
    ///     // 2. Process all changes in the batch
    ///     for change in &changes {
    ///         process_change(&change)?;
    ///     }
    ///
    ///     // 3. Advance to nextlsn after successful processing
    ///     slot.advance(&nextlsn).await?;
    /// }
    /// ```
    pub async fn peek(&self) -> Result<(Vec<Value>, String)> {
        // wal2json options for formatting
        //
        // 'format-version', '2' - use format version 2
        // 'include-lsn', 'true' - include LSN and nextlsn fields in the output
        // 'include-pk', 'true' - add primary key information as pk. Column name and data type is included
        let wal2json_options = "'format-version', '2', 'include-lsn', 'true', 'include-pk', 'true'";

        let query = format!(
            "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_changes('{}', NULL, NULL, {})",
            self.slot_name, wal2json_options
        );

        let rows = self
            .pg_client
            .query(&query, &[])
            .await
            .context("Failed to peek changes from replication slot")?;

        let mut changes = Vec::new();
        let mut last_nextlsn = String::new();
        let mut current_xid: Option<String> = None;

        for row in rows {
            let _lsn: String = row.get(0);
            let xid: String = row.get(1);
            let data: String = row.get(2);

            match parse_wal2json(&data) {
                Ok(parsed) => {
                    // Check the action type
                    if let Some(action) = parsed.get("action").and_then(|a| a.as_str()) {
                        match action {
                            "B" => {
                                // Begin transaction
                                if current_xid.is_some() {
                                    bail!("Found Begin transaction xid={} while previous transaction xid={} is not committed",
                                          xid, current_xid.as_ref().unwrap());
                                }
                                current_xid = Some(xid.clone());
                                debug!("Begin transaction xid={}", xid);
                            }
                            "C" => {
                                // Commit transaction - extract nextlsn
                                if let Some(ref expected_xid) = current_xid {
                                    if expected_xid != &xid {
                                        bail!("Transaction xid mismatch: expected {expected_xid} but got {xid} in Commit");
                                    }
                                } else {
                                    bail!("Found Commit for xid={xid} without corresponding Begin");
                                }

                                if let Some(nextlsn) =
                                    parsed.get("nextlsn").and_then(|n| n.as_str())
                                {
                                    last_nextlsn = nextlsn.to_string();
                                    debug!("Commit transaction xid={xid} with nextlsn: {nextlsn}");
                                }
                                current_xid = None;
                            }
                            "I" | "U" | "D" => {
                                // Insert, Update, or Delete - actual data changes
                                // Validate transaction consistency
                                if let Some(ref expected_xid) = current_xid {
                                    if expected_xid != &xid {
                                        bail!("Transaction xid mismatch: expected {expected_xid} but got {xid} in {action} action");
                                    }
                                } else {
                                    bail!("Found {action} action with xid={xid} outside of transaction");
                                }

                                // Only include changes if they match our table filter (if any)
                                if !self.table_names.is_empty() {
                                    if let Some(table) =
                                        parsed.get("table").and_then(|t| t.as_str())
                                    {
                                        if self.table_names.iter().any(|t| t == table) {
                                            changes.push(parsed);
                                        }
                                    }
                                } else {
                                    changes.push(parsed);
                                }
                            }
                            _ => {
                                bail!("Unknown action type '{action}' in wal2json data: {data}");
                            }
                        }
                    }
                }
                Err(e) => {
                    bail!("Failed to parse wal2json data, due to {e}: {data}");
                }
            }
        }

        // If we have no nextlsn, it means no complete transactions were found
        if last_nextlsn.is_empty() && !changes.is_empty() {
            bail!("Found changes but no complete transaction (missing COMMIT). This may indicate a long-running transaction.");
        }

        Ok((changes, last_nextlsn))
    }
}
