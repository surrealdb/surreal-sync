//! State management for PostgreSQL logical decoding sync
//!
//! This module provides a durable state machine backed by SurrealDB to track
//! the progress of PostgreSQL logical decoding synchronization. The state machine
//! ensures we don't repeat the initial full snapshot sync on restarts and properly
//! tracks the LSN position for continuing logical replication from where the initial
//! snapshotting started.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use surrealdb::sql::Thing;

/// Unique identifier for a PostgreSQL replication state
/// Used as the record ID in SurrealDB's surreal_sync_postgresql table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StateID {
    /// PostgreSQL host (e.g., "localhost", "db.example.com")
    pub host: String,
    /// PostgreSQL schema (e.g., "public")
    pub schema: String,
    /// Replication slot name
    pub slot: String,
}

impl StateID {
    /// Create a new StateID
    pub fn new(host: String, schema: String, slot: String) -> Self {
        Self { host, schema, slot }
    }

    /// Convert to a SurrealDB Thing (record ID)
    /// Format: surreal_sync_postgresql:host_schema_slot
    pub fn to_thing(&self) -> Thing {
        // Create a composite ID string that's safe for SurrealDB
        let id = format!(
            "{}_{}_{}",
            self.host.replace(['.', ':'], "_"),
            self.schema,
            self.slot
        );
        Thing::from(("surreal_sync_postgresql", id.as_str()))
    }

    /// Parse from a connection string and extract the host
    pub fn from_connection_and_slot(
        connection_string: &str,
        schema: &str,
        slot: &str,
    ) -> Result<Self> {
        // Parse the connection string to extract the host
        // Connection strings can be in various formats:
        // - "host=localhost port=5432 ..."
        // - "postgresql://user:pass@host:port/db"

        let host = if connection_string.starts_with("postgresql://")
            || connection_string.starts_with("postgres://")
        {
            // URL format
            let url = connection_string
                .trim_start_matches("postgresql://")
                .trim_start_matches("postgres://");

            // Extract host from user:pass@host:port/db format
            let host_part = url
                .split('/')
                .next()
                .and_then(|s| s.split('@').next_back())
                .and_then(|s| s.split(':').next())
                .unwrap_or("localhost");

            host_part.to_string()
        } else {
            // Key-value format
            connection_string
                .split_whitespace()
                .find(|s| s.starts_with("host="))
                .and_then(|s| s.strip_prefix("host="))
                .unwrap_or("localhost")
                .to_string()
        };

        Ok(Self::new(host, schema.to_string(), slot.to_string()))
    }
}

/// State of the PostgreSQL replication process
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum State {
    /// Initial state when setting up a new replication slot
    Pending,

    /// Full snapshot sync phase with the LSN position before starting
    /// This LSN is needed to know where to start logical decoding from
    Initial { pre_lsn: String },

    /// Incremental logical replication phase
    /// The slot itself maintains the current LSN position
    Incremental,
}

/// State record stored in SurrealDB
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateRecord {
    /// The state ID components
    id: StateID,
    /// Current state of the replication
    state: State,
    /// Timestamp of last state transition
    updated_at: chrono::DateTime<chrono::Utc>,
}

/// Durable state store backed by SurrealDB
pub struct Store {
    /// SurrealDB client connection
    surreal: surrealdb::Surreal<surrealdb::engine::any::Any>,
}

impl Store {
    /// Create a new state store
    pub fn new(surreal: surrealdb::Surreal<surrealdb::engine::any::Any>) -> Self {
        Self { surreal }
    }

    /// Transition to a new state
    ///
    /// # State Transitions:
    /// - `Pending`: Called when starting a brand-new replication slot
    /// - `Initial(pre_lsn)`: Called AFTER getting current LSN, BEFORE full snapshot
    /// - `Incremental`: Called AFTER snapshot completion, BEFORE logical decoding starts
    pub async fn transition(&self, id: &StateID, to_state: State) -> Result<()> {
        // Validate state transition
        if let Some(current) = self.get_state(id).await? {
            self.validate_transition(&current, &to_state)?;
        }

        let record = StateRecord {
            id: id.clone(),
            state: to_state.clone(),
            updated_at: chrono::Utc::now(),
        };

        // Store the state in SurrealDB
        let thing = id.to_thing();
        let query = "UPDATE $record_id CONTENT $content";

        self.surreal
            .query(query)
            .bind(("record_id", thing))
            .bind(("content", record))
            .await
            .context("Failed to update state in SurrealDB")?;

        tracing::info!("State transitioned for {:?} to {:?}", id, to_state);

        Ok(())
    }

    /// Get the current state for a given ID
    pub async fn get_state(&self, id: &StateID) -> Result<Option<State>> {
        let thing = id.to_thing();
        let query = "SELECT state FROM $record_id";

        let mut result = self
            .surreal
            .query(query)
            .bind(("record_id", thing))
            .await
            .context("Failed to query state from SurrealDB")?;

        // Extract the state from the result
        let states: Vec<State> = result.take((0, "state")).unwrap_or_else(|_| Vec::new());

        Ok(states.into_iter().next())
    }

    /// Remove a state record (useful for cleanup)
    #[allow(dead_code)]
    pub async fn remove(&self, id: &StateID) -> Result<()> {
        let thing = id.to_thing();
        let query = "DELETE $record_id";

        self.surreal
            .query(query)
            .bind(("record_id", thing))
            .await
            .context("Failed to delete state from SurrealDB")?;

        tracing::info!("State removed for {:?}", id);

        Ok(())
    }

    /// Get all state records (useful for monitoring/debugging)
    #[allow(dead_code)]
    pub async fn list_all(&self) -> Result<Vec<(StateID, State)>> {
        let query = "SELECT id, state FROM surreal_sync_postgresql";

        let mut result = self
            .surreal
            .query(query)
            .await
            .context("Failed to list all states from SurrealDB")?;

        let ids: Vec<StateID> = result.take((0, "id")).unwrap_or_else(|_| Vec::new());

        let states: Vec<State> = result.take((0, "state")).unwrap_or_else(|_| Vec::new());

        Ok(ids.into_iter().zip(states.into_iter()).collect())
    }

    /// Validate state transitions
    fn validate_transition(&self, from: &State, to: &State) -> Result<()> {
        match (from, to) {
            // Valid transitions
            (State::Pending, State::Initial { .. }) => Ok(()),
            (State::Initial { .. }, State::Incremental) => Ok(()),

            // Allow re-initialization (useful for recovery scenarios)
            (State::Incremental, State::Pending) => {
                tracing::warn!("Resetting from Incremental to Pending - full resync will occur");
                Ok(())
            }

            // Invalid transitions
            _ => Err(anyhow::anyhow!(
                "Invalid state transition from {from:?} to {to:?}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_id_creation() {
        let id = StateID::new(
            "localhost".to_string(),
            "public".to_string(),
            "test_slot".to_string(),
        );

        assert_eq!(id.host, "localhost");
        assert_eq!(id.schema, "public");
        assert_eq!(id.slot, "test_slot");
    }

    #[test]
    fn test_state_id_to_thing() {
        let id = StateID::new(
            "db.example.com".to_string(),
            "public".to_string(),
            "sync_slot".to_string(),
        );

        let thing = id.to_thing();
        assert_eq!(thing.tb, "surreal_sync_postgresql");
        assert_eq!(thing.id.to_string(), "db_example_com_public_sync_slot");
    }

    #[test]
    fn test_state_id_from_connection_string_kv_format() {
        let conn_str = "host=db.example.com port=5432 user=postgres";
        let id = StateID::from_connection_and_slot(conn_str, "public", "test_slot").unwrap();

        assert_eq!(id.host, "db.example.com");
        assert_eq!(id.schema, "public");
        assert_eq!(id.slot, "test_slot");
    }

    #[test]
    fn test_state_id_from_connection_string_url_format() {
        let conn_str = "postgresql://user:pass@db.example.com:5432/mydb";
        let id = StateID::from_connection_and_slot(conn_str, "public", "test_slot").unwrap();

        assert_eq!(id.host, "db.example.com");
        assert_eq!(id.schema, "public");
        assert_eq!(id.slot, "test_slot");
    }

    #[test]
    fn test_state_serialization() {
        let state = State::Initial {
            pre_lsn: "0/1234567".to_string(),
        };

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: State = serde_json::from_str(&json).unwrap();

        assert_eq!(state, deserialized);
    }
}
