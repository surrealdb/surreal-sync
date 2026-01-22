//! SurrealDB checkpoint storage backend
//!
//! This module provides checkpoint storage in SurrealDB for better Kubernetes compatibility.
//! Unlike filesystem storage, SurrealDB storage doesn't require shared volumes or PV/PVC.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::Any;
use surrealdb::sql::{Id, Thing};

/// Checkpoint identifier for SurrealDB storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointID {
    /// Database type (e.g., "postgresql-wal2json", "mysql", "mongodb")
    pub database_type: String,
    /// Sync phase ("full_sync_start" or "full_sync_end")
    pub phase: String,
}

impl CheckpointID {
    /// Convert to SurrealDB Thing (record ID)
    ///
    /// Creates a record ID in the format: `{table_name}:postgresql_wal2json_full_sync_start`
    pub fn to_thing(&self, table_name: &str) -> Thing {
        let id_str = format!("{}_{}", self.database_type.replace('-', "_"), self.phase);
        Thing::from((table_name, Id::String(id_str)))
    }
}

/// Checkpoint data stored in SurrealDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCheckpoint {
    /// Serialized checkpoint (e.g., LSN for PostgreSQL, resume token for MongoDB)
    pub checkpoint_data: String,
    /// Database type for validation
    pub database_type: String,
    /// Sync phase for validation
    pub phase: String,
    /// Timestamp when checkpoint was created
    pub created_at: DateTime<Utc>,
}

/// Store for managing checkpoints in SurrealDB
pub struct CheckpointStore {
    client: surrealdb::Surreal<Any>,
    table_name: String,
}

impl CheckpointStore {
    /// Create a new checkpoint store
    ///
    /// # Arguments
    /// * `client` - SurrealDB client (already connected to target namespace/database)
    /// * `table_name` - Table name for storing checkpoints (e.g., "surreal_sync_checkpoints")
    pub fn new(client: surrealdb::Surreal<Any>, table_name: String) -> Self {
        Self { client, table_name }
    }

    /// Store a checkpoint in SurrealDB
    ///
    /// Uses UPSERT to create or replace the checkpoint record.
    pub async fn store_checkpoint(&self, id: &CheckpointID, checkpoint_data: String) -> Result<()> {
        let thing = id.to_thing(&self.table_name);
        let stored = StoredCheckpoint {
            checkpoint_data,
            database_type: id.database_type.clone(),
            phase: id.phase.clone(),
            created_at: Utc::now(),
        };

        self.client
            .query("UPSERT $record_id CONTENT $content")
            .bind(("record_id", thing))
            .bind(("content", stored))
            .await?;

        Ok(())
    }

    /// Read a checkpoint from SurrealDB
    ///
    /// Returns None if the checkpoint doesn't exist.
    pub async fn read_checkpoint(&self, id: &CheckpointID) -> Result<Option<StoredCheckpoint>> {
        let thing = id.to_thing(&self.table_name);

        let mut response = self
            .client
            .query("SELECT * FROM $record_id")
            .bind(("record_id", thing))
            .await?;

        let checkpoints: Vec<StoredCheckpoint> = response.take(0)?;

        Ok(checkpoints.into_iter().next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_id_to_thing() {
        let id = CheckpointID {
            database_type: "postgresql-wal2json".to_string(),
            phase: "full_sync_start".to_string(),
        };

        let thing = id.to_thing("surreal_sync_checkpoints");

        assert_eq!(thing.tb, "surreal_sync_checkpoints");
        match &thing.id {
            Id::String(s) => {
                assert_eq!(s, "postgresql_wal2json_full_sync_start");
            }
            _ => panic!("Expected String ID"),
        }
    }

    #[test]
    fn test_checkpoint_id_to_thing_replaces_hyphens() {
        let id = CheckpointID {
            database_type: "my-custom-source".to_string(),
            phase: "test_phase".to_string(),
        };

        let thing = id.to_thing("checkpoints");

        match &thing.id {
            Id::String(s) => {
                assert_eq!(s, "my_custom_source_test_phase");
            }
            _ => panic!("Expected String ID"),
        }
    }
}
