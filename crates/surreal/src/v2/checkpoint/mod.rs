//! SurrealDB v2 SDK checkpoint storage implementation.

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
pub use surreal_sync_core::{CheckpointID, CheckpointStore, StoredCheckpoint, SyncManager};
use surrealdb2::engine::any::Any;
use surrealdb2::sql::{Id, Thing};

/// SurrealDB v2 SDK implementation of CheckpointStore trait.
///
/// Stores checkpoints in a SurrealDB table using the v2 SDK.
pub struct Surreal2Store {
    client: surrealdb2::Surreal<Any>,
    table_name: String,
}

impl Surreal2Store {
    /// Create a new Surreal2Store with the given client and table name.
    pub fn new(client: surrealdb2::Surreal<Any>, table_name: String) -> Self {
        Self { client, table_name }
    }

    /// Convert a CheckpointID to a SurrealDB Thing (record ID).
    fn to_thing(&self, id: &CheckpointID) -> Thing {
        let id_str = format!("{}_{}", id.database_type.replace('-', "_"), id.phase);
        Thing::from((self.table_name.as_str(), Id::String(id_str)))
    }
}

#[async_trait]
impl CheckpointStore for Surreal2Store {
    async fn store_checkpoint(&self, id: &CheckpointID, checkpoint_data: String) -> Result<()> {
        let thing = self.to_thing(id);
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

    async fn read_checkpoint(&self, id: &CheckpointID) -> Result<Option<StoredCheckpoint>> {
        let thing = self.to_thing(id);
        let mut response = self
            .client
            .query("SELECT * FROM $record_id")
            .bind(("record_id", thing))
            .await?;
        let checkpoints: Vec<StoredCheckpoint> = response.take(0)?;
        Ok(checkpoints.into_iter().next())
    }
}
