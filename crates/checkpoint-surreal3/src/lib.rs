//! SurrealDB v3 checkpoint storage implementation.
//!
//! This crate provides `Surreal3Store`, which implements the `CheckpointStore`
//! trait for SurrealDB v3 SDK. It enables checkpoint storage in SurrealDB v3
//! servers using the v3 SDK types (`RecordId`, `RecordIdKey`).

use anyhow::Result;
use async_trait::async_trait;
use checkpoint::{CheckpointID, CheckpointStore, StoredCheckpoint};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use surrealdb::engine::any::Any;
use surrealdb::types::{RecordId, RecordIdKey};

/// Internal struct for storing checkpoint data in SurrealDB v3.
///
/// This struct mirrors `StoredCheckpoint` but implements the traits
/// needed by SurrealDB v3 SDK for serialization/deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointRecord {
    checkpoint_data: String,
    database_type: String,
    phase: String,
    created_at: String, // Store as RFC3339 string for compatibility
}

impl From<&StoredCheckpoint> for CheckpointRecord {
    fn from(stored: &StoredCheckpoint) -> Self {
        Self {
            checkpoint_data: stored.checkpoint_data.clone(),
            database_type: stored.database_type.clone(),
            phase: stored.phase.clone(),
            created_at: stored.created_at.to_rfc3339(),
        }
    }
}

impl TryFrom<CheckpointRecord> for StoredCheckpoint {
    type Error = anyhow::Error;

    fn try_from(record: CheckpointRecord) -> Result<Self> {
        Ok(Self {
            checkpoint_data: record.checkpoint_data,
            database_type: record.database_type,
            phase: record.phase,
            created_at: chrono::DateTime::parse_from_rfc3339(&record.created_at)?
                .with_timezone(&Utc),
        })
    }
}

/// SurrealDB v3 SDK implementation of CheckpointStore trait.
///
/// Stores checkpoints in a SurrealDB table using the v3 SDK.
pub struct Surreal3Store {
    client: surrealdb::Surreal<Any>,
    table_name: String,
}

impl Surreal3Store {
    /// Create a new Surreal3Store with the given client and table name.
    pub fn new(client: surrealdb::Surreal<Any>, table_name: String) -> Self {
        Self { client, table_name }
    }

    /// Convert a CheckpointID to a SurrealDB v3 RecordId.
    fn to_record_id(&self, id: &CheckpointID) -> RecordId {
        let id_str = format!("{}_{}", id.database_type.replace('-', "_"), id.phase);
        RecordId::new(self.table_name.as_str(), RecordIdKey::String(id_str))
    }
}

#[async_trait]
impl CheckpointStore for Surreal3Store {
    async fn store_checkpoint(&self, id: &CheckpointID, checkpoint_data: String) -> Result<()> {
        let record_id = self.to_record_id(id);

        let stored = StoredCheckpoint {
            checkpoint_data,
            database_type: id.database_type.clone(),
            phase: id.phase.clone(),
            created_at: Utc::now(),
        };

        // Convert to CheckpointRecord and serialize to JSON Value for binding
        let record = CheckpointRecord::from(&stored);
        let json_value = serde_json::to_value(&record)?;

        // Use UPSERT with raw JSON content
        self.client
            .query("UPSERT $record_id CONTENT $content")
            .bind(("record_id", record_id))
            .bind(("content", json_value))
            .await?;

        tracing::info!(
            "Stored checkpoint in SurrealDB v3 table '{}': {} / {}",
            self.table_name,
            id.database_type,
            id.phase
        );

        Ok(())
    }

    async fn read_checkpoint(&self, id: &CheckpointID) -> Result<Option<StoredCheckpoint>> {
        let record_id = self.to_record_id(id);

        let mut response = self
            .client
            .query("SELECT * FROM $record_id")
            .bind(("record_id", record_id))
            .await?;

        // Take the result as serde_json::Value for easier handling
        let records: Vec<serde_json::Value> = response.take(0)?;

        if let Some(json_val) = records.into_iter().next() {
            let record: CheckpointRecord = serde_json::from_value(json_val)?;
            Ok(Some(record.try_into()?))
        } else {
            Ok(None)
        }
    }
}
