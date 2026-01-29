//! SurrealDB checkpoint storage trait and types
//!
//! This module defines the CheckpointStore trait for version-agnostic
//! checkpoint storage operations, plus shared types.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Checkpoint identifier for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointID {
    /// Database type (e.g., "postgresql-wal2json", "mysql", "mongodb")
    pub database_type: String,
    /// Sync phase ("full_sync_start" or "full_sync_end")
    pub phase: String,
}

/// Checkpoint data stored in backend
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

/// Trait for checkpoint storage operations.
///
/// This trait abstracts the storage backend for checkpoint operations,
/// allowing the same checkpoint logic to work with:
/// - Filesystem storage (`FilesystemStore`)
/// - SurrealDB v2 (`Surreal2Store`)
/// - SurrealDB v3 (`Surreal3Store` in checkpoint-surreal3 crate)
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Store a checkpoint in the storage backend.
    async fn store_checkpoint(&self, id: &CheckpointID, checkpoint_data: String) -> Result<()>;

    /// Read a checkpoint from the storage backend.
    ///
    /// Returns None if the checkpoint doesn't exist.
    async fn read_checkpoint(&self, id: &CheckpointID) -> Result<Option<StoredCheckpoint>>;
}
