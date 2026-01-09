//! Checkpoint file wrapper for storage-agnostic serialization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Checkpoint, SyncPhase};

/// Storage-agnostic checkpoint file wrapper.
///
/// This struct wraps database-specific checkpoints with metadata
/// for storage and retrieval. The format is designed to be:
/// - Self-describing (includes `database_type` field)
/// - Extensible (uses JSON Value for checkpoint data)
/// - Storage-agnostic (can be saved to local fs, remote storage, etc.)
///
/// # File Format
///
/// ```json
/// {
///     "database_type": "mongodb",
///     "checkpoint": {
///         "resume_token": [1, 2, 3],
///         "timestamp": "2024-01-01T00:00:00Z"
///     },
///     "phase": "FullSyncStart",
///     "created_at": "2024-01-01T00:00:00Z"
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointFile {
    /// Database type identifier (e.g., "mongodb", "neo4j")
    pub database_type: String,
    /// Serialized checkpoint data as JSON Value
    pub checkpoint: serde_json::Value,
    /// Sync phase when this checkpoint was created
    pub phase: SyncPhase,
    /// Timestamp when this checkpoint file was created
    pub created_at: DateTime<Utc>,
}

impl CheckpointFile {
    /// Create new checkpoint file from database-specific checkpoint.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Database-specific checkpoint implementing `Checkpoint` trait
    /// * `phase` - Sync phase (FullSyncStart, FullSyncEnd)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let checkpoint = MongoDBCheckpoint { ... };
    /// let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncStart)?;
    /// ```
    pub fn new<C: Checkpoint>(checkpoint: &C, phase: SyncPhase) -> anyhow::Result<Self> {
        Ok(Self {
            database_type: C::DATABASE_TYPE.to_string(),
            checkpoint: serde_json::to_value(checkpoint)?,
            phase,
            created_at: Utc::now(),
        })
    }

    /// Parse checkpoint into database-specific type.
    ///
    /// Validates that the stored `database_type` matches the expected type `C`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `database_type` doesn't match `C::DATABASE_TYPE`
    /// - The checkpoint data can't be deserialized into type `C`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let file: CheckpointFile = /* loaded from file */;
    /// let checkpoint: MongoDBCheckpoint = file.parse()?;
    /// ```
    pub fn parse<C: Checkpoint>(&self) -> anyhow::Result<C> {
        if self.database_type != C::DATABASE_TYPE {
            anyhow::bail!(
                "Checkpoint type mismatch: expected '{}', found '{}'",
                C::DATABASE_TYPE,
                self.database_type
            );
        }
        Ok(serde_json::from_value(self.checkpoint.clone())?)
    }

    /// Get the database type of this checkpoint file.
    pub fn database_type(&self) -> &str {
        &self.database_type
    }

    /// Get the phase when this checkpoint was created.
    pub fn phase(&self) -> &SyncPhase {
        &self.phase
    }

    /// Get the timestamp when this checkpoint file was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
