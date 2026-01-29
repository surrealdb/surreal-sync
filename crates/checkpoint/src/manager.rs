//! Generic sync manager for checkpoint operations.

use std::marker::PhantomData;

use crate::{store::CheckpointStore, Checkpoint, CheckpointFile, CheckpointID, SyncPhase};

/// Manager for handling sync operations with checkpoint tracking.
///
/// The `SyncManager` is generic over the checkpoint store type `S`,
/// providing storage-agnostic checkpoint management:
/// - **FilesystemStore**: Writes checkpoint files to disk
/// - **Surreal2Store**: Stores checkpoints in SurrealDB v2
/// - **Surreal3Store**: Stores checkpoints in SurrealDB v3 (in checkpoint-surreal3 crate)
///
/// # Example
///
/// ```rust,ignore
/// use checkpoint::{SyncManager, SyncPhase, FilesystemStore};
///
/// let store = FilesystemStore::new("/tmp/checkpoints");
/// let manager = SyncManager::new(store);
///
/// // Save a checkpoint
/// let checkpoint = MongoDBCheckpoint { ... };
/// manager.emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart).await?;
///
/// // Load a checkpoint
/// let loaded: MongoDBCheckpoint = manager.read_checkpoint(SyncPhase::FullSyncStart).await?;
/// ```
pub struct SyncManager<S: CheckpointStore> {
    store: S,
    emit_checkpoints: bool,
}

impl<S: CheckpointStore> SyncManager<S> {
    /// Create a new sync manager with the given store.
    ///
    /// By default, checkpoint emission is enabled.
    pub fn new(store: S) -> Self {
        Self {
            store,
            emit_checkpoints: true,
        }
    }

    /// Create a new sync manager with checkpoint emission disabled.
    ///
    /// Useful for incremental sync where you don't want to emit new checkpoints.
    pub fn new_without_emit(store: S) -> Self {
        Self {
            store,
            emit_checkpoints: false,
        }
    }

    /// Set whether to emit checkpoints.
    pub fn with_emit_checkpoints(mut self, emit: bool) -> Self {
        self.emit_checkpoints = emit;
        self
    }

    /// Check if checkpoint emission is enabled.
    pub fn emit_checkpoints(&self) -> bool {
        self.emit_checkpoints
    }

    /// Get a reference to the underlying store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Emit checkpoint for any database-specific checkpoint type.
    ///
    /// This is a **SAVING** operation - writes to the configured store.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Database-specific checkpoint (e.g., `MongoDBCheckpoint`)
    /// * `phase` - Sync phase when this checkpoint is being emitted
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `emit_checkpoints` is false (returns Ok without writing)
    /// - Failed to serialize checkpoint
    /// - Failed to write to store
    pub async fn emit_checkpoint<C: Checkpoint>(
        &self,
        checkpoint: &C,
        phase: SyncPhase,
    ) -> anyhow::Result<()> {
        if !self.emit_checkpoints {
            return Ok(());
        }

        let id = CheckpointID {
            database_type: C::DATABASE_TYPE.to_string(),
            phase: phase.as_str().to_string(),
        };

        let checkpoint_data = serde_json::to_string(checkpoint)?;
        self.store.store_checkpoint(&id, checkpoint_data).await?;

        tracing::info!(
            "Emitted {} checkpoint: {}",
            phase,
            checkpoint.to_cli_string()
        );

        Ok(())
    }

    /// Read and parse checkpoint into database-specific type.
    ///
    /// This is a **LOADING** operation - it reads from the store and parses
    /// into the specified checkpoint type.
    ///
    /// # Type Inference
    ///
    /// The checkpoint type `C` can be inferred from context:
    ///
    /// ```rust,ignore
    /// let checkpoint: MongoDBCheckpoint = manager.read_checkpoint(SyncPhase::FullSyncStart).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No checkpoint found for the phase
    /// - Failed to deserialize checkpoint data
    pub async fn read_checkpoint<C: Checkpoint>(&self, phase: SyncPhase) -> anyhow::Result<C> {
        let id = CheckpointID {
            database_type: C::DATABASE_TYPE.to_string(),
            phase: phase.as_str().to_string(),
        };

        let stored = self
            .store
            .read_checkpoint(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No checkpoint found for phase: {phase}"))?;

        // Validate database type matches
        if stored.database_type != C::DATABASE_TYPE {
            return Err(anyhow::anyhow!(
                "Checkpoint database type mismatch: expected '{}', found '{}'",
                C::DATABASE_TYPE,
                stored.database_type
            ));
        }

        Ok(serde_json::from_str(&stored.checkpoint_data)?)
    }
}

/// A no-op checkpoint store for when checkpoint storage is disabled.
///
/// All operations return successfully without storing anything.
pub struct NullStore;

#[async_trait::async_trait]
impl CheckpointStore for NullStore {
    async fn store_checkpoint(
        &self,
        _id: &CheckpointID,
        _checkpoint_data: String,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn read_checkpoint(
        &self,
        _id: &CheckpointID,
    ) -> anyhow::Result<Option<crate::StoredCheckpoint>> {
        Ok(None)
    }
}

/// Type alias for SyncManager with disabled checkpointing.
pub type NullSyncManager = SyncManager<NullStore>;

impl NullSyncManager {
    /// Create a sync manager that does nothing (for disabled checkpoint storage).
    pub fn disabled() -> Self {
        SyncManager::new_without_emit(NullStore)
    }
}

// ============================================================================
// Legacy compatibility: CheckpointFile-based operations
// ============================================================================

/// Manager for reading checkpoint files directly from filesystem.
///
/// This is kept for backward compatibility with code that reads
/// `CheckpointFile` structures directly.
pub struct CheckpointFileReader {
    dir: std::path::PathBuf,
    _marker: PhantomData<()>,
}

impl CheckpointFileReader {
    /// Create a new checkpoint file reader for the given directory.
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            _marker: PhantomData,
        }
    }

    /// Read the latest checkpoint file for a given phase.
    ///
    /// Scans the checkpoint directory for files matching the phase
    /// and returns the most recently modified one.
    pub fn read_latest_checkpoint(&self, phase: SyncPhase) -> anyhow::Result<CheckpointFile> {
        let mut latest_file = None;
        let mut latest_time = None;

        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(filename) = path.file_name() {
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with(&format!("checkpoint_{}_", phase.as_str())) {
                    let metadata = entry.metadata()?;
                    let modified = metadata.modified()?;
                    if latest_time.is_none() || modified > latest_time.unwrap() {
                        latest_time = Some(modified);
                        latest_file = Some(path);
                    }
                }
            }
        }

        let file_path =
            latest_file.ok_or_else(|| anyhow::anyhow!("No checkpoint found for phase: {phase}"))?;

        let content = std::fs::read_to_string(file_path)?;

        // Files are stored in StoredCheckpoint format, convert to CheckpointFile
        let stored: crate::StoredCheckpoint = serde_json::from_str(&content)?;
        let checkpoint: serde_json::Value = serde_json::from_str(&stored.checkpoint_data)?;
        let file = CheckpointFile {
            database_type: stored.database_type,
            checkpoint,
            phase,
            created_at: stored.created_at,
        };

        Ok(file)
    }

    /// Read and parse checkpoint into database-specific type.
    pub fn read_checkpoint<C: Checkpoint>(&self, phase: SyncPhase) -> anyhow::Result<C> {
        let file = self.read_latest_checkpoint(phase)?;
        file.parse::<C>()
    }
}
