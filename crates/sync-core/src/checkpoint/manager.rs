//! Generic sync manager for checkpoint operations.

use crate::checkpoint::{
    store::CheckpointStore, Checkpoint, CheckpointID, StoredCheckpoint, SyncPhase,
};

/// Manager for handling sync operations with checkpoint tracking.
///
/// The `SyncManager` is generic over the checkpoint store type `S`,
/// providing storage-agnostic checkpoint management.
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
    ) -> anyhow::Result<Option<StoredCheckpoint>> {
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
