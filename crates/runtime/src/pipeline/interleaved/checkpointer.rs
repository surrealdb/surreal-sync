//! Persisting resumable snapshot progress.
//!
//! The watermark loop is decoupled from any concrete storage: it builds a
//! [`InterleavedSnapshotCheckpoint`] after each chunk and hands it to a
//! [`SnapshotCheckpointer`]. A no-op implementation is provided for tests, and
//! [`ManagerCheckpointer`] bridges to the shared [`SyncManager`] /
//! [`CheckpointStore`] JSON-blob storage.

use anyhow::Result;
use surreal_sync_core::{CheckpointStore, InterleavedSnapshotCheckpoint, SyncManager, SyncPhase};

pub use surreal_sync_core::SnapshotCheckpointer;

/// A checkpointer that discards progress. Useful for tests and one-shot runs
/// where resumability is not required.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopCheckpointer;

#[async_trait::async_trait]
impl SnapshotCheckpointer for NoopCheckpointer {
    async fn save_progress(&mut self, _checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()> {
        Ok(())
    }
}

/// Persists snapshot checkpoints through the shared [`SyncManager`], reusing
/// any [`CheckpointStore`] backend (filesystem, SurrealDB, ...).
pub struct ManagerCheckpointer<S: CheckpointStore> {
    manager: SyncManager<S>,
}

impl<S: CheckpointStore> ManagerCheckpointer<S> {
    /// Wrap a sync manager.
    pub fn new(manager: SyncManager<S>) -> Self {
        Self { manager }
    }

    /// Persist the final checkpoint under the handoff phase, recording the
    /// position downstream incremental/live processing should resume from.
    pub async fn save_handoff(&self, checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()> {
        self.manager
            .emit_checkpoint(checkpoint, SyncPhase::SnapshotHandoff)
            .await
    }

    /// Access the underlying sync manager.
    pub fn manager(&self) -> &SyncManager<S> {
        &self.manager
    }
}

#[async_trait::async_trait]
impl<S: CheckpointStore> SnapshotCheckpointer for ManagerCheckpointer<S> {
    async fn save_progress(&mut self, checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()> {
        self.manager
            .emit_checkpoint(checkpoint, SyncPhase::SnapshotProgress)
            .await
    }
}
