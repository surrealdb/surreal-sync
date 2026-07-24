//! Trait for persisting resumable interleaved-snapshot progress.
//!
//! The watermark loop is decoupled from any concrete storage: it builds an
//! [`InterleavedSnapshotCheckpoint`] after each chunk and hands it to a
//! [`SnapshotCheckpointer`]. Concrete backends (no-op, manager bridge, …) live
//! outside this crate.

use super::InterleavedSnapshotCheckpoint;
use anyhow::Result;

/// Receives resumable snapshot checkpoints emitted per chunk.
#[async_trait::async_trait]
pub trait SnapshotCheckpointer: Send {
    /// Persist progress made so far. Called after each chunk is durably
    /// applied to the sink and before the corresponding change-log data is
    /// freed.
    async fn save_progress(&mut self, checkpoint: &InterleavedSnapshotCheckpoint) -> Result<()>;
}
