//! Sync phase enumeration for checkpoint tracking.

use serde::{Deserialize, Serialize};

/// Represents different phases of the synchronization process.
///
/// Checkpoints are emitted at specific phases during sync operations
/// to enable:
/// - Resumable synchronization (start from last known position)
/// - Incremental sync coordination (use t1/t2 checkpoints)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncPhase {
    /// Checkpoint emitted before full sync begins (t1).
    ///
    /// This checkpoint captures the database state at the moment
    /// before data migration starts. It is used to:
    /// - Resume incremental sync from this point
    /// - Replay changes that occurred during full sync
    FullSyncStart,

    /// Checkpoint emitted after full sync completes (t2).
    ///
    /// This checkpoint captures the database state at the moment
    /// after data migration completes. It marks the point where
    /// incremental sync should stop replaying and switch to live mode.
    FullSyncEnd,
}

impl SyncPhase {
    /// Get the string representation of this phase.
    ///
    /// Used for:
    /// - Checkpoint file naming (e.g., `checkpoint_full_sync_start_2024-01-01.json`)
    /// - Logging and debugging output
    pub fn as_str(&self) -> &str {
        match self {
            SyncPhase::FullSyncStart => "full_sync_start",
            SyncPhase::FullSyncEnd => "full_sync_end",
        }
    }
}

impl std::fmt::Display for SyncPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
