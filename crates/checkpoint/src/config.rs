//! Sync configuration for checkpoint operations.

/// Configuration for sync operations.
///
/// Controls checkpoint emission and storage behavior.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Whether to run in incremental mode.
    ///
    /// When `true`, the sync operation will:
    /// - Read from a starting checkpoint
    /// - Process only changes since that checkpoint
    pub incremental: bool,

    /// Whether to emit checkpoints during sync.
    ///
    /// When `true`, checkpoints are written to disk at:
    /// - Start of full sync (t1)
    /// - End of full sync (t2)
    pub emit_checkpoints: bool,

    /// Path to write checkpoint files.
    ///
    /// Checkpoint files are named with the pattern:
    /// `checkpoint_{phase}_{timestamp}.json`
    ///
    /// If `None`, checkpoint emission is disabled even if `emit_checkpoints` is `true`.
    pub checkpoint_dir: Option<String>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_dir: Some(".surreal-sync-checkpoints".to_string()),
        }
    }
}

impl SyncConfig {
    /// Create a new sync config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config for full sync with checkpoint emission.
    pub fn full_sync_with_checkpoints(checkpoint_dir: String) -> Self {
        Self {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_dir: Some(checkpoint_dir),
        }
    }

    /// Create a config for incremental sync.
    pub fn incremental() -> Self {
        Self {
            incremental: true,
            emit_checkpoints: false,
            checkpoint_dir: None,
        }
    }

    /// Check if checkpoint emission is enabled and configured.
    pub fn should_emit_checkpoints(&self) -> bool {
        self.emit_checkpoints && self.checkpoint_dir.is_some()
    }
}
