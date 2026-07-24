//! Sync configuration for checkpoint operations.

/// Checkpoint storage backend
#[derive(Debug, Clone)]
pub enum CheckpointStorage {
    /// No checkpoint storage
    Disabled,
    /// Filesystem storage at specified directory
    Filesystem { dir: String },
    /// SurrealDB storage in specified table
    SurrealDB {
        table_name: String,
        namespace: String,
        database: String,
    },
}

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
    /// When `true`, checkpoints are written according to `checkpoint_storage`.
    pub emit_checkpoints: bool,

    /// Checkpoint storage backend configuration
    pub checkpoint_storage: CheckpointStorage,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_storage: CheckpointStorage::Filesystem {
                dir: ".surreal-sync-checkpoints".to_string(),
            },
        }
    }
}

impl SyncConfig {
    /// Create a new sync config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config for full sync with filesystem checkpoint emission.
    pub fn full_sync_with_checkpoints(checkpoint_dir: String) -> Self {
        Self {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_storage: CheckpointStorage::Filesystem {
                dir: checkpoint_dir,
            },
        }
    }

    /// Create a config for full sync with SurrealDB checkpoint emission.
    pub fn full_sync_with_surrealdb_checkpoints(
        table_name: String,
        namespace: String,
        database: String,
    ) -> Self {
        Self {
            incremental: false,
            emit_checkpoints: true,
            checkpoint_storage: CheckpointStorage::SurrealDB {
                table_name,
                namespace,
                database,
            },
        }
    }

    /// Create a config for incremental sync.
    pub fn incremental() -> Self {
        Self {
            incremental: true,
            emit_checkpoints: false,
            checkpoint_storage: CheckpointStorage::Disabled,
        }
    }

    /// Check if checkpoint emission is enabled and configured.
    pub fn should_emit_checkpoints(&self) -> bool {
        self.emit_checkpoints && !matches!(self.checkpoint_storage, CheckpointStorage::Disabled)
    }
}
