//! Generic sync manager for checkpoint operations.

use chrono::Utc;

use crate::{
    store::CheckpointStore, Checkpoint, CheckpointFile, CheckpointID, CheckpointStorage,
    SyncConfig, SyncPhase,
};

/// Manager for handling sync operations with checkpoint tracking.
///
/// The `SyncManager` provides storage-agnostic checkpoint management:
/// - **Filesystem**: Writes checkpoint files to disk
/// - **SurrealDB**: Stores checkpoints in SurrealDB table
///
/// # Example
///
/// ```rust,ignore
/// use checkpoint::{SyncConfig, SyncManager, SyncPhase};
///
/// let config = SyncConfig::full_sync_with_checkpoints("/tmp/checkpoints".into());
/// let manager = SyncManager::new(config, None);
///
/// // Save a checkpoint
/// let checkpoint = MongoDBCheckpoint { ... };
/// manager.emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart).await?;
///
/// // Load a checkpoint
/// let loaded: MongoDBCheckpoint = manager.read_checkpoint(SyncPhase::FullSyncStart).await?;
/// ```
pub struct SyncManager {
    config: SyncConfig,
    surreal_client: Option<surrealdb::Surreal<surrealdb::engine::any::Any>>,
}

impl SyncManager {
    /// Create a new sync manager with the given configuration.
    ///
    /// # Arguments
    /// * `config` - Sync configuration including checkpoint storage backend
    /// * `surreal_client` - Optional SurrealDB client (required if using SurrealDB storage)
    pub fn new(
        config: SyncConfig,
        surreal_client: Option<surrealdb::Surreal<surrealdb::engine::any::Any>>,
    ) -> Self {
        Self {
            config,
            surreal_client,
        }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Emit checkpoint for any database-specific checkpoint type.
    ///
    /// This is a **SAVING** operation - writes to filesystem or SurrealDB based on config.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Database-specific checkpoint (e.g., `MongoDBCheckpoint`)
    /// * `phase` - Sync phase when this checkpoint is being emitted
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `emit_checkpoints` is false in config
    /// - Storage backend not configured
    /// - Failed to write checkpoint
    pub async fn emit_checkpoint<C: Checkpoint>(
        &self,
        checkpoint: &C,
        phase: SyncPhase,
    ) -> anyhow::Result<()> {
        if !self.config.emit_checkpoints {
            return Ok(());
        }

        match &self.config.checkpoint_storage {
            CheckpointStorage::Disabled => {
                anyhow::bail!("No checkpoint directory configured (checkpoint storage is disabled)")
            }

            CheckpointStorage::Filesystem { dir } => {
                self.emit_checkpoint_to_filesystem(checkpoint, phase, dir)
                    .await
            }

            CheckpointStorage::SurrealDB { table_name, .. } => {
                self.emit_checkpoint_to_surrealdb(checkpoint, phase, table_name)
                    .await
            }
        }
    }

    /// Emit checkpoint to filesystem
    async fn emit_checkpoint_to_filesystem<C: Checkpoint>(
        &self,
        checkpoint: &C,
        phase: SyncPhase,
        dir: &str,
    ) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;

        let file = CheckpointFile::new(checkpoint, phase.clone())?;
        let timestamp = Utc::now().to_rfc3339();
        let filename = format!("{}/checkpoint_{}_{}.json", dir, phase.as_str(), timestamp);

        std::fs::write(&filename, serde_json::to_string_pretty(&file)?)?;

        tracing::info!(
            "Emitted {} checkpoint to {}: {}",
            phase,
            filename,
            checkpoint.to_cli_string()
        );

        Ok(())
    }

    /// Emit checkpoint to SurrealDB
    async fn emit_checkpoint_to_surrealdb<C: Checkpoint>(
        &self,
        checkpoint: &C,
        phase: SyncPhase,
        table_name: &str,
    ) -> anyhow::Result<()> {
        let client = self.surreal_client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("SurrealDB client not provided for checkpoint storage")
        })?;

        let store = CheckpointStore::new(client.clone(), table_name.to_string());

        let id = CheckpointID {
            database_type: C::DATABASE_TYPE.to_string(),
            phase: phase.as_str().to_string(),
        };

        let checkpoint_data = serde_json::to_string(checkpoint)?;
        store.store_checkpoint(&id, checkpoint_data).await?;

        tracing::info!(
            "Stored {} checkpoint in SurrealDB table '{}': {}",
            phase,
            table_name,
            checkpoint.to_cli_string()
        );

        Ok(())
    }

    /// Read the latest checkpoint file for a given phase.
    ///
    /// Scans the checkpoint directory for files matching the phase
    /// and returns the most recently modified one.
    ///
    /// Note: This method only supports filesystem storage.
    ///
    /// # Arguments
    ///
    /// * `phase` - Sync phase to find checkpoint for
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Storage backend is not filesystem
    /// - No checkpoint file found for the phase
    /// - Failed to read or parse the file
    pub async fn read_latest_checkpoint(&self, phase: SyncPhase) -> anyhow::Result<CheckpointFile> {
        let dir = match &self.config.checkpoint_storage {
            CheckpointStorage::Filesystem { dir } => dir,
            _ => {
                return Err(anyhow::anyhow!(
                    "Filesystem checkpoint storage not configured"
                ))
            }
        };

        let mut latest_file = None;
        let mut latest_time = None;

        for entry in std::fs::read_dir(dir)? {
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
        let file: CheckpointFile = serde_json::from_str(&content)?;
        Ok(file)
    }

    /// Read and parse checkpoint into database-specific type.
    ///
    /// This is a **LOADING** operation - it reads from disk and parses
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
    /// - No checkpoint file found for the phase
    /// - Checkpoint database type doesn't match `C::DATABASE_TYPE`
    /// - Failed to deserialize checkpoint data
    pub async fn read_checkpoint<C: Checkpoint>(&self, phase: SyncPhase) -> anyhow::Result<C> {
        let file = self.read_latest_checkpoint(phase).await?;
        file.parse::<C>()
    }
}
