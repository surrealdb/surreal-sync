//! Generic sync manager for checkpoint operations.

use chrono::Utc;

use crate::{Checkpoint, CheckpointFile, SyncConfig, SyncPhase};

/// Manager for handling sync operations with checkpoint tracking.
///
/// The `SyncManager` provides storage-agnostic checkpoint management:
/// - **Saving**: Writes checkpoint files to disk
/// - **Loading**: Reads and parses checkpoint files
///
/// # Example
///
/// ```rust,ignore
/// use checkpoint::{SyncConfig, SyncManager, SyncPhase};
///
/// let config = SyncConfig::full_sync_with_checkpoints("/tmp/checkpoints".into());
/// let manager = SyncManager::new(config);
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
}

impl SyncManager {
    /// Create a new sync manager with the given configuration.
    pub fn new(config: SyncConfig) -> Self {
        Self { config }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Emit checkpoint for any database-specific checkpoint type.
    ///
    /// This is a **SAVING** operation - it writes the checkpoint to disk.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Database-specific checkpoint (e.g., `MongoDBCheckpoint`)
    /// * `phase` - Sync phase when this checkpoint is being emitted
    ///
    /// # File Naming
    ///
    /// Checkpoint files are named: `checkpoint_{phase}_{timestamp}.json`
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `emit_checkpoints` is false in config
    /// - `checkpoint_dir` is not configured
    /// - Failed to create directory or write file
    pub async fn emit_checkpoint<C: Checkpoint>(
        &self,
        checkpoint: &C,
        phase: SyncPhase,
    ) -> anyhow::Result<()> {
        if !self.config.emit_checkpoints {
            return Ok(());
        }

        let dir = self
            .config
            .checkpoint_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No checkpoint directory configured"))?;

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

    /// Read the latest checkpoint file for a given phase.
    ///
    /// Scans the checkpoint directory for files matching the phase
    /// and returns the most recently modified one.
    ///
    /// # Arguments
    ///
    /// * `phase` - Sync phase to find checkpoint for
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `checkpoint_dir` is not configured
    /// - No checkpoint file found for the phase
    /// - Failed to read or parse the file
    pub async fn read_latest_checkpoint(&self, phase: SyncPhase) -> anyhow::Result<CheckpointFile> {
        let dir = self
            .config
            .checkpoint_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No checkpoint directory configured"))?;

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
