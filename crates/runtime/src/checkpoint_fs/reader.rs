//! Blocking filesystem checkpoint file reader.

use std::marker::PhantomData;
use surreal_sync_core::{Checkpoint, CheckpointFile, StoredCheckpoint, SyncPhase};

/// Manager for reading checkpoint files directly from filesystem.
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

        let stored: StoredCheckpoint = serde_json::from_str(&content)?;
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
