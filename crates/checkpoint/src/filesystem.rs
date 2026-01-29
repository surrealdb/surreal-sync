//! Filesystem-based checkpoint storage implementation.

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use std::path::PathBuf;

use crate::store::{CheckpointID, CheckpointStore, StoredCheckpoint};

/// Filesystem implementation of CheckpointStore trait.
///
/// Stores checkpoints as JSON files in a directory.
pub struct FilesystemStore {
    dir: PathBuf,
}

impl FilesystemStore {
    /// Create a new FilesystemStore with the given directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Get the directory path.
    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }
}

#[async_trait]
impl CheckpointStore for FilesystemStore {
    async fn store_checkpoint(&self, id: &CheckpointID, checkpoint_data: String) -> Result<()> {
        std::fs::create_dir_all(&self.dir)?;

        let stored = StoredCheckpoint {
            checkpoint_data,
            database_type: id.database_type.clone(),
            phase: id.phase.clone(),
            created_at: Utc::now(),
        };

        let timestamp = Utc::now().to_rfc3339();
        let filename = self
            .dir
            .join(format!("checkpoint_{}_{}.json", id.phase, timestamp));

        std::fs::write(&filename, serde_json::to_string_pretty(&stored)?)?;
        tracing::info!("Stored checkpoint to {}", filename.display());
        Ok(())
    }

    async fn read_checkpoint(&self, id: &CheckpointID) -> Result<Option<StoredCheckpoint>> {
        if !self.dir.exists() {
            return Ok(None);
        }

        // Find latest checkpoint file for this phase
        let prefix = format!("checkpoint_{}_", id.phase);
        let mut latest_file = None;
        let mut latest_time = None;

        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let filename = entry.file_name().to_string_lossy().to_string();
            if filename.starts_with(&prefix) && filename.ends_with(".json") {
                let modified = entry.metadata()?.modified()?;
                if latest_time.is_none() || modified > latest_time.unwrap() {
                    latest_time = Some(modified);
                    latest_file = Some(entry.path());
                }
            }
        }

        match latest_file {
            Some(path) => {
                let content = std::fs::read_to_string(path)?;
                Ok(Some(serde_json::from_str(&content)?))
            }
            None => Ok(None),
        }
    }
}
