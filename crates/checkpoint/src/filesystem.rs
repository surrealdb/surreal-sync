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
        tokio::fs::create_dir_all(&self.dir).await?;

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

        tokio::fs::write(&filename, serde_json::to_string_pretty(&stored)?).await?;
        tracing::info!("Stored checkpoint to {}", filename.display());
        Ok(())
    }

    async fn read_checkpoint(&self, id: &CheckpointID) -> Result<Option<StoredCheckpoint>> {
        if !tokio::fs::try_exists(&self.dir).await.unwrap_or(false) {
            return Ok(None);
        }

        // Find latest checkpoint file for this phase
        let prefix = format!("checkpoint_{}_", id.phase);
        let mut latest_file = None;
        let mut latest_time = None;

        let mut entries = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let filename = entry.file_name().to_string_lossy().to_string();
            if filename.starts_with(&prefix) && filename.ends_with(".json") {
                let modified = entry.metadata().await?.modified()?;
                if latest_time.is_none() || modified > latest_time.unwrap() {
                    latest_time = Some(modified);
                    latest_file = Some(entry.path());
                }
            }
        }

        match latest_file {
            Some(path) => {
                let content = tokio::fs::read_to_string(path).await?;
                Ok(Some(serde_json::from_str(&content)?))
            }
            None => Ok(None),
        }
    }
}
