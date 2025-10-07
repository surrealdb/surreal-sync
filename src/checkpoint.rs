//! Checkpoint management utilities
//!
//! This module provides helper functions for reading and managing checkpoint files
//! generated during sync operations.

use anyhow::Result;
use std::path::Path;

/// Read the first checkpoint from a directory containing checkpoint files
///
/// This function looks for JSON checkpoint files in the specified directory
/// and returns the checkpoint from the first file found. This is commonly used
/// in tests and CLI scenarios where you need to read a checkpoint generated
/// by a previous sync operation.
///
/// # Arguments
/// * `checkpoint_dir` - Path to directory containing checkpoint files
///
/// # Returns
/// * `Result<SyncCheckpoint>` - The parsed checkpoint from the first JSON file found
///
/// # Errors
/// * Returns error if no checkpoint files are found
/// * Returns error if checkpoint file cannot be read or parsed
pub async fn get_first_checkpoint_from_dir<P: AsRef<Path>>(
    checkpoint_dir: P,
) -> Result<crate::sync::SyncCheckpoint> {
    // Find all JSON files in the checkpoint directory
    let checkpoint_files: Vec<_> = std::fs::read_dir(checkpoint_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect();

    if checkpoint_files.is_empty() {
        return Err(anyhow::anyhow!("No checkpoint files found in directory"));
    }

    // Read the first checkpoint file
    let checkpoint_content = std::fs::read_to_string(checkpoint_files[0].path())?;
    let checkpoint_json: serde_json::Value = serde_json::from_str(&checkpoint_content)?;

    // Parse the checkpoint from the JSON structure
    let sync_checkpoint: crate::sync::SyncCheckpoint =
        serde_json::from_value(checkpoint_json["checkpoint"].clone())?;

    Ok(sync_checkpoint)
}
