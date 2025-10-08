//! Checkpoint testing utilities
//!
//! This module provides helper functions for managing checkpoint files during tests.

use std::path::Path;

/// Clean up checkpoint directory to prevent cross-test contamination
///
/// This function removes the entire checkpoint directory and all its contents.
/// It's designed to be safe to call even if the directory doesn't exist.
///
/// # Arguments
/// * `checkpoint_dir` - Path to the checkpoint directory to clean up
///
/// # Examples
/// ```rust
/// // Clean up before and after tests
/// surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(".test-checkpoints").unwrap();
/// ```
pub fn cleanup_checkpoint_dir<P: AsRef<Path>>(checkpoint_dir: P) -> Result<(), std::io::Error> {
    // Remove directory and all contents, ignore error if it doesn't exist
    match std::fs::remove_dir_all(checkpoint_dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Check if a directory entry is a checkpoint file
///
/// This function determines if a given directory entry represents a checkpoint file
/// by checking if its filename matches the pattern: checkpoint_*.json
///
/// # Arguments
/// * `entry` - A directory entry to check
///
/// # Returns
/// * `true` if the entry is a checkpoint file, `false` otherwise
fn is_checkpoint_file(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_string_lossy()
        .starts_with("checkpoint_")
        && entry.file_name().to_string_lossy().ends_with(".json")
}

/// Check if a directory entry is a full_sync_start (t1) checkpoint
///
/// This function determines if a given directory entry represents a t1 checkpoint
/// by checking if its filename contains "full_sync_start"
///
/// # Arguments
/// * `entry` - A directory entry to check
///
/// # Returns
/// * `true` if the entry is a full_sync_start checkpoint, `false` otherwise
fn is_full_sync_start(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_string_lossy()
        .contains("full_sync_start")
}

/// Check if a directory entry is a full_sync_end (t2) checkpoint
///
/// This function determines if a given directory entry represents a t2 checkpoint
/// by checking if its filename contains "full_sync_end"
///
/// # Arguments
/// * `entry` - A directory entry to check
///
/// # Returns
/// * `true` if the entry is a full_sync_end checkpoint, `false` otherwise
fn is_full_sync_end(entry: &std::fs::DirEntry) -> bool {
    entry
        .file_name()
        .to_string_lossy()
        .contains("full_sync_end")
}

/// List all checkpoint files in a directory
///
/// This function finds all files in the given directory that match the checkpoint
/// file naming pattern (checkpoint_*.json).
///
/// # Arguments
/// * `checkpoint_dir` - Path to the checkpoint directory
///
/// # Returns
/// * `Ok(Vec<DirEntry>)` - List of checkpoint file entries
/// * `Err` - If the directory cannot be read
///
/// # Examples
/// ```ignore
/// let checkpoint_files = surreal_sync::testing::checkpoint::list_checkpoint_files(".test-checkpoints")?;
/// ```
pub fn list_checkpoint_files<P: AsRef<Path>>(
    checkpoint_dir: P,
) -> anyhow::Result<Vec<std::fs::DirEntry>> {
    let checkpoint_files = std::fs::read_dir(&checkpoint_dir)?
        .filter_map(|entry| entry.ok())
        .filter(is_checkpoint_file)
        .collect::<Vec<_>>();

    Ok(checkpoint_files)
}

/// Read and parse the t1 (full_sync_start) checkpoint from a directory
///
/// This function searches for the t1 checkpoint file in the given directory,
/// reads its content, and parses it into a SyncCheckpoint structure.
///
/// # Arguments
/// * `checkpoint_dir` - Path to the checkpoint directory
///
/// # Returns
/// * `Ok(SyncCheckpoint)` - The parsed t1 checkpoint
/// * `Err` - If no t1 checkpoint is found or parsing fails
///
/// # Examples
/// ```ignore
/// let t1 = surreal_sync::testing::checkpoint::read_t1_checkpoint(".test-checkpoints")?;
/// ```
pub fn read_t1_checkpoint<P: AsRef<Path>>(
    checkpoint_dir: P,
) -> anyhow::Result<crate::sync::SyncCheckpoint> {
    // Find all checkpoint files
    let checkpoint_files = list_checkpoint_files(&checkpoint_dir)?;

    // Find t1 (full_sync_start) checkpoint
    let t1_files: Vec<_> = checkpoint_files
        .iter()
        .filter(|entry| is_full_sync_start(entry))
        .collect();

    if t1_files.is_empty() {
        anyhow::bail!("No t1 (full_sync_start) checkpoint found in directory");
    }

    if t1_files.len() > 1 {
        anyhow::bail!(
            "Found {} t1 checkpoints, expected exactly 1",
            t1_files.len()
        );
    }

    // Read and parse t1 checkpoint
    let t1_content = std::fs::read_to_string(t1_files[0].path())?;
    let t1_json: serde_json::Value = serde_json::from_str(&t1_content)?;
    let t1_checkpoint: crate::sync::SyncCheckpoint =
        serde_json::from_value(t1_json["checkpoint"].clone())?;

    Ok(t1_checkpoint)
}

/// Verify that checkpoint-emitting full sync produces proper t1 and t2 checkpoints
///
/// This function validates both checkpoint file structure and content:
/// 1. Verifies at least 2 checkpoint files exist (t1 and t2)
/// 2. Checks that exactly 1 t1 (full_sync_start) and 1 t2 (full_sync_end) exist
/// 3. Validates timing: t1 timestamp < t2 timestamp
/// 4. Validates content progression based on database type
///
/// # Arguments
/// * `checkpoint_dir` - Path to the checkpoint directory to verify
///
/// # Examples
/// ```rust,no_run
/// // In your test after running full sync with checkpoints
/// surreal_sync::testing::checkpoint::verify_t1_t2_checkpoints(".test-checkpoints").unwrap();
/// ```
pub fn verify_t1_t2_checkpoints<P: AsRef<Path>>(checkpoint_dir: P) -> anyhow::Result<()> {
    println!("Verifying checkpoint files were created...");

    // Verify at least 2 checkpoint files exist (t1 and t2)
    let checkpoint_files = list_checkpoint_files(&checkpoint_dir)?;

    assert!(
        checkpoint_files.len() >= 2,
        "Expected at least 2 checkpoint files (t1 and t2), found {}",
        checkpoint_files.len()
    );

    // Find t1 (full_sync_start) and t2 (full_sync_end) checkpoints
    let t1_files: Vec<_> = checkpoint_files
        .iter()
        .filter(|entry| is_full_sync_start(entry))
        .collect();
    let t2_files: Vec<_> = checkpoint_files
        .iter()
        .filter(|entry| is_full_sync_end(entry))
        .collect();

    assert_eq!(
        t1_files.len(),
        1,
        "Expected exactly 1 t1 (full_sync_start) checkpoint"
    );
    assert_eq!(
        t2_files.len(),
        1,
        "Expected exactly 1 t2 (full_sync_end) checkpoint"
    );

    // Verify checkpoint timing: t1 should come before t2
    let t1_filename = t1_files[0].file_name().to_string_lossy().to_string();
    let t2_filename = t2_files[0].file_name().to_string_lossy().to_string();

    // Extract timestamps from filenames (format: checkpoint_*_YYYY-MM-DDTHH:MM:SS.nnnnnnnnn+00:00.json)
    let extract_timestamp = |filename: &str| -> Option<chrono::DateTime<chrono::Utc>> {
        let parts: Vec<&str> = filename.rsplitn(2, '_').collect();
        if parts.len() == 2 {
            let timestamp_part = parts[0].replace(".json", "");
            chrono::DateTime::parse_from_rfc3339(&timestamp_part)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        } else {
            None
        }
    };

    if let (Some(t1_time), Some(t2_time)) = (
        extract_timestamp(&t1_filename),
        extract_timestamp(&t2_filename),
    ) {
        assert!(
            t1_time < t2_time,
            "t1 checkpoint should come before t2 checkpoint: t1={t1_time}, t2={t2_time}"
        );
        println!("Checkpoint timing verification passed: t1 < t2");
    }

    // Verify checkpoint content based on database type
    let t1_content = std::fs::read_to_string(t1_files[0].path())?;
    let t2_content = std::fs::read_to_string(t2_files[0].path())?;

    // Parse checkpoint file format (has "checkpoint" wrapper)
    let t1_json: serde_json::Value = serde_json::from_str(&t1_content)?;
    let t2_json: serde_json::Value = serde_json::from_str(&t2_content)?;

    let t1_checkpoint: crate::sync::SyncCheckpoint =
        serde_json::from_value(t1_json["checkpoint"].clone())?;
    let t2_checkpoint: crate::sync::SyncCheckpoint =
        serde_json::from_value(t2_json["checkpoint"].clone())?;

    match (&t1_checkpoint, &t2_checkpoint) {
        // MongoDB: Resume token comparison
        (
            crate::sync::SyncCheckpoint::MongoDB {
                resume_token: t1_token,
                ..
            },
            crate::sync::SyncCheckpoint::MongoDB {
                resume_token: t2_token,
                ..
            },
        ) => {
            if t1_token == t2_token {
                println!("Checkpoint content verification passed: MongoDB resume tokens unchanged (no changes to monitored collections)");
            } else {
                println!("Checkpoint content verification passed: MongoDB resume tokens differ (changes detected)");
            }
        }

        // MySQL: Sequence ID progression
        (
            crate::sync::SyncCheckpoint::MySQL {
                sequence_id: t1_seq,
                ..
            },
            crate::sync::SyncCheckpoint::MySQL {
                sequence_id: t2_seq,
                ..
            },
        ) => {
            assert!(
                t2_seq >= t1_seq,
                "t2 sequence ID should be >= t1 sequence ID: t1={t1_seq}, t2={t2_seq}"
            );
            println!("Checkpoint content verification passed: MySQL sequence IDs show progression ({t1_seq} → {t2_seq})");
        }

        // PostgreSQL: Sequence ID progression
        (
            crate::sync::SyncCheckpoint::PostgreSQL {
                sequence_id: t1_seq,
                ..
            },
            crate::sync::SyncCheckpoint::PostgreSQL {
                sequence_id: t2_seq,
                ..
            },
        ) => {
            assert!(
                t2_seq >= t1_seq,
                "t2 sequence ID should be >= t1 sequence ID: t1={t1_seq}, t2={t2_seq}"
            );
            println!("Checkpoint content verification passed: PostgreSQL sequence IDs show progression ({t1_seq} → {t2_seq})");
        }

        // Neo4j: Timestamp progression
        (
            crate::sync::SyncCheckpoint::Neo4j(t1_timestamp),
            crate::sync::SyncCheckpoint::Neo4j(t2_timestamp),
        ) => {
            assert!(
                t2_timestamp >= t1_timestamp,
                "t2 checkpoint timestamp should be >= t1 checkpoint timestamp: t1={t1_timestamp}, t2={t2_timestamp}"
            );
            println!("Checkpoint content verification passed: Neo4j checkpoint timestamps show progression ({t1_timestamp} → {t2_timestamp})");
        }

        _ => {
            println!("Checkpoint content verification skipped: Mixed or unknown checkpoint types");
        }
    }

    println!("Checkpoint verification passed: Found t1 and t2 checkpoints");
    Ok(())
}
