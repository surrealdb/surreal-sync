//! Checkpoint management for surreal-sync
//!
//! Provides storage-agnostic checkpoint file handling with support for
//! database-specific checkpoint types.
//!
//! # Architecture
//!
//! This crate provides a generic checkpoint system that:
//! - Defines the `Checkpoint` trait for database-specific checkpoint types
//! - Provides `CheckpointFile` wrapper for storage-agnostic serialization
//! - Manages checkpoint saving/loading via `SyncManager`
//! - Supports multiple storage backends via `CheckpointStore` trait
//!
//! ## Storage Backends
//!
//! - `FilesystemStore` - Stores checkpoints as JSON files
//! - `Surreal2Store` - Stores checkpoints in SurrealDB v2
//! - `Surreal3Store` - Stores checkpoints in SurrealDB v3 (in checkpoint-surreal3 crate)
//!
//! Each database (MongoDB, Neo4j, PostgreSQL, MySQL) implements its own
//! checkpoint type with the `Checkpoint` trait.

mod config;
mod file;
mod filesystem;
mod manager;
mod phase;
pub mod store;
mod surreal2;

#[cfg(test)]
mod tests;

// Re-export config types
pub use config::{CheckpointStorage, SyncConfig};

// Re-export file types
pub use file::CheckpointFile;

// Re-export manager types
pub use manager::{CheckpointFileReader, NullStore, NullSyncManager, SyncManager};

// Re-export phase types
pub use phase::SyncPhase;

// Re-export store trait and types
pub use store::{CheckpointID, CheckpointStore, StoredCheckpoint};

// Re-export storage implementations
pub use filesystem::FilesystemStore;
pub use surreal2::Surreal2Store;

/// Trait that database-specific checkpoints must implement.
///
/// This trait defines the interface for checkpoint types, enabling
/// storage-agnostic checkpoint file handling while preserving
/// database-specific data structures.
///
/// # Example
///
/// ```rust
/// use checkpoint::Checkpoint;
/// use serde::{Deserialize, Serialize};
/// use chrono::{DateTime, Utc};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct MyDatabaseCheckpoint {
///     pub position: i64,
///     pub timestamp: DateTime<Utc>,
/// }
///
/// impl Checkpoint for MyDatabaseCheckpoint {
///     const DATABASE_TYPE: &'static str = "mydatabase";
///
///     fn to_cli_string(&self) -> String {
///         format!("{}:{}", self.position, self.timestamp.to_rfc3339())
///     }
///
///     fn from_cli_string(s: &str) -> anyhow::Result<Self> {
///         let parts: Vec<&str> = s.splitn(2, ':').collect();
///         if parts.len() != 2 {
///             anyhow::bail!("Invalid checkpoint format");
///         }
///         Ok(Self {
///             position: parts[0].parse()?,
///             timestamp: chrono::DateTime::parse_from_rfc3339(parts[1])?
///                 .with_timezone(&Utc),
///         })
///     }
/// }
/// ```
pub trait Checkpoint: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone {
    /// Database type identifier (e.g., "mongodb", "neo4j", "postgresql", "mysql").
    ///
    /// This constant is used to:
    /// - Identify the checkpoint type in serialized files
    /// - Validate checkpoint type when loading from file
    const DATABASE_TYPE: &'static str;

    /// Convert to CLI-friendly string format.
    ///
    /// The returned string should be parseable by `from_cli_string()`.
    /// This format is used for:
    /// - Command-line arguments (e.g., `--incremental-from`)
    /// - Logging and debugging output
    fn to_cli_string(&self) -> String;

    /// Parse from CLI string format.
    ///
    /// Should parse the format produced by `to_cli_string()`.
    fn from_cli_string(s: &str) -> anyhow::Result<Self>
    where
        Self: Sized;
}

// ============================================================================
// Standalone helper functions for reading checkpoint files
// ============================================================================

/// Read a checkpoint file for a specific phase from a directory.
///
/// Standalone function that doesn't require a `SyncConfig`.
/// Useful for tests and CLI scenarios where you just have a directory path.
///
/// # Arguments
/// * `checkpoint_dir` - Path to directory containing checkpoint files
/// * `phase` - The sync phase to look for (FullSyncStart or FullSyncEnd)
///
/// # Returns
/// * `Result<CheckpointFile>` - The parsed checkpoint file for the specified phase
///
/// # Errors
/// * Returns error if no checkpoint files for the phase are found
/// * Returns error if checkpoint file cannot be read or parsed
///
/// # Example
/// ```ignore
/// let checkpoint_file = get_checkpoint_for_phase(".test-checkpoints", SyncPhase::FullSyncStart).await?;
/// let pg_checkpoint: PostgreSQLCheckpoint = checkpoint_file.parse()?;
/// ```
pub async fn get_checkpoint_for_phase<P: AsRef<std::path::Path>>(
    checkpoint_dir: P,
    phase: SyncPhase,
) -> anyhow::Result<CheckpointFile> {
    let phase_str = phase.as_str();

    // Find checkpoint file matching the phase
    let checkpoint_files: Vec<_> = std::fs::read_dir(checkpoint_dir.as_ref())?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.contains(phase_str) && name.ends_with(".json"))
                .unwrap_or(false)
        })
        .collect();

    if checkpoint_files.is_empty() {
        return Err(anyhow::anyhow!(
            "No checkpoint files found for phase: {phase_str}"
        ));
    }

    // Read the matching checkpoint file - it's stored as StoredCheckpoint format
    let checkpoint_content = std::fs::read_to_string(checkpoint_files[0].path())?;
    let stored: StoredCheckpoint = serde_json::from_str(&checkpoint_content)?;

    // Convert StoredCheckpoint to CheckpointFile
    // The checkpoint_data field contains the JSON-serialized checkpoint
    let checkpoint: serde_json::Value = serde_json::from_str(&stored.checkpoint_data)?;
    let checkpoint_file = CheckpointFile {
        database_type: stored.database_type,
        checkpoint,
        phase,
        created_at: stored.created_at,
    };

    Ok(checkpoint_file)
}

/// Read the first checkpoint file from a directory containing checkpoint files.
///
/// This function looks for JSON checkpoint files in the specified directory
/// and returns the `CheckpointFile` from the first file found.
///
/// **Note**: Prefer `get_checkpoint_for_phase` for explicit phase selection.
///
/// # Arguments
/// * `checkpoint_dir` - Path to directory containing checkpoint files
///
/// # Returns
/// * `Result<CheckpointFile>` - The parsed checkpoint file from the first JSON file found
///
/// # Errors
/// * Returns error if no checkpoint files are found
/// * Returns error if checkpoint file cannot be read or parsed
///
/// # Example
/// ```ignore
/// let checkpoint_file = get_first_checkpoint_from_dir(".test-checkpoints").await?;
/// let mongodb_checkpoint: MongoDBCheckpoint = checkpoint_file.parse()?;
/// ```
pub async fn get_first_checkpoint_from_dir<P: AsRef<std::path::Path>>(
    checkpoint_dir: P,
) -> anyhow::Result<CheckpointFile> {
    // Find all JSON files in the checkpoint directory
    let checkpoint_files: Vec<_> = std::fs::read_dir(checkpoint_dir.as_ref())?
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

    // Read the first checkpoint file - it's stored as StoredCheckpoint format
    let checkpoint_content = std::fs::read_to_string(checkpoint_files[0].path())?;
    let stored: StoredCheckpoint = serde_json::from_str(&checkpoint_content)?;

    // Parse phase string to SyncPhase enum
    let phase = parse_sync_phase(&stored.phase)?;

    // Convert StoredCheckpoint to CheckpointFile
    let checkpoint: serde_json::Value = serde_json::from_str(&stored.checkpoint_data)?;
    let checkpoint_file = CheckpointFile {
        database_type: stored.database_type,
        checkpoint,
        phase,
        created_at: stored.created_at,
    };

    Ok(checkpoint_file)
}

/// Parse a phase string into a SyncPhase enum.
fn parse_sync_phase(phase: &str) -> anyhow::Result<SyncPhase> {
    match phase {
        "full_sync_start" => Ok(SyncPhase::FullSyncStart),
        "full_sync_end" => Ok(SyncPhase::FullSyncEnd),
        other => Err(anyhow::anyhow!("Unknown sync phase: {other}")),
    }
}
