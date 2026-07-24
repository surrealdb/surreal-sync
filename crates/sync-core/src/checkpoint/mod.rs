//! Checkpoint management API for surreal-sync.
//!
//! Storage backends live in separate crates (`surreal_sync_runtime::checkpoint_fs`,
//! `surreal_sync_surreal` features `v2` / `v3` checkpoint modules). This module holds only the
//! storage-agnostic types and traits — no `tokio` or `surrealdb`.

mod config;
mod file;
mod manager;
mod phase;
mod snapshot;
mod snapshot_checkpointer;
pub mod store;

pub use config::{CheckpointStorage, SyncConfig};
pub use file::CheckpointFile;
pub use manager::{NullStore, NullSyncManager, SyncManager};
pub use phase::SyncPhase;
pub use snapshot::{InterleavedSnapshotCheckpoint, SnapshotTableProgress};
pub use snapshot_checkpointer::SnapshotCheckpointer;
pub use store::{CheckpointID, CheckpointStore, StoredCheckpoint};

/// Trait that database-specific checkpoints must implement.
///
/// This trait defines the interface for checkpoint types, enabling
/// storage-agnostic checkpoint file handling while preserving
/// database-specific data structures.
pub trait Checkpoint: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone {
    /// Database type identifier (e.g., "mongodb", "neo4j", "postgresql", "mysql").
    const DATABASE_TYPE: &'static str;

    /// Convert to CLI-friendly string format.
    fn to_cli_string(&self) -> String;

    /// Parse from CLI string format.
    fn from_cli_string(s: &str) -> anyhow::Result<Self>
    where
        Self: Sized;
}
