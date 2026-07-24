//! Filesystem checkpoint storage for surreal-sync.
//!
//! Provides [`FilesystemStore`] and helpers for reading checkpoint files
//! from a directory. The storage-agnostic API lives in `sync-core`.

mod filesystem;
mod helpers;
mod reader;

pub use filesystem::FilesystemStore;
pub use helpers::{get_checkpoint_for_phase, get_first_checkpoint_from_dir};
pub use reader::CheckpointFileReader;

// Re-export API types commonly used with the filesystem backend.
pub use surreal_sync_core::{
    Checkpoint, CheckpointFile, CheckpointID, CheckpointStore, NullStore, NullSyncManager,
    StoredCheckpoint, SyncManager, SyncPhase,
};

#[cfg(test)]
mod tests;
