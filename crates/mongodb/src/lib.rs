//! MongoDB sync utilities for surreal-sync
//!
//! Provides full and incremental sync from MongoDB to SurrealDB.

mod checkpoint;
mod full_sync;
mod incremental_sync;
mod sync_types;

pub use full_sync::{
    convert_bson_to_surreal_value, migrate_from_mongodb, run_full_sync, SourceOpts, SurrealOpts,
};
pub use incremental_sync::{run_incremental_sync, MongoChangeStream, MongodbIncrementalSource};

// Re-export sync types
pub use sync_types::{
    ChangeStream, IncrementalSource, SourceDatabase, SyncCheckpoint, SyncConfig, SyncManager,
    SyncPhase,
};
