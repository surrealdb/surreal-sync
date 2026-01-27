//! MongoDB sync utilities for surreal-sync
//!
//! Provides full and incremental sync from MongoDB to SurrealDB.

pub mod checkpoint;
mod full_sync;
mod incremental_sync;

pub use full_sync::{
    convert_bson_to_universal_value, migrate_from_mongodb, run_full_sync, SourceOpts, SurrealOpts,
};
pub use incremental_sync::{run_incremental_sync, MongoChangeStream, MongodbIncrementalSource};

// Re-export checkpoint types from this crate
pub use checkpoint::MongoDBCheckpoint;
