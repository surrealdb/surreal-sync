//! MongoDB sync utilities for surreal-sync
//!
//! Provides full and incremental sync from MongoDB to SurrealDB.

pub mod checkpoint;
mod full_sync;
mod incremental_sync;

pub use full_sync::{
    convert_bson_document_to_record_with_schema, convert_bson_to_universal_value,
    convert_bson_to_universal_value_with_schema, migrate_from_mongodb, run_full_sync, SourceOpts,
    SyncOpts,
};
pub use incremental_sync::{run_incremental_sync, MongoChangeStream, MongodbIncrementalSource};

// Re-export checkpoint types from this crate
pub use checkpoint::MongoDBCheckpoint;
