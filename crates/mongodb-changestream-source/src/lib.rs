//! MongoDB sync utilities for surreal-sync
//!
//! Provides full and incremental sync from MongoDB to SurrealDB.

pub mod checkpoint;
mod full_sync;
mod incremental_sync;

pub use full_sync::{
    convert_bson_document_to_record_with_schema, convert_bson_to_universal_value,
    convert_bson_to_universal_value_with_schema, migrate_from_mongodb, run_full_sync,
    run_full_sync_with_transforms, SourceOpts, SyncOpts,
};
pub use incremental_sync::{
    run_incremental_sync, run_incremental_sync_with_transforms, MongoChangeStream,
    MongodbIncrementalSource, ReplicationTailOptions,
};

// Re-export checkpoint types from this crate
pub use checkpoint::{get_current_checkpoint, get_resume_token, MongoDBCheckpoint};
