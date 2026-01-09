//! Neo4j sync utilities for surreal-sync
//!
//! Provides full and incremental sync from Neo4j to SurrealDB.

mod full_sync;
mod incremental_sync;
mod neo4j_checkpoint;
mod neo4j_client;
mod sync_types;

pub use full_sync::{
    convert_neo4j_type_to_surreal_value, row_to_relation, run_full_sync, Neo4jConversionContext,
    Neo4jJsonProperty, Relation, SourceOpts, SurrealOpts,
};
pub use incremental_sync::{run_incremental_sync, Neo4jChangeStream, Neo4jIncrementalSource};
pub use neo4j_checkpoint::get_current_checkpoint;
pub use neo4j_client::new_neo4j_client;

// Re-export sync types
pub use sync_types::{
    ChangeStream, IncrementalSource, SourceDatabase, SyncCheckpoint, SyncConfig, SyncManager,
    SyncPhase,
};
