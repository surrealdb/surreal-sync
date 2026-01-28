//! Neo4j sync utilities for surreal-sync
//!
//! Provides full and incremental sync from Neo4j to SurrealDB.

mod full_sync;
mod incremental_sync;
pub mod neo4j_checkpoint;
mod neo4j_client;

pub use full_sync::{
    convert_neo4j_type_to_universal_value, row_to_relation, run_full_sync, Neo4jConversionContext,
    Neo4jJsonProperty, Relation, SourceOpts, SyncOpts,
};
pub use incremental_sync::{run_incremental_sync, Neo4jChangeStream, Neo4jIncrementalSource};
pub use neo4j_checkpoint::{get_current_checkpoint, Neo4jCheckpoint};
pub use neo4j_client::new_neo4j_client;
