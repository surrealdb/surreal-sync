//! Universal sync module - re-exports from checkpoint crate
//!
//! This module re-exports checkpoint management types from the `checkpoint` crate.
//! Database-specific incremental sync traits and implementations are defined in
//! their respective modules:
//!
//! - [`crate::mysql::source`] - MySQL trigger-based incremental sync
//! - [`crate::postgresql::incremental_sync`] - PostgreSQL trigger-based incremental sync
//! - `surreal_sync_mongodb` - MongoDB change stream incremental sync
//! - `surreal_sync_neo4j` - Neo4j timestamp-based incremental sync
//!
//! # Checkpoint Management
//!
//! The checkpoint system allows reliable resumption of sync operations after failures.
//! Each database has its own checkpoint type:
//!
//! - `MySQLCheckpoint` - Sequence ID from MySQL audit table
//! - `PostgreSQLCheckpoint` - Sequence ID from PostgreSQL audit table
//! - `MongoDBCheckpoint` - Resume token from MongoDB change streams
//! - `Neo4jCheckpoint` - Timestamp from Neo4j
//!
//! All checkpoint types implement the `Checkpoint` trait from the checkpoint crate.

// Re-export checkpoint management types (from checkpoint crate)
pub use checkpoint::{Checkpoint, CheckpointFile, SyncConfig, SyncManager, SyncPhase};

// Re-export checkpoint helper functions
pub use checkpoint::{get_checkpoint_for_phase, get_first_checkpoint_from_dir};
