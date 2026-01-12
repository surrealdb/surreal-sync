//! PostgreSQL logical replication support using wal2json
//!
//! This library provides support for PostgreSQL logical replication
//! using regular SQL connections and the wal2json output plugin.
//!
//! # Full Sync
//!
//! Full sync migrates all data from PostgreSQL to SurrealDB and emits
//! checkpoints for coordination with incremental sync:
//!
//! ```ignore
//! use surreal_sync_postgresql_wal2json_source::{run_full_sync, SourceOpts};
//!
//! let source_opts = SourceOpts {
//!     connection_string: "postgresql://user:pass@localhost:5432/mydb".to_string(),
//!     slot_name: "surreal_sync_slot".to_string(),
//!     tables: vec!["users".to_string(), "orders".to_string()],
//!     schema: "public".to_string(),
//! };
//!
//! run_full_sync(source_opts, "namespace", "database", surreal_opts, sync_config).await?;
//! ```
//!
//! # Incremental Sync
//!
//! Incremental sync streams changes from a starting checkpoint:
//!
//! ```ignore
//! use surreal_sync_postgresql_wal2json_source::{run_incremental_sync, PostgreSQLLogicalCheckpoint};
//!
//! let from_checkpoint = PostgreSQLLogicalCheckpoint::from_cli_string("0/1949850")?;
//!
//! run_incremental_sync(
//!     source_opts,
//!     "namespace",
//!     "database",
//!     surreal_opts,
//!     from_checkpoint,
//!     None, // no target checkpoint
//!     3600, // 1 hour timeout
//! ).await?;
//! ```

mod change;
pub mod checkpoint;
mod config;
mod full_sync;
mod incremental_sync;
mod logical_replication;
pub mod sync;
mod value;
mod wal2json;

// Make testing module available for integration tests
#[doc(hidden)]
pub mod testing;

pub use change::{Action, Row, Value};
pub use checkpoint::PostgreSQLLogicalCheckpoint;
pub use config::Config;
pub use full_sync::{run_full_sync, SourceOpts};
pub use incremental_sync::run_incremental_sync;
pub use logical_replication::{Client, Slot};
pub use sync::{State, StateID, Store};
