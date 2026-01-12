//! PostgreSQL logical replication sync to SurrealDB.
//!
//! This module provides sync functionality for PostgreSQL logical decoding (WAL-based)
//! to SurrealDB.

mod config_and_sync;
mod incremental;
mod initial;
pub mod state;

pub use config_and_sync::{sync, Config, SurrealOpts};
pub use state::{State, StateID, Store};
