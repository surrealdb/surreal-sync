//! SurrealDB connection and write utilities
//!
//! Provides functions for connecting to SurrealDB and writing records/relations.

mod change;
mod connect;
mod write;

pub use change::{Change, ChangeOp};
pub use connect::{surreal_connect, SurrealOpts};
pub use write::{apply_change, write_record, write_records, write_relations};
