//! PostgreSQL logical replication support using wal2json
//!
//! This library provides support for PostgreSQL logical replication
//! using regular SQL connections and the wal2json output plugin.

mod logical_replication;
mod wal2json;

pub use logical_replication::{Client, Slot};
pub use wal2json::parse_wal2json;
