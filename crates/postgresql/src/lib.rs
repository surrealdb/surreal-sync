//! PostgreSQL logical replication support using wal2json
//!
//! This library provides support for PostgreSQL logical replication
//! using regular SQL connections and the wal2json output plugin.

mod change;
mod config;
mod logical_replication;
mod value;
mod wal2json;

// Make testing module available for integration tests
#[doc(hidden)]
pub mod testing;

pub use change::{wal2json_to_psql, Action, Row, Value};
pub use config::Config;
pub use logical_replication::{Client, Slot};
pub use value::{Date, Interval, Time, TimeTz, Timestamp, TimestampTz, Uuid};
pub use wal2json::parse_wal2json;
