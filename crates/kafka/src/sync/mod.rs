//! Kafka sync to SurrealDB.
//!
//! This module provides incremental sync functionality, consuming
//! protobuf-encoded messages from Kafka and writing to SurrealDB.

mod incremental;

pub use incremental::{run_incremental_sync, Config, SurrealOpts};
