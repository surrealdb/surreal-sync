//! CSV file import module for SurrealDB
//!
//! This module provides functionality to stream CSV files from various sources
//! (local files, S3, HTTP/HTTPS) and import them into SurrealDB tables.

mod metrics;
mod sync;

pub use sync::{sync, Config};

// Re-export SurrealDB utilities from shared crate
pub use surreal2_sink::{surreal_connect, write_records, SurrealOpts};

// Re-export file source types for convenience
pub use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};
