//! CSV file import module for SurrealDB
//!
//! This module provides functionality to stream CSV files from various sources
//! (local files, S3, HTTP/HTTPS) and import them into SurrealDB tables.

mod metrics;
pub mod surreal;
mod sync;

pub use sync::{sync, Config};

// Re-export file source types for convenience
pub use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};
