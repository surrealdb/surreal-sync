//! CSV file import for SurrealDB.
//!
//! Streams CSV files from various sources (local files, S3, HTTP/HTTPS) and
//! imports them into SurrealDB tables.

mod metrics;
mod sync;

pub use sync::{sync, sync_with_transforms, Config};

// Re-export file source types for convenience
pub use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};
