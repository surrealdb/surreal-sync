//! JSONL import for SurrealDB
//!
//! This crate provides functionality for importing JSONL (JSON Lines) files into SurrealDB.
//! It supports conversion rules that transform JSON objects into SurrealDB Thing references.

pub mod conversion;
mod sync;

pub use conversion::ConversionRule;
pub use sync::{migrate_from_jsonl, sync, Config, SourceOpts};

// Re-export SurrealDB utilities from shared crate
pub use surreal_sync_surreal::{surreal_connect, write_records, SurrealOpts};

// Re-export file source types for convenience
pub use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};
