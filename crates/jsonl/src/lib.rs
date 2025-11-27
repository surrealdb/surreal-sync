//! JSONL import for SurrealDB
//!
//! This crate provides functionality for importing JSONL (JSON Lines) files into SurrealDB.
//! It supports conversion rules that transform JSON objects into SurrealDB Thing references.

pub mod conversion;
mod surreal;
mod sync;

pub use conversion::ConversionRule;
pub use surreal::{SourceOpts, SurrealOpts};
pub use sync::migrate_from_jsonl;
