//! JSON type conversions and from_jsonl origin for surreal-sync.
//!
//! - [`types`] — bidirectional TypedValue ↔ JSON conversions
//! - [`from_jsonl`] — JSONL file/S3/HTTP origin sync (feature-gated)

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_jsonl")]
pub mod from_jsonl;
