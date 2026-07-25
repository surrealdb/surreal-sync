//! JSON type conversions and from_jsonl origin for surreal-sync.
//!
//! - [`types`] — bidirectional TypedValue ↔ JSON conversions
//! - [`from_jsonl`] — JSONL file/S3/HTTP origin sync (feature-gated)
//!
//! # Embed surface
//!
//! With the `from_jsonl` feature, embedders use only:
//!
//! ```ignore
//! use surreal_sync_json::{run, FlattenId, InPlaceTransform, Value};
//! // or: use surreal_sync_json::from_jsonl::{run, FlattenId, InPlaceTransform, Value};
//! ```

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_jsonl")]
pub mod from_jsonl;

/// Crate-root sugar for the public embed surface (same four items as
/// [`from_jsonl`]).
#[cfg(feature = "from_jsonl")]
pub use from_jsonl::{run, FlattenId, InPlaceTransform, Value};
