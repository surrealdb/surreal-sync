//! BigQuery type conversions and from_bigquery origin for surreal-sync.
//!
//! # Embed surface
//!
//! With the `from_bigquery` feature, embedders use only:
//!
//! ```ignore
//! use surreal_sync_bigquery::{run, FlattenId, InPlaceTransform, Value};
//! // or: use surreal_sync_bigquery::from_bigquery::{run, FlattenId, InPlaceTransform, Value};
//! ```

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_bigquery")]
pub mod from_bigquery;

/// Crate-root sugar for the public embed surface (same four items as
/// [`from_bigquery`]).
#[cfg(feature = "from_bigquery")]
pub use from_bigquery::{run, FlattenId, InPlaceTransform, Value};
