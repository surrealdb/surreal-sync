//! Kafka type conversions, from_kafka origin, and producer for surreal-sync.
//!
//! - [`types`] — protobuf message types and TypedValue conversions (default)
//! - [`from_kafka`] — Kafka consumer / incremental sync origin (`from_kafka` feature)
//! - [`producer`] — test producer helpers (`producer` feature)
//!
//! # Embed surface
//!
//! With the `from_kafka` feature, embedders use only:
//!
//! ```ignore
//! use surreal_sync_kafka::{run, FlattenId, InPlaceTransform, Value};
//! // or: use surreal_sync_kafka::from_kafka::{run, FlattenId, InPlaceTransform, Value};
//! ```

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_kafka")]
pub mod from_kafka;

#[cfg(feature = "producer")]
pub mod producer;

/// Crate-root sugar for the public embed surface (same four items as
/// [`from_kafka`]).
#[cfg(feature = "from_kafka")]
pub use from_kafka::{run, FlattenId, InPlaceTransform, Value};
