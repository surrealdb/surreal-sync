//! Kafka type conversions, from_kafka origin, and producer for surreal-sync.
//!
//! - [`types`] — protobuf message types and TypedValue conversions (default)
//! - [`from_kafka`] — Kafka consumer / incremental sync origin (`from_kafka` feature)
//! - [`producer`] — test producer helpers (`producer` feature)

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_kafka")]
pub mod from_kafka;

#[cfg(feature = "producer")]
pub mod producer;
