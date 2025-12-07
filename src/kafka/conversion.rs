//! Kafka conversion module
//!
//! This module re-exports conversion functions from the kafka-types crate.
//! The actual conversion logic has been moved to the kafka-types crate for
//! consistent TypedValue-based conversion across the codebase.

// All conversion functions are now in the kafka-types crate.
// The Kafka source (incremental.rs) uses kafka-types directly:
// - kafka_types::message_to_typed_values() for Message -> TypedValue conversion
// - kafka_types::typed_values_to_surreal() for TypedValue -> surrealdb::sql::Value conversion
