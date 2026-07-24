//! Protobuf encoder for Row to wire format.
//!
//! This module re-exports encoding functions from kafka-types crate.

// Re-export from kafka-types
pub use kafka_types::forward::{encode_row, get_message_key};
