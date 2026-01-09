//! Kafka message types.
//!
//! High-level wrappers for decoded Kafka messages. These types are shared
//! between the kafka consumer crate and the kafka-types conversion crate.
//!
//! ## Why in kafka-types?
//!
//! Originally in the kafka crate's consumer module, these types were moved
//! here to break a circular dependency. The kafka crate now depends on
//! kafka-types for these definitions.

use crate::proto::ProtoMessage;

/// A decoded Kafka message with metadata.
///
/// Represents a fully decoded message from a Kafka topic, including
/// the payload and Kafka-specific metadata (topic, partition, offset, etc.).
#[derive(Debug, Clone)]
pub struct Message {
    /// Decoded message payload
    pub payload: Payload,
    /// Kafka topic name
    pub topic: String,
    /// Kafka partition number
    pub partition: i32,
    /// Kafka offset within the partition
    pub offset: i64,
    /// Message key (if any)
    pub key: Option<Vec<u8>>,
    /// Message timestamp in milliseconds since epoch (if available)
    pub timestamp: Option<i64>,
}

/// Message payload variants.
///
/// Currently only supports protobuf-encoded payloads, but designed
/// for extensibility to other formats (Avro, JSON, etc.) if needed.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Protobuf-encoded message
    Protobuf(ProtoMessage),
}
