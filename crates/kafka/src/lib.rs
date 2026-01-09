//! Kafka consumer library designed for `surreal-sync`'s specific use case of processing Kafka messages
//! whose values are encoded in protobuf.
//!
//! Features:
//!
//! - Runtime Protobuf Support: Parse `.proto` files at runtime and decode messages without code generation
//! - Field Introspection: List fields and extract values from decoded messages dynamically
//! - Consumer Groups: Spawn multiple consumers in the same consumer group
//! - Batch Processing: Process messages in batches with atomic commits

/// High-level API for spawning consumer tasks
///
/// Takes the consumer config and .proto schema, to create one or more consumers
/// in the same consumer group, each running in its own async task.
pub mod client;

/// Low-level consumer with peek buffer and manual offsets
///
/// Created by the client given the consumer config and .proto schema.
pub mod consumer;
pub mod error;
pub mod proto;

// Re-export main types for easy access
pub use client::Client;
pub use consumer::{Consumer, ConsumerConfig, Message, Payload};
pub use error::{Error, Result};
pub use proto::decoder::{ProtoDecoder, ProtoFieldValue, ProtoMessage};
pub use proto::parser::{ProtoFieldDescriptor, ProtoMessageDescriptor, ProtoSchema, ProtoType};
