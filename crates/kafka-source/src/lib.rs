//! Kafka consumer and sync library for surreal-sync.
//!
//! This crate provides:
//! - Kafka consumer with protobuf decoding
//! - Incremental sync to SurrealDB
//!
//! # Features
//!
//! - Runtime Protobuf Support: Parse `.proto` files at runtime and decode messages without code generation
//! - Field Introspection: List fields and extract values from decoded messages dynamically
//! - Consumer Groups: Spawn multiple consumers in the same consumer group
//! - Batch Processing: Process messages in batches with atomic commits
//! - SurrealDB Sync: Incremental sync from Kafka to SurrealDB
//!
//! # Dependency Direction
//!
//! This crate depends on `kafka-types` for shared type definitions (ProtoFieldValue,
//! ProtoMessage, Message, Payload, etc.). This breaks the circular dependency that
//! would otherwise exist if kafka-types depended on this crate.

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
pub mod sync;

// Re-export from kafka-types for convenience
// NOTE: These types are defined in kafka-types, not here.
// This re-export maintains backward compatibility with existing code.
pub use kafka_types::{
    Message, Payload, ProtoFieldDescriptor, ProtoFieldValue, ProtoMessage, ProtoMessageDescriptor,
    ProtoSchema, ProtoType,
};

// Re-export sync functions
pub use sync::{run_incremental_sync, Config};

// Re-export consumer types
pub use client::Client;
pub use consumer::{Consumer, ConsumerConfig};
pub use error::{Error, Result};
pub use proto::decoder::ProtoDecoder;
pub use proto::parser::ProtoParser;
