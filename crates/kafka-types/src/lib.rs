//! Kafka type conversion library for surreal-sync.
//!
//! This crate provides:
//! - Shared proto type definitions (ProtoFieldValue, ProtoMessage, ProtoType, etc.)
//! - Kafka message types (Message, Payload)
//! - Bidirectional conversion between TypedValue and protobuf messages
//!
//! # Architecture
//!
//! ```text
//! Forward (Populator):  TypedValue → ProtoFieldValue → encoded bytes
//! Reverse (Source):     encoded bytes → ProtoFieldValue → TypedValue → surrealdb::sql::Value
//! ```
//!
//! # Dependency Direction
//!
//! This crate defines the shared types that both the kafka consumer crate
//! and sync logic need. The dependency flows:
//!
//! ```text
//! kafka-types (this crate):
//!   - Defines: ProtoFieldValue, ProtoMessage, Message, Payload, etc.
//!   - NO dependency on kafka crate (breaks circular dependency)
//!
//! kafka (consumer crate):
//!   - Depends on: kafka-types
//!   - Uses types from here, adds decoding and consumer logic
//! ```
//!
//! # Modules
//!
//! - [`proto`] - Protobuf type definitions (ProtoFieldValue, ProtoMessage, ProtoType)
//! - [`message`] - Kafka message types (Message, Payload)
//! - [`forward`] - TypedValue → Protobuf encoding for producing messages
//! - [`reverse`] - Protobuf → TypedValue conversion for consuming messages
//! - [`error`] - Error types for conversion operations
//!
//! # Examples
//!
//! ## Forward Conversion (Populator)
//!
//! ```ignore
//! use kafka_types::forward::{encode_row, get_message_key};
//! use sync_core::UniversalRow;
//!
//! let encoded = encode_row(&row, &table_schema)?;
//! let key = get_message_key(&row);
//! ```
//!
//! ## Reverse Conversion (Source)
//!
//! ```ignore
//! use kafka_types::reverse::message_to_typed_values;
//! use surrealdb_types::typed_values_to_surreal_map;
//!
//! let typed_values = message_to_typed_values(message, Some(&table_schema))?;
//! let surreal_values = typed_values_to_surreal_map(typed_values);
//! ```

pub mod error;
pub mod forward;
pub mod message;
pub mod proto;
pub mod reverse;

// Re-export proto types for convenient access
pub use proto::{
    ProtoFieldDescriptor, ProtoFieldValue, ProtoMessage, ProtoMessageDescriptor, ProtoSchema,
    ProtoType,
};

// Re-export message types for convenient access
pub use message::{Message, Payload};

// Re-export error types
pub use error::{KafkaTypesError, Result};

// Re-export conversion functions
pub use forward::{
    encode_generated_value, encode_row, encode_typed_value, encode_typed_values, get_message_key,
    get_message_key_from_typed_values, get_proto_type,
};
pub use reverse::{
    message_to_typed_values, proto_to_typed_value, proto_to_typed_value_with_schema,
};
