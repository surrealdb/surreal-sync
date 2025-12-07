//! Kafka type conversion library for surreal-sync.
//!
//! This crate provides bidirectional conversion between TypedValue (the unified
//! intermediate representation used by surreal-sync) and Kafka protobuf messages.
//!
//! # Architecture
//!
//! ```text
//! Forward (Populator):  TypedValue → ProtoFieldValue → encoded bytes
//! Reverse (Source):     encoded bytes → ProtoFieldValue → TypedValue → surrealdb::sql::Value
//! ```
//!
//! # Modules
//!
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
//! use sync_core::InternalRow;
//!
//! let encoded = encode_row(&row, &table_schema)?;
//! let key = get_message_key(&row);
//! ```
//!
//! ## Reverse Conversion (Source)
//!
//! ```ignore
//! use kafka_types::reverse::{message_to_typed_values, typed_values_to_surreal};
//!
//! let typed_values = message_to_typed_values(message, Some(&table_schema))?;
//! let surreal_values = typed_values_to_surreal(typed_values);
//! ```

pub mod error;
pub mod forward;
pub mod reverse;

// Re-export main types for convenient access
pub use error::{KafkaTypesError, Result};
pub use forward::{
    encode_generated_value, encode_row, encode_typed_value, encode_typed_values, get_message_key,
    get_message_key_from_typed_values, get_proto_type,
};
pub use reverse::{
    message_to_typed_values, proto_to_typed_value, proto_to_typed_value_with_schema,
    typed_values_to_surreal,
};
