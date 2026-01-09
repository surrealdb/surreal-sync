//! Protobuf type definitions.
//!
//! These types are the shared data structures for decoded protobuf messages.
//! They are used by both the decoder (in the kafka crate) and the converter (here).
//!
//! ## Why in kafka-types?
//!
//! Originally in the kafka crate, these types were moved here to break
//! a circular dependency. The kafka crate now depends on kafka-types
//! for these definitions, allowing both crates to share the same types
//! while keeping sync logic in kafka.
//!
//! ## Dependency Flow After Refactoring
//!
//! ```text
//! kafka-types (this crate):
//!   - Defines: ProtoFieldValue, ProtoMessage, ProtoType, etc.
//!   - NO dependency on kafka crate
//!
//! kafka (consumer crate):
//!   - Depends on: kafka-types
//!   - Uses: ProtoFieldValue, ProtoMessage, etc. from kafka-types
//!   - Provides: ProtoDecoder, Consumer, Client, sync logic
//! ```

use std::collections::HashMap;

/// Represents a field value in a decoded protobuf message.
///
/// This is the runtime representation of protobuf values after decoding.
/// Used by the decoder in the kafka crate and by type conversion in kafka-types.
#[derive(Debug, Clone)]
pub enum ProtoFieldValue {
    Double(f64),
    Float(f32),
    Int32(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Message(Box<ProtoMessage>),
    Repeated(Vec<ProtoFieldValue>),
    Null,
}

impl ProtoFieldValue {
    /// Get the type descriptor for this field value.
    pub fn proto_field_type(&self) -> ProtoType {
        match self {
            ProtoFieldValue::Double(_) => ProtoType::Double,
            ProtoFieldValue::Float(_) => ProtoType::Float,
            ProtoFieldValue::Int32(_) => ProtoType::Int32,
            ProtoFieldValue::Int64(_) => ProtoType::Int64,
            ProtoFieldValue::Uint32(_) => ProtoType::Uint32,
            ProtoFieldValue::Uint64(_) => ProtoType::Uint64,
            ProtoFieldValue::Bool(_) => ProtoType::Bool,
            ProtoFieldValue::String(_) => ProtoType::String,
            ProtoFieldValue::Bytes(_) => ProtoType::Bytes,
            ProtoFieldValue::Message(msg) => ProtoType::Message(msg.message_type.clone()),
            ProtoFieldValue::Repeated(_) => ProtoType::Repeated(Box::new(ProtoType::Null)),
            ProtoFieldValue::Null => ProtoType::Null,
        }
    }
}

/// Represents a decoded protobuf message.
///
/// Contains the message type name, decoded fields, and the schema descriptor
/// for field introspection.
#[derive(Debug, Clone)]
pub struct ProtoMessage {
    /// Message type name (e.g., "mypackage.MyMessage")
    pub message_type: String,
    /// Decoded field values by field name
    pub fields: HashMap<String, ProtoFieldValue>,
    /// Schema reference for field introspection
    pub descriptor: ProtoMessageDescriptor,
}

/// Protobuf field type enumeration.
///
/// Represents all possible protobuf scalar and composite types.
#[derive(Debug, Clone, PartialEq)]
pub enum ProtoType {
    Double,
    Float,
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Bool,
    String,
    Bytes,
    Message(String),
    Enum(String),
    Repeated(Box<ProtoType>),
    Optional(Box<ProtoType>),
    Null,
}

impl std::fmt::Display for ProtoType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name())
    }
}

impl ProtoType {
    /// Get the human-readable type name.
    pub fn type_name(&self) -> String {
        match self {
            ProtoType::Double => "double".to_string(),
            ProtoType::Float => "float".to_string(),
            ProtoType::Int32 => "int32".to_string(),
            ProtoType::Int64 => "int64".to_string(),
            ProtoType::Uint32 => "uint32".to_string(),
            ProtoType::Uint64 => "uint64".to_string(),
            ProtoType::Sint32 => "sint32".to_string(),
            ProtoType::Sint64 => "sint64".to_string(),
            ProtoType::Fixed32 => "fixed32".to_string(),
            ProtoType::Fixed64 => "fixed64".to_string(),
            ProtoType::Sfixed32 => "sfixed32".to_string(),
            ProtoType::Sfixed64 => "sfixed64".to_string(),
            ProtoType::Bool => "bool".to_string(),
            ProtoType::String => "string".to_string(),
            ProtoType::Bytes => "bytes".to_string(),
            ProtoType::Message(name) => format!("message:{name}"),
            ProtoType::Enum(name) => format!("enum:{name}"),
            ProtoType::Repeated(inner) => format!("repeated<{}>", inner.type_name()),
            ProtoType::Optional(inner) => format!("optional<{}>", inner.type_name()),
            ProtoType::Null => "null".to_string(),
        }
    }
}

/// Describes a single field in a protobuf message.
#[derive(Debug, Clone)]
pub struct ProtoFieldDescriptor {
    /// Field name
    pub name: String,
    /// Field number (tag)
    pub number: i32,
    /// Field type
    pub field_type: ProtoType,
    /// Whether the field is repeated
    pub is_repeated: bool,
    /// Whether the field is optional
    pub is_optional: bool,
}

/// Describes a protobuf message type (schema).
#[derive(Debug, Clone)]
pub struct ProtoMessageDescriptor {
    /// Fully qualified message name (e.g., "mypackage.MyMessage")
    pub name: String,
    /// Map of field names to their descriptors
    pub fields: HashMap<String, ProtoFieldDescriptor>,
    /// Ordered list of field names (preserves proto definition order)
    pub field_order: Vec<String>,
}

impl ProtoMessageDescriptor {
    /// Get a field descriptor by name.
    pub fn get_field(&self, name: &str) -> Option<&ProtoFieldDescriptor> {
        self.fields.get(name)
    }

    /// List all field names in definition order.
    pub fn list_fields(&self) -> &[String] {
        &self.field_order
    }
}

/// Represents a parsed protobuf schema containing multiple message types.
#[derive(Debug, Clone)]
pub struct ProtoSchema {
    /// Map of message type names to their descriptors
    pub messages: HashMap<String, ProtoMessageDescriptor>,
}

impl ProtoSchema {
    /// Get a message descriptor by name.
    pub fn get_message(&self, name: &str) -> Option<&ProtoMessageDescriptor> {
        self.messages.get(name)
    }

    /// List all message type names in the schema.
    pub fn list_messages(&self) -> Vec<String> {
        self.messages.keys().cloned().collect()
    }
}
