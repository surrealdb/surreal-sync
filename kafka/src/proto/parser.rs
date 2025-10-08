use crate::error::{Error, Result};
use protobuf_parse::Parser;
use std::collections::HashMap;
use std::path::Path;

/// Represents a parsed protobuf schema
#[derive(Debug, Clone)]
pub struct ProtoSchema {
    /// Map of message type names to their field descriptors
    pub(crate) messages: HashMap<String, ProtoMessageDescriptor>,
}

/// Describes a protobuf message type
#[derive(Debug, Clone)]
pub struct ProtoMessageDescriptor {
    /// Fully qualified message name (e.g., "mypackage.MyMessage")
    pub name: String,
    /// Map of field names to their descriptors
    pub fields: HashMap<String, ProtoFieldDescriptor>,
    /// Ordered list of field names
    pub field_order: Vec<String>,
}

/// Describes a single field in a message
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

/// Protobuf field types
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
    Message(String), // Nested message type name
    Enum(String),    // Enum type name
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

impl ProtoSchema {
    /// Parse a .proto file and create a schema
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;
        Self::from_string(&content)
    }

    /// Parse a .proto file content from string
    pub fn from_string(content: &str) -> Result<Self> {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Write content to a temporary file
        let mut temp_file = NamedTempFile::new()
            .map_err(|e| Error::ProtobufParse(format!("Failed to create temp file: {e}")))?;
        temp_file
            .write_all(content.as_bytes())
            .map_err(|e| Error::ProtobufParse(format!("Failed to write temp file: {e}")))?;
        let temp_path = temp_file.path();

        // Parse using protobuf_parse
        let mut parser = Parser::new();
        parser.input(temp_path);
        let parsed = parser
            .parse_and_typecheck()
            .map_err(|e| Error::ProtobufParse(e.to_string()))?;

        let file_descriptor = parsed
            .file_descriptors
            .into_iter()
            .next()
            .ok_or_else(|| Error::ProtobufParse("No file descriptor found".to_string()))?;

        let mut messages = HashMap::new();

        // Extract all message types from the file descriptor
        for message in &file_descriptor.message_type {
            let mut fields = HashMap::new();
            let mut field_order = Vec::new();

            for field in &message.field {
                let field_name = field.name.clone().unwrap_or_default();
                if field_name.is_empty() {
                    continue;
                }
                field_order.push(field_name.clone());

                let field_type = Self::parse_field_type(field)?;

                let descriptor = ProtoFieldDescriptor {
                    name: field_name.clone(),
                    number: field.number.unwrap_or(0),
                    field_type,
                    is_repeated: field.label
                        == Some(
                            protobuf::descriptor::field_descriptor_proto::Label::LABEL_REPEATED
                                .into(),
                        ),
                    is_optional: field.label
                        == Some(
                            protobuf::descriptor::field_descriptor_proto::Label::LABEL_OPTIONAL
                                .into(),
                        ),
                };

                fields.insert(field_name, descriptor);
            }

            let message_name_simple = message.name.clone().unwrap_or_default();
            let message_name = if let Some(ref package) = file_descriptor.package {
                format!("{package}.{message_name_simple}")
            } else {
                message_name_simple.clone()
            };

            messages.insert(
                message_name_simple,
                ProtoMessageDescriptor {
                    name: message_name,
                    fields,
                    field_order,
                },
            );
        }

        Ok(ProtoSchema { messages })
    }

    fn parse_field_type(field: &protobuf::descriptor::FieldDescriptorProto) -> Result<ProtoType> {
        use protobuf::descriptor::field_descriptor_proto::Type;

        let field_type_enum_or_unknown = field
            .type_
            .ok_or_else(|| Error::ProtobufParse("Field missing type".to_string()))?;

        // Convert EnumOrUnknown to the enum value
        let field_type_enum = field_type_enum_or_unknown.enum_value_or_default();

        Ok(match field_type_enum {
            Type::TYPE_DOUBLE => ProtoType::Double,
            Type::TYPE_FLOAT => ProtoType::Float,
            Type::TYPE_INT64 => ProtoType::Int64,
            Type::TYPE_UINT64 => ProtoType::Uint64,
            Type::TYPE_INT32 => ProtoType::Int32,
            Type::TYPE_FIXED64 => ProtoType::Fixed64,
            Type::TYPE_FIXED32 => ProtoType::Fixed32,
            Type::TYPE_BOOL => ProtoType::Bool,
            Type::TYPE_STRING => ProtoType::String,
            Type::TYPE_MESSAGE => {
                let type_name = field.type_name.clone().unwrap_or_default();
                ProtoType::Message(type_name)
            }
            Type::TYPE_BYTES => ProtoType::Bytes,
            Type::TYPE_UINT32 => ProtoType::Uint32,
            Type::TYPE_ENUM => {
                let type_name = field.type_name.clone().unwrap_or_default();
                ProtoType::Enum(type_name)
            }
            Type::TYPE_SFIXED32 => ProtoType::Sfixed32,
            Type::TYPE_SFIXED64 => ProtoType::Sfixed64,
            Type::TYPE_SINT32 => ProtoType::Sint32,
            Type::TYPE_SINT64 => ProtoType::Sint64,
            Type::TYPE_GROUP => {
                return Err(Error::ProtobufParse(
                    "TYPE_GROUP is Proto2 syntax only and deprecated hence not supported"
                        .to_string(),
                ))
            }
        })
    }

    /// Get a message descriptor by name
    pub fn get_message(&self, name: &str) -> Result<&ProtoMessageDescriptor> {
        self.messages
            .get(name)
            .ok_or_else(|| Error::MessageTypeNotFound(name.to_string()))
    }

    /// List all message types in the schema
    pub fn list_messages(&self) -> Vec<String> {
        self.messages.keys().cloned().collect()
    }
}

impl ProtoMessageDescriptor {
    /// Get a field descriptor by name
    pub fn get_field(&self, name: &str) -> Result<&ProtoFieldDescriptor> {
        self.fields
            .get(name)
            .ok_or_else(|| Error::FieldNotFound(name.to_string()))
    }

    /// List all field names in order
    pub fn list_fields(&self) -> &[String] {
        &self.field_order
    }
}
