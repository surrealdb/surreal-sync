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
        let p = path.as_ref();

        // Parse using protobuf_parse with pure parsing (no includes needed)
        let mut parser = Parser::new();
        parser.input(p);

        // Get parent directory of temp file for includes
        if let Some(parent) = p.parent() {
            parser.include(parent);
        }

        let parsed = parser
            .parse_and_typecheck()
            .map_err(|e| Error::ProtobufParse(e.to_string()))?;

        let file_descriptors = parsed.file_descriptors.into_iter();

        let mut messages = HashMap::new();

        // Extract all message types from the file descriptor
        for file_descriptor in file_descriptors {
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
        }

        Ok(ProtoSchema { messages })
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

        Self::from_file(temp_path)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_message() {
        let proto = r#"
            syntax = "proto3";

            message User {
                string name = 1;
                int32 age = 2;
                bool active = 3;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        assert_eq!(schema.messages.len(), 1);

        let user_msg = schema.get_message("User").expect("User message not found");
        assert_eq!(user_msg.fields.len(), 3);

        let name_field = user_msg.get_field("name").expect("name field not found");
        assert_eq!(name_field.name, "name");
        assert_eq!(name_field.number, 1);
        assert_eq!(name_field.field_type, ProtoType::String);
        assert!(!name_field.is_repeated);

        let age_field = user_msg.get_field("age").expect("age field not found");
        assert_eq!(age_field.field_type, ProtoType::Int32);
        assert_eq!(age_field.number, 2);

        let active_field = user_msg
            .get_field("active")
            .expect("active field not found");
        assert_eq!(active_field.field_type, ProtoType::Bool);
        assert_eq!(active_field.number, 3);
    }

    #[test]
    fn test_parse_with_package() {
        let proto = r#"
            syntax = "proto3";
            package myapp.v1;

            message Product {
                string id = 1;
                double price = 2;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let product_msg = schema
            .get_message("Product")
            .expect("Product message not found");

        assert_eq!(product_msg.name, "myapp.v1.Product");
        assert_eq!(product_msg.fields.len(), 2);

        let price_field = product_msg
            .get_field("price")
            .expect("price field not found");
        assert_eq!(price_field.field_type, ProtoType::Double);
    }

    #[test]
    fn test_parse_repeated_fields() {
        let proto = r#"
            syntax = "proto3";

            message Team {
                string name = 1;
                repeated string members = 2;
                repeated int64 scores = 3;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let team_msg = schema.get_message("Team").expect("Team message not found");

        let members_field = team_msg
            .get_field("members")
            .expect("members field not found");
        assert!(members_field.is_repeated);
        assert_eq!(members_field.field_type, ProtoType::String);

        let scores_field = team_msg
            .get_field("scores")
            .expect("scores field not found");
        assert!(scores_field.is_repeated);
        assert_eq!(scores_field.field_type, ProtoType::Int64);
    }

    #[test]
    fn test_parse_all_numeric_types() {
        let proto = r#"
            syntax = "proto3";

            message NumericTypes {
                int32 int32_field = 1;
                int64 int64_field = 2;
                uint32 uint32_field = 3;
                uint64 uint64_field = 4;
                sint32 sint32_field = 5;
                sint64 sint64_field = 6;
                fixed32 fixed32_field = 7;
                fixed64 fixed64_field = 8;
                sfixed32 sfixed32_field = 9;
                sfixed64 sfixed64_field = 10;
                float float_field = 11;
                double double_field = 12;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let msg = schema
            .get_message("NumericTypes")
            .expect("NumericTypes message not found");

        assert_eq!(
            msg.get_field("int32_field").unwrap().field_type,
            ProtoType::Int32
        );
        assert_eq!(
            msg.get_field("int64_field").unwrap().field_type,
            ProtoType::Int64
        );
        assert_eq!(
            msg.get_field("uint32_field").unwrap().field_type,
            ProtoType::Uint32
        );
        assert_eq!(
            msg.get_field("uint64_field").unwrap().field_type,
            ProtoType::Uint64
        );
        assert_eq!(
            msg.get_field("sint32_field").unwrap().field_type,
            ProtoType::Sint32
        );
        assert_eq!(
            msg.get_field("sint64_field").unwrap().field_type,
            ProtoType::Sint64
        );
        assert_eq!(
            msg.get_field("fixed32_field").unwrap().field_type,
            ProtoType::Fixed32
        );
        assert_eq!(
            msg.get_field("fixed64_field").unwrap().field_type,
            ProtoType::Fixed64
        );
        assert_eq!(
            msg.get_field("sfixed32_field").unwrap().field_type,
            ProtoType::Sfixed32
        );
        assert_eq!(
            msg.get_field("sfixed64_field").unwrap().field_type,
            ProtoType::Sfixed64
        );
        assert_eq!(
            msg.get_field("float_field").unwrap().field_type,
            ProtoType::Float
        );
        assert_eq!(
            msg.get_field("double_field").unwrap().field_type,
            ProtoType::Double
        );
    }

    #[test]
    fn test_parse_bytes_type() {
        let proto = r#"
            syntax = "proto3";

            message BinaryData {
                string id = 1;
                bytes content = 2;
                bytes metadata = 3;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let msg = schema
            .get_message("BinaryData")
            .expect("BinaryData message not found");

        let content_field = msg.get_field("content").expect("content field not found");
        assert_eq!(content_field.field_type, ProtoType::Bytes);

        let metadata_field = msg.get_field("metadata").expect("metadata field not found");
        assert_eq!(metadata_field.field_type, ProtoType::Bytes);
    }

    #[test]
    fn test_parse_nested_message() {
        let proto = r#"
            syntax = "proto3";

            message Address {
                string street = 1;
                string city = 2;
            }

            message Person {
                string name = 1;
                Address address = 2;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        assert_eq!(schema.messages.len(), 2);

        let address_msg = schema
            .get_message("Address")
            .expect("Address message not found");
        assert_eq!(address_msg.fields.len(), 2);

        let person_msg = schema
            .get_message("Person")
            .expect("Person message not found");
        let address_field = person_msg
            .get_field("address")
            .expect("address field not found");

        match &address_field.field_type {
            ProtoType::Message(type_name) => {
                assert!(type_name.contains("Address"));
            }
            _ => panic!("Expected Message type for address field"),
        }
    }

    #[test]
    fn test_parse_enum() {
        let proto = r#"
            syntax = "proto3";

            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
                INACTIVE = 2;
            }

            message Entity {
                string id = 1;
                Status status = 2;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let entity_msg = schema
            .get_message("Entity")
            .expect("Entity message not found");

        let status_field = entity_msg
            .get_field("status")
            .expect("status field not found");
        match &status_field.field_type {
            ProtoType::Enum(type_name) => {
                assert!(type_name.contains("Status"));
            }
            _ => panic!("Expected Enum type for status field"),
        }
    }

    #[test]
    fn test_parse_optional_fields() {
        let proto = r#"
            syntax = "proto3";

            message OptionalExample {
                string implicit_field = 1;
                optional string optional_field = 2;
                optional int32 optional_number = 3;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let msg = schema
            .get_message("OptionalExample")
            .expect("OptionalExample message not found");

        // In proto3, implicit fields are neither optional nor required
        let implicit_field = msg
            .get_field("implicit_field")
            .expect("implicit_field not found");
        assert!(implicit_field.is_optional);

        let optional_field = msg
            .get_field("optional_field")
            .expect("optional_field not found");
        assert!(optional_field.is_optional);

        let optional_number = msg
            .get_field("optional_number")
            .expect("optional_number not found");
        assert!(optional_number.is_optional);
    }

    #[test]
    fn test_parse_multiple_messages() {
        let proto = r#"
            syntax = "proto3";

            message User {
                string name = 1;
                int32 age = 2;
            }

            message Post {
                string title = 1;
                string content = 2;
                string author_id = 3;
            }

            message Comment {
                string text = 1;
                string post_id = 2;
                string user_id = 3;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        assert_eq!(schema.messages.len(), 3);

        assert!(schema.get_message("User").is_ok());
        assert!(schema.get_message("Post").is_ok());
        assert!(schema.get_message("Comment").is_ok());

        let messages = schema.list_messages();
        assert_eq!(messages.len(), 3);
    }

    #[test]
    fn test_parse_field_order() {
        let proto = r#"
            syntax = "proto3";

            message OrderedMessage {
                string field_a = 1;
                int32 field_b = 2;
                bool field_c = 3;
                double field_d = 4;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let msg = schema
            .get_message("OrderedMessage")
            .expect("OrderedMessage not found");

        let field_order = msg.list_fields();
        assert_eq!(field_order.len(), 4);
        assert_eq!(field_order, &["field_a", "field_b", "field_c", "field_d"]);
    }

    #[test]
    fn test_parse_complex_schema() {
        let proto = r#"
            syntax = "proto3";
            package ecommerce.v1;

            enum OrderStatus {
                PENDING = 0;
                PROCESSING = 1;
                SHIPPED = 2;
                DELIVERED = 3;
            }

            message Product {
                string id = 1;
                string name = 2;
                double price = 3;
                repeated string tags = 4;
            }

            message Order {
                string order_id = 1;
                repeated Product products = 2;
                OrderStatus status = 3;
                optional string tracking_number = 4;
                int64 created_at = 5;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        assert_eq!(schema.messages.len(), 2);

        let product_msg = schema.get_message("Product").expect("Product not found");
        assert_eq!(product_msg.name, "ecommerce.v1.Product");
        assert!(product_msg.get_field("tags").unwrap().is_repeated);

        let order_msg = schema.get_message("Order").expect("Order not found");
        assert_eq!(order_msg.name, "ecommerce.v1.Order");
        assert!(order_msg.get_field("products").unwrap().is_repeated);
        assert!(order_msg.get_field("tracking_number").unwrap().is_optional);
    }

    #[test]
    fn test_parse_invalid_proto() {
        let proto = r#"
            syntax = "proto3";

            message InvalidMessage {
                string name
            }
        "#;

        let result = ProtoSchema::from_string(proto);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_nonexistent_message() {
        let proto = r#"
            syntax = "proto3";

            message User {
                string name = 1;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let result = schema.get_message("NonExistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_nonexistent_field() {
        let proto = r#"
            syntax = "proto3";

            message User {
                string name = 1;
            }
        "#;

        let schema = ProtoSchema::from_string(proto).expect("Failed to parse proto");
        let user_msg = schema.get_message("User").expect("User not found");
        let result = user_msg.get_field("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_proto_type_display() {
        assert_eq!(ProtoType::String.to_string(), "string");
        assert_eq!(ProtoType::Int32.to_string(), "int32");
        assert_eq!(ProtoType::Bool.to_string(), "bool");
        assert_eq!(ProtoType::Bytes.to_string(), "bytes");
        assert_eq!(
            ProtoType::Message("Address".to_string()).to_string(),
            "message:Address"
        );
        assert_eq!(
            ProtoType::Enum("Status".to_string()).to_string(),
            "enum:Status"
        );
    }
}
