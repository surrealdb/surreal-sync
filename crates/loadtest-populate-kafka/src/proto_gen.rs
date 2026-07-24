//! Proto schema generation from Schema.
//!
//! This module generates .proto file content from a Schema table definition,
//! enabling dynamic protobuf schema generation for Kafka loadtesting.

use sync_core::{GeneratorTableDefinition, Type};

/// Information about a proto field type.
struct ProtoTypeInfo {
    /// The protobuf type name (e.g., "int64", "string", "google.protobuf.Timestamp")
    type_name: String,
    /// Whether this type requires an import
    requires_timestamp_import: bool,
}

/// Generate a .proto file content from a table schema.
///
/// The message name is the PascalCase of table name (e.g., "order_items" -> "OrderItems").
pub fn generate_proto_for_table(
    table_schema: &GeneratorTableDefinition,
    package_name: &str,
) -> String {
    let mut proto = String::new();

    // Syntax declaration
    proto.push_str("syntax = \"proto3\";\n");
    proto.push_str(&format!("package {package_name};\n\n"));

    // Check if we need timestamp import
    let needs_timestamp = table_schema.fields.iter().any(|f| {
        matches!(
            f.field_type,
            Type::LocalDateTime | Type::LocalDateTimeNano | Type::ZonedDateTime
        )
    });

    if needs_timestamp {
        proto.push_str("import \"google/protobuf/timestamp.proto\";\n\n");
    }

    // Message name is PascalCase of table name (e.g., "order_items" -> "OrderItems")
    let message_name = to_pascal_case(&table_schema.name);
    proto.push_str(&format!("message {message_name} {{\n"));

    // Field number counter (starts at 1)
    let mut field_number = 1;

    // Add the id field first
    let id_proto_type = sync_type_to_proto_type(&table_schema.id.id_type);
    proto.push_str(&format!(
        "  {} id = {};\n",
        id_proto_type.type_name, field_number
    ));
    field_number += 1;

    // Add each field
    for field in &table_schema.fields {
        let proto_type = sync_type_to_proto_type(&field.field_type);
        proto.push_str(&format!(
            "  {} {} = {};\n",
            proto_type.type_name, field.name, field_number
        ));
        field_number += 1;
    }

    proto.push_str("}\n");

    proto
}

/// Convert a Type to protobuf type information.
fn sync_type_to_proto_type(sync_type: &Type) -> ProtoTypeInfo {
    match sync_type {
        // Boolean
        Type::Bool => ProtoTypeInfo {
            type_name: "bool".to_string(),
            requires_timestamp_import: false,
        },

        // Integer types -> int64 (safest for all integer ranges)
        Type::Int8 { .. } | Type::Int16 | Type::Int32 | Type::Int64 => ProtoTypeInfo {
            type_name: "int64".to_string(),
            requires_timestamp_import: false,
        },

        // Floating point
        Type::Float32 => ProtoTypeInfo {
            type_name: "float".to_string(),
            requires_timestamp_import: false,
        },
        Type::Float64 => ProtoTypeInfo {
            type_name: "double".to_string(),
            requires_timestamp_import: false,
        },

        // Decimal -> string (preserve precision)
        Type::Decimal { .. } => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // String types
        Type::Char { .. } | Type::VarChar { .. } | Type::Text => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // Binary types
        Type::Blob | Type::Bytes => ProtoTypeInfo {
            type_name: "bytes".to_string(),
            requires_timestamp_import: false,
        },

        // Temporal types -> google.protobuf.Timestamp
        Type::LocalDateTime | Type::LocalDateTimeNano | Type::ZonedDateTime => ProtoTypeInfo {
            type_name: "google.protobuf.Timestamp".to_string(),
            requires_timestamp_import: true,
        },

        // Date and Time -> string (ISO format)
        Type::Date | Type::Time => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // UUID -> string
        Type::Uuid => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // ULID -> string
        Type::Ulid => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // JSON types -> string (serialized JSON)
        Type::Json | Type::Jsonb => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // Array -> repeated
        Type::Array { element_type } => {
            let inner = sync_type_to_proto_type(element_type);
            ProtoTypeInfo {
                type_name: format!("repeated {}", inner.type_name),
                requires_timestamp_import: inner.requires_timestamp_import,
            }
        }

        // Set and Enum -> string
        Type::Set { .. } | Type::Enum { .. } => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // Geometry -> string (GeoJSON or WKT)
        Type::Geometry { .. } => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // Duration -> string (ISO 8601 duration format)
        Type::Duration => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // Thing -> string (table:id format)
        Type::Thing => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // Object -> string (serialized JSON)
        Type::Object => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },

        // TimeTz -> string (time with timezone preserved as string)
        Type::TimeTz => ProtoTypeInfo {
            type_name: "string".to_string(),
            requires_timestamp_import: false,
        },
    }
}

/// Convert a snake_case string to PascalCase.
/// e.g., "order_items" -> "OrderItems", "users" -> "Users"
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::Schema;

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
tables:
  - name: users
    id:
      type: big_int
      generator:
        type: sequential
        start: 1
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@test.com"
      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80
      - name: is_active
        type: bool
        generator:
          type: weighted_bool
          true_weight: 0.8
      - name: score
        type: double
        generator:
          type: float_range
          min: 0.0
          max: 100.0
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_generate_proto_for_users() {
        let schema = test_schema();
        let table = schema.get_table("users").unwrap();
        let proto = generate_proto_for_table(table, "loadtest");

        assert!(proto.contains("syntax = \"proto3\";"));
        assert!(proto.contains("package loadtest;"));
        assert!(proto.contains("message Users {"));
        assert!(proto.contains("int64 id = 1;"));
        assert!(proto.contains("string email = 2;"));
        assert!(proto.contains("int64 age = 3;"));
        assert!(proto.contains("bool is_active = 4;"));
        assert!(proto.contains("double score = 5;"));
    }

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("users"), "Users");
        assert_eq!(to_pascal_case("orders"), "Orders");
        assert_eq!(to_pascal_case("order_items"), "OrderItems");
        assert_eq!(to_pascal_case(""), "");
        assert_eq!(to_pascal_case("a"), "A");
        assert_eq!(to_pascal_case("foo_bar_baz"), "FooBarBaz");
    }

    #[test]
    fn test_proto_type_mapping() {
        // Boolean
        let info = sync_type_to_proto_type(&Type::Bool);
        assert_eq!(info.type_name, "bool");
        assert!(!info.requires_timestamp_import);

        // Integer types
        let info = sync_type_to_proto_type(&Type::Int64);
        assert_eq!(info.type_name, "int64");

        // Float
        let info = sync_type_to_proto_type(&Type::Float32);
        assert_eq!(info.type_name, "float");

        // DateTime
        let info = sync_type_to_proto_type(&Type::LocalDateTime);
        assert_eq!(info.type_name, "google.protobuf.Timestamp");
        assert!(info.requires_timestamp_import);

        // Array<Int>
        let info = sync_type_to_proto_type(&Type::Array {
            element_type: Box::new(Type::Int32),
        });
        assert_eq!(info.type_name, "repeated int64");
    }
}
