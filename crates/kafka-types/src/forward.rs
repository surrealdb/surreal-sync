//! Forward conversion: TypedValue → Protobuf encoding.
//!
//! This module encodes TypedValue/UniversalValue to protobuf binary format
//! using the schema information from TableSchema.

use crate::error::{KafkaTypesError, Result};
use protobuf::CodedOutputStream;
use std::collections::HashMap;
use sync_core::{TableDefinition, TypedValue, UniversalRow, UniversalType, UniversalValue};

/// Encode an UniversalRow to protobuf binary format.
///
/// The encoding follows proto3 wire format:
/// - Each field is encoded as (tag, value) pairs
/// - Tag = (field_number << 3) | wire_type
/// - Wire types: 0=varint, 1=64-bit, 2=length-delimited, 5=32-bit
pub fn encode_row(row: &UniversalRow, table_schema: &TableDefinition) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut stream = CodedOutputStream::vec(&mut buffer);

        // Field number 1 is always the id
        encode_generated_value(&mut stream, 1, &row.id)?;

        // Encode remaining fields in order (starting from field number 2)
        let mut field_number = 2u32;
        for field_schema in &table_schema.fields {
            if let Some(value) = row.fields.get(&field_schema.name) {
                encode_generated_value(&mut stream, field_number, value)?;
            }
            field_number += 1;
        }

        stream
            .flush()
            .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
    }

    Ok(buffer)
}

/// Encode a TypedValue map to protobuf binary format.
///
/// This is an alternative entry point that works with TypedValue maps
/// rather than InternalRow.
pub fn encode_typed_values(
    values: &HashMap<String, TypedValue>,
    table_schema: &TableDefinition,
) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut stream = CodedOutputStream::vec(&mut buffer);

        // Field number 1 is always the id (if present)
        let pk_field = &table_schema.id.id_type;
        let pk_name = get_pk_field_name(pk_field);
        if let Some(id_value) = values.get(&pk_name) {
            encode_typed_value(&mut stream, 1, id_value)?;
        }

        // Encode remaining fields in order (starting from field number 2)
        let mut field_number = 2u32;
        for field_schema in &table_schema.fields {
            if let Some(value) = values.get(&field_schema.name) {
                encode_typed_value(&mut stream, field_number, value)?;
            }
            field_number += 1;
        }

        stream
            .flush()
            .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
    }

    Ok(buffer)
}

/// Get the primary key field name based on the type.
fn get_pk_field_name(_pk_type: &UniversalType) -> String {
    "id".to_string()
}

/// Encode a single TypedValue with its field number.
pub fn encode_typed_value(
    stream: &mut CodedOutputStream,
    field_number: u32,
    value: &TypedValue,
) -> Result<()> {
    encode_generated_value(stream, field_number, &value.value)
}

/// Encode a single UniversalValue with its field number.
pub fn encode_generated_value(
    stream: &mut CodedOutputStream,
    field_number: u32,
    value: &UniversalValue,
) -> Result<()> {
    match value {
        UniversalValue::Bool(b) => {
            // Wire type 0 (varint)
            stream
                .write_bool(field_number, *b)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Int32(i) => {
            // Wire type 0 (varint) - encode as int64 for proto3 compatibility
            stream
                .write_int64(field_number, *i as i64)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Int64(i) => {
            // Wire type 0 (varint)
            stream
                .write_int64(field_number, *i)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Float64(f) => {
            // Wire type 1 (64-bit)
            stream
                .write_double(field_number, *f)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::String(s) => {
            // Wire type 2 (length-delimited)
            stream
                .write_string(field_number, s)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Bytes(b) => {
            // Wire type 2 (length-delimited)
            stream
                .write_bytes(field_number, b)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Uuid(u) => {
            // Encode UUID as string
            stream
                .write_string(field_number, &u.to_string())
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::DateTime(dt) => {
            // Encode as google.protobuf.Timestamp (nested message)
            // Timestamp has: int64 seconds = 1, int32 nanos = 2
            let mut timestamp_bytes = Vec::new();
            {
                let mut ts_stream = CodedOutputStream::vec(&mut timestamp_bytes);
                ts_stream
                    .write_int64(1, dt.timestamp())
                    .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
                ts_stream
                    .write_int32(2, dt.timestamp_subsec_nanos() as i32)
                    .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
                ts_stream
                    .flush()
                    .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
            }
            // Write as length-delimited message
            stream
                .write_bytes(field_number, &timestamp_bytes)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Decimal { value, .. } => {
            // Encode decimal as string to preserve precision
            stream
                .write_string(field_number, value)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Array(arr) => {
            // Encode as repeated field - each element is written separately
            for element in arr {
                encode_generated_value(stream, field_number, element)?;
            }
        }
        UniversalValue::Object(obj) => {
            // Encode object as JSON string
            let json = serde_json::to_string(obj)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
            stream
                .write_string(field_number, &json)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Null => {
            // Skip null values (proto3 default behavior)
        }
    }
    Ok(())
}

/// Get the Kafka message key from an UniversalRow.
///
/// Uses the id field value as the message key.
pub fn get_message_key(row: &UniversalRow) -> Vec<u8> {
    generated_value_to_key(&row.id)
}

/// Get the Kafka message key from a TypedValue map.
///
/// Uses the id field value as the message key.
pub fn get_message_key_from_typed_values(
    values: &HashMap<String, TypedValue>,
    pk_field: &str,
) -> Vec<u8> {
    if let Some(id) = values.get(pk_field) {
        generated_value_to_key(&id.value)
    } else {
        Vec::new()
    }
}

/// Convert a UniversalValue to message key bytes.
fn generated_value_to_key(value: &UniversalValue) -> Vec<u8> {
    match value {
        UniversalValue::Int64(i) => i.to_string().into_bytes(),
        UniversalValue::Int32(i) => i.to_string().into_bytes(),
        UniversalValue::String(s) => s.as_bytes().to_vec(),
        UniversalValue::Uuid(u) => u.to_string().into_bytes(),
        _ => format!("{value:?}").into_bytes(),
    }
}

/// Map UniversalType to protobuf type string.
///
/// Used for generating .proto schema files.
pub fn get_proto_type(sync_type: &UniversalType) -> &'static str {
    match sync_type {
        UniversalType::Bool => "bool",
        UniversalType::TinyInt { .. } | UniversalType::SmallInt | UniversalType::Int => "int32",
        UniversalType::BigInt => "int64",
        UniversalType::Float => "float",
        UniversalType::Double => "double",
        UniversalType::Decimal { .. } => "string",
        UniversalType::Char { .. }
        | UniversalType::VarChar { .. }
        | UniversalType::Text
        | UniversalType::Uuid
        | UniversalType::Enum { .. } => "string",
        UniversalType::Bytes | UniversalType::Blob => "bytes",
        UniversalType::DateTime
        | UniversalType::DateTimeNano
        | UniversalType::TimestampTz
        | UniversalType::Date
        | UniversalType::Time => "google.protobuf.Timestamp",
        UniversalType::Json | UniversalType::Jsonb => "string", // JSON encoded as string
        UniversalType::Array { .. } => "repeated",              // Caller handles element type
        UniversalType::Set { .. } => "repeated",                // Encode as repeated
        UniversalType::Geometry { .. } => "string",             // GeoJSON string
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use protobuf::CodedInputStream;
    use sync_core::{FieldDefinition, GeneratorConfig, IDDefinition};

    fn test_table_schema() -> TableDefinition {
        TableDefinition {
            name: "users".to_string(),
            id: IDDefinition {
                id_type: UniversalType::BigInt,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![
                FieldDefinition {
                    name: "email".to_string(),
                    field_type: UniversalType::VarChar { length: 255 },
                    generator: GeneratorConfig::Pattern {
                        pattern: "user_{index}@test.com".to_string(),
                    },
                    nullable: false,
                },
                FieldDefinition {
                    name: "age".to_string(),
                    field_type: UniversalType::Int,
                    generator: GeneratorConfig::IntRange { min: 18, max: 80 },
                    nullable: false,
                },
                FieldDefinition {
                    name: "is_active".to_string(),
                    field_type: UniversalType::Bool,
                    generator: GeneratorConfig::WeightedBool { true_weight: 0.8 },
                    nullable: false,
                },
            ],
        }
    }

    #[test]
    fn test_encode_simple_row() {
        let schema = test_table_schema();
        let mut fields = HashMap::new();
        fields.insert(
            "email".to_string(),
            UniversalValue::String("test@example.com".to_string()),
        );
        fields.insert("age".to_string(), UniversalValue::Int32(25));
        fields.insert("is_active".to_string(), UniversalValue::Bool(true));

        let row = UniversalRow::new("users", 0, UniversalValue::Int64(1), fields);

        let encoded = encode_row(&row, &schema).unwrap();
        assert!(!encoded.is_empty());

        // Verify we can read the encoded data
        let mut stream = CodedInputStream::from_bytes(&encoded);

        // Read id (field 1)
        let tag = stream.read_raw_varint32().unwrap();
        assert_eq!(tag >> 3, 1); // field number 1
        let id = stream.read_int64().unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_encode_datetime() {
        let schema = TableDefinition {
            name: "events".to_string(),
            id: IDDefinition {
                id_type: UniversalType::BigInt,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![FieldDefinition {
                name: "created_at".to_string(),
                field_type: UniversalType::DateTime,
                generator: GeneratorConfig::TimestampRange {
                    start: "2024-01-01T00:00:00Z".to_string(),
                    end: "2024-12-31T23:59:59Z".to_string(),
                },
                nullable: false,
            }],
        };

        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 45).unwrap();
        let mut fields = HashMap::new();
        fields.insert("created_at".to_string(), UniversalValue::DateTime(dt));

        let row = UniversalRow::new("events", 0, UniversalValue::Int64(1), fields);
        let encoded = encode_row(&row, &schema).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_array() {
        let schema = TableDefinition {
            name: "products".to_string(),
            id: IDDefinition {
                id_type: UniversalType::BigInt,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![FieldDefinition {
                name: "tags".to_string(),
                field_type: UniversalType::Array {
                    element_type: Box::new(UniversalType::Text),
                },
                generator: GeneratorConfig::SampleArray {
                    pool: vec!["a".to_string(), "b".to_string()],
                    min_length: 1,
                    max_length: 3,
                },
                nullable: false,
            }],
        };

        let mut fields = HashMap::new();
        fields.insert(
            "tags".to_string(),
            UniversalValue::Array(vec![
                UniversalValue::String("tag1".to_string()),
                UniversalValue::String("tag2".to_string()),
            ]),
        );

        let row = UniversalRow::new("products", 0, UniversalValue::Int64(1), fields);
        let encoded = encode_row(&row, &schema).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_get_message_key_int() {
        let row = UniversalRow::new("test", 0, UniversalValue::Int64(42), HashMap::new());
        let key = get_message_key(&row);
        assert_eq!(key, b"42");
    }

    #[test]
    fn test_get_message_key_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let row = UniversalRow::new("test", 0, UniversalValue::Uuid(uuid), HashMap::new());
        let key = get_message_key(&row);
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_get_message_key_string() {
        let row = UniversalRow::new(
            "test",
            0,
            UniversalValue::String("my-key".to_string()),
            HashMap::new(),
        );
        let key = get_message_key(&row);
        assert_eq!(key, b"my-key");
    }
}
