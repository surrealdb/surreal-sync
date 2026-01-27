//! Forward conversion: TypedValue → Protobuf encoding.
//!
//! This module encodes TypedValue/UniversalValue to protobuf binary format
//! using the schema information from TableSchema.

use crate::error::{KafkaTypesError, Result};
use protobuf::CodedOutputStream;
use std::collections::HashMap;
use sync_core::{
    GeneratorTableDefinition, TypedValue, UniversalRow, UniversalType, UniversalValue,
};

/// Encode an UniversalRow to protobuf binary format.
///
/// The encoding follows proto3 wire format:
/// - Each field is encoded as (tag, value) pairs
/// - Tag = (field_number << 3) | wire_type
/// - Wire types: 0=varint, 1=64-bit, 2=length-delimited, 5=32-bit
pub fn encode_row(row: &UniversalRow, table_schema: &GeneratorTableDefinition) -> Result<Vec<u8>> {
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
    table_schema: &GeneratorTableDefinition,
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
        UniversalValue::Null => {
            // Skip null values (proto3 default behavior)
        }
        UniversalValue::Bool(b) => {
            // Wire type 0 (varint)
            stream
                .write_bool(field_number, *b)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Integer types - strict 1:1 matching
        UniversalValue::Int8 { value: i, .. } => {
            stream
                .write_int64(field_number, *i as i64)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Int16(i) => {
            stream
                .write_int64(field_number, *i as i64)
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

        // Float types - strict 1:1 matching
        UniversalValue::Float32(f) => {
            stream
                .write_float(field_number, *f)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Float64(f) => {
            // Wire type 1 (64-bit)
            stream
                .write_double(field_number, *f)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Decimal { value, .. } => {
            // Encode decimal as string to preserve precision
            stream
                .write_string(field_number, value)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // String types - strict 1:1 matching
        UniversalValue::Text(s) => {
            stream
                .write_string(field_number, s)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Char { value: s, .. } => {
            stream
                .write_string(field_number, s)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::VarChar { value: s, .. } => {
            stream
                .write_string(field_number, s)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Binary types - strict 1:1 matching
        UniversalValue::Bytes(b) => {
            stream
                .write_bytes(field_number, b)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Blob(b) => {
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

        UniversalValue::Ulid(u) => {
            // Encode ULID as string
            stream
                .write_string(field_number, &u.to_string())
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // DateTime types - strict 1:1 matching
        UniversalValue::LocalDateTime(dt)
        | UniversalValue::LocalDateTimeNano(dt)
        | UniversalValue::ZonedDateTime(dt) => {
            // Encode as google.protobuf.Timestamp (nested message)
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
            stream
                .write_bytes(field_number, &timestamp_bytes)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Date(dt) => {
            // Encode date as string
            stream
                .write_string(field_number, &dt.format("%Y-%m-%d").to_string())
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }
        UniversalValue::Time(dt) => {
            // Encode time as string
            stream
                .write_string(field_number, &dt.format("%H:%M:%S").to_string())
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // JSON types - strict 1:1 matching
        UniversalValue::Json(obj) | UniversalValue::Jsonb(obj) => {
            let json = serde_json::to_string(obj)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
            stream
                .write_string(field_number, &json)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Enum type - strict 1:1 matching
        UniversalValue::Enum { value, .. } => {
            stream
                .write_string(field_number, value)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Set type - encode as repeated strings
        UniversalValue::Set { elements, .. } => {
            for element in elements {
                stream
                    .write_string(field_number, element)
                    .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
            }
        }

        // Geometry type - encode as GeoJSON string
        UniversalValue::Geometry { data, .. } => {
            use sync_core::GeometryData;
            let GeometryData(json) = data;
            let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string());
            stream
                .write_string(field_number, &json_str)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Array type - recursive handling
        UniversalValue::Array { elements: arr, .. } => {
            // Encode as repeated field - each element is written separately
            for element in arr {
                encode_generated_value(stream, field_number, element)?;
            }
        }

        // Duration type - encode as ISO 8601 duration string
        UniversalValue::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            let duration_str = if nanos == 0 {
                format!("PT{secs}S")
            } else {
                format!("PT{secs}.{nanos:09}S")
            };
            stream
                .write_string(field_number, &duration_str)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Thing - encode as string in "table:id" format
        UniversalValue::Thing { table, id } => {
            let id_str = match id.as_ref() {
                UniversalValue::Text(s) => s.clone(),
                UniversalValue::Int32(i) => i.to_string(),
                UniversalValue::Int64(i) => i.to_string(),
                UniversalValue::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for Kafka: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            let thing_str = format!("{table}:{id_str}");
            stream
                .write_string(field_number, &thing_str)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
        }

        // Object - encode as JSON string
        UniversalValue::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_value_to_json(v)))
                .collect();
            let json_str = serde_json::to_string(&serde_json::Value::Object(obj))
                .unwrap_or_else(|_| "{}".to_string());
            stream
                .write_string(field_number, &json_str)
                .map_err(|e| KafkaTypesError::ProtobufEncode(e.to_string()))?;
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
        // Integer types
        UniversalValue::Int8 { value: i, .. } => i.to_string().into_bytes(),
        UniversalValue::Int16(i) => i.to_string().into_bytes(),
        UniversalValue::Int32(i) => i.to_string().into_bytes(),
        UniversalValue::Int64(i) => i.to_string().into_bytes(),
        // String types
        UniversalValue::Text(s) => s.as_bytes().to_vec(),
        UniversalValue::Char { value: s, .. } => s.as_bytes().to_vec(),
        UniversalValue::VarChar { value: s, .. } => s.as_bytes().to_vec(),
        // UUID
        UniversalValue::Uuid(u) => u.to_string().into_bytes(),
        // Fallback - try to produce meaningful key
        _ => format!("{value:?}").into_bytes(),
    }
}

/// Map UniversalType to protobuf type string.
///
/// Used for generating .proto schema files.
pub fn get_proto_type(sync_type: &UniversalType) -> &'static str {
    match sync_type {
        UniversalType::Bool => "bool",
        UniversalType::Int8 { .. } | UniversalType::Int16 | UniversalType::Int32 => "int32",
        UniversalType::Int64 => "int64",
        UniversalType::Float32 => "float",
        UniversalType::Float64 => "double",
        UniversalType::Decimal { .. } => "string",
        UniversalType::Char { .. }
        | UniversalType::VarChar { .. }
        | UniversalType::Text
        | UniversalType::Uuid
        | UniversalType::Ulid
        | UniversalType::Enum { .. } => "string",
        UniversalType::Bytes | UniversalType::Blob => "bytes",
        UniversalType::LocalDateTime
        | UniversalType::LocalDateTimeNano
        | UniversalType::ZonedDateTime
        | UniversalType::Date
        | UniversalType::Time => "google.protobuf.Timestamp",
        UniversalType::Json | UniversalType::Jsonb => "string", // JSON encoded as string
        UniversalType::Array { .. } => "repeated",              // Caller handles element type
        UniversalType::Set { .. } => "repeated",                // Encode as repeated
        UniversalType::Geometry { .. } => "string",             // GeoJSON string
        UniversalType::Duration => "string",                    // ISO 8601 duration string
        UniversalType::Thing => "string",                       // Record reference as table:id
        UniversalType::Object => "string",                      // Object encoded as JSON string
    }
}

/// Convert UniversalValue to serde_json::Value for JSON encoding.
fn universal_value_to_json(value: &UniversalValue) -> serde_json::Value {
    match value {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => serde_json::Value::Bool(*b),
        UniversalValue::Int8 { value, .. } => serde_json::json!(*value),
        UniversalValue::Int16(i) => serde_json::json!(*i),
        UniversalValue::Int32(i) => serde_json::json!(*i),
        UniversalValue::Int64(i) => serde_json::json!(*i),
        UniversalValue::Float32(f) => serde_json::json!(*f),
        UniversalValue::Float64(f) => serde_json::json!(*f),
        UniversalValue::Decimal { value, .. } => serde_json::json!(value),
        UniversalValue::Char { value, .. } => serde_json::json!(value),
        UniversalValue::VarChar { value, .. } => serde_json::json!(value),
        UniversalValue::Text(s) => serde_json::json!(s),
        UniversalValue::Blob(b) | UniversalValue::Bytes(b) => {
            serde_json::json!(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b
            ))
        }
        UniversalValue::Uuid(u) => serde_json::json!(u.to_string()),
        UniversalValue::Ulid(u) => serde_json::json!(u.to_string()),
        UniversalValue::Date(dt)
        | UniversalValue::Time(dt)
        | UniversalValue::LocalDateTime(dt)
        | UniversalValue::LocalDateTimeNano(dt)
        | UniversalValue::ZonedDateTime(dt) => serde_json::json!(dt.to_rfc3339()),
        UniversalValue::Json(v) | UniversalValue::Jsonb(v) => (**v).clone(),
        UniversalValue::Array { elements, .. } => {
            serde_json::Value::Array(elements.iter().map(universal_value_to_json).collect())
        }
        UniversalValue::Set { elements, .. } => {
            serde_json::Value::Array(elements.iter().map(|s| serde_json::json!(s)).collect())
        }
        UniversalValue::Enum { value, .. } => serde_json::json!(value),
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(v) = data;
            v.clone()
        }
        UniversalValue::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                serde_json::json!(format!("PT{secs}S"))
            } else {
                serde_json::json!(format!("PT{secs}.{nanos:09}S"))
            }
        }
        UniversalValue::Thing { table, id } => {
            let id_str = match id.as_ref() {
                UniversalValue::Text(s) => s.clone(),
                UniversalValue::Int32(i) => i.to_string(),
                UniversalValue::Int64(i) => i.to_string(),
                UniversalValue::Uuid(u) => u.to_string(),
                other => format!("{other:?}"),
            };
            serde_json::json!(format!("{table}:{id_str}"))
        }
        UniversalValue::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use protobuf::CodedInputStream;
    use sync_core::{FieldDefinition, GeneratorConfig, IDDefinition};

    fn test_table_schema() -> GeneratorTableDefinition {
        GeneratorTableDefinition {
            name: "users".to_string(),
            id: IDDefinition {
                id_type: UniversalType::Int64,
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
                    field_type: UniversalType::Int32,
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
            UniversalValue::Text("test@example.com".to_string()),
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
        let schema = GeneratorTableDefinition {
            name: "events".to_string(),
            id: IDDefinition {
                id_type: UniversalType::Int64,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![FieldDefinition {
                name: "created_at".to_string(),
                field_type: UniversalType::LocalDateTime,
                generator: GeneratorConfig::TimestampRange {
                    start: "2024-01-01T00:00:00Z".to_string(),
                    end: "2024-12-31T23:59:59Z".to_string(),
                },
                nullable: false,
            }],
        };

        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 45).unwrap();
        let mut fields = HashMap::new();
        fields.insert("created_at".to_string(), UniversalValue::LocalDateTime(dt));

        let row = UniversalRow::new("events", 0, UniversalValue::Int64(1), fields);
        let encoded = encode_row(&row, &schema).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_array() {
        let schema = GeneratorTableDefinition {
            name: "products".to_string(),
            id: IDDefinition {
                id_type: UniversalType::Int64,
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
            UniversalValue::Array {
                elements: vec![
                    UniversalValue::Text("tag1".to_string()),
                    UniversalValue::Text("tag2".to_string()),
                ],
                element_type: Box::new(UniversalType::Text),
            },
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
            UniversalValue::Text("my-key".to_string()),
            HashMap::new(),
        );
        let key = get_message_key(&row);
        assert_eq!(key, b"my-key");
    }
}
