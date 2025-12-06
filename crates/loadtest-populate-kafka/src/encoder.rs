//! Protobuf encoder for InternalRow to wire format.
//!
//! This module encodes InternalRow values into protobuf binary format
//! using the schema information from ProtoMessageDescriptor.

use crate::error::KafkaPopulatorError;
use protobuf::CodedOutputStream;
use sync_core::{GeneratedValue, InternalRow, TableSchema};

/// Result type for encoder operations.
pub type Result<T> = std::result::Result<T, KafkaPopulatorError>;

/// Encode an InternalRow to protobuf binary format.
///
/// The encoding follows proto3 wire format:
/// - Each field is encoded as (tag, value) pairs
/// - Tag = (field_number << 3) | wire_type
/// - Wire types: 0=varint, 1=64-bit, 2=length-delimited, 5=32-bit
pub fn encode_row(row: &InternalRow, table_schema: &TableSchema) -> Result<Vec<u8>> {
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
            .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
    }

    Ok(buffer)
}

/// Encode a single GeneratedValue with its field number.
fn encode_generated_value(
    stream: &mut CodedOutputStream,
    field_number: u32,
    value: &GeneratedValue,
) -> Result<()> {
    match value {
        GeneratedValue::Bool(b) => {
            // Wire type 0 (varint)
            stream
                .write_bool(field_number, *b)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Int32(i) => {
            // Wire type 0 (varint) - encode as int64 for proto3 compatibility
            stream
                .write_int64(field_number, *i as i64)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Int64(i) => {
            // Wire type 0 (varint)
            stream
                .write_int64(field_number, *i)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Float64(f) => {
            // Wire type 1 (64-bit)
            stream
                .write_double(field_number, *f)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::String(s) => {
            // Wire type 2 (length-delimited)
            stream
                .write_string(field_number, s)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Bytes(b) => {
            // Wire type 2 (length-delimited)
            stream
                .write_bytes(field_number, b)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Uuid(u) => {
            // Encode UUID as string
            stream
                .write_string(field_number, &u.to_string())
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::DateTime(dt) => {
            // Encode as google.protobuf.Timestamp (nested message)
            // Timestamp has: int64 seconds = 1, int32 nanos = 2
            let mut timestamp_bytes = Vec::new();
            {
                let mut ts_stream = CodedOutputStream::vec(&mut timestamp_bytes);
                ts_stream
                    .write_int64(1, dt.timestamp())
                    .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
                ts_stream
                    .write_int32(2, dt.timestamp_subsec_nanos() as i32)
                    .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
                ts_stream
                    .flush()
                    .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
            }
            // Write as length-delimited message
            stream
                .write_bytes(field_number, &timestamp_bytes)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Decimal { value, .. } => {
            // Encode decimal as string to preserve precision
            stream
                .write_string(field_number, value)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Array(arr) => {
            // Encode as repeated field - each element is written separately
            for element in arr {
                encode_generated_value(stream, field_number, element)?;
            }
        }
        GeneratedValue::Object(obj) => {
            // Encode object as JSON string
            let json = serde_json::to_string(obj)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
            stream
                .write_string(field_number, &json)
                .map_err(|e| KafkaPopulatorError::ProtoEncoding(e.to_string()))?;
        }
        GeneratedValue::Null => {
            // Skip null values (proto3 default behavior)
        }
    }
    Ok(())
}

/// Get the Kafka message key from an InternalRow.
///
/// Uses the id field value as the message key.
pub fn get_message_key(row: &InternalRow) -> Vec<u8> {
    match &row.id {
        GeneratedValue::Int64(i) => i.to_string().into_bytes(),
        GeneratedValue::Int32(i) => i.to_string().into_bytes(),
        GeneratedValue::String(s) => s.as_bytes().to_vec(),
        GeneratedValue::Uuid(u) => u.to_string().into_bytes(),
        _ => format!("{:?}", row.id).into_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use protobuf::CodedInputStream;
    use std::collections::HashMap;
    use sync_core::{FieldSchema, GeneratorConfig, IdField, SyncDataType, TableSchema};

    fn test_table_schema() -> TableSchema {
        TableSchema {
            name: "users".to_string(),
            id: IdField {
                id_type: SyncDataType::BigInt,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![
                FieldSchema {
                    name: "email".to_string(),
                    field_type: SyncDataType::VarChar { length: 255 },
                    generator: GeneratorConfig::Pattern {
                        pattern: "user_{index}@test.com".to_string(),
                    },
                    nullable: false,
                },
                FieldSchema {
                    name: "age".to_string(),
                    field_type: SyncDataType::Int,
                    generator: GeneratorConfig::IntRange { min: 18, max: 80 },
                    nullable: false,
                },
                FieldSchema {
                    name: "is_active".to_string(),
                    field_type: SyncDataType::Bool,
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
            GeneratedValue::String("test@example.com".to_string()),
        );
        fields.insert("age".to_string(), GeneratedValue::Int32(25));
        fields.insert("is_active".to_string(), GeneratedValue::Bool(true));

        let row = InternalRow::new("users", 0, GeneratedValue::Int64(1), fields);

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
        let schema = TableSchema {
            name: "events".to_string(),
            id: IdField {
                id_type: SyncDataType::BigInt,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![FieldSchema {
                name: "created_at".to_string(),
                field_type: SyncDataType::DateTime,
                generator: GeneratorConfig::TimestampRange {
                    start: "2024-01-01T00:00:00Z".to_string(),
                    end: "2024-12-31T23:59:59Z".to_string(),
                },
                nullable: false,
            }],
        };

        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 45).unwrap();
        let mut fields = HashMap::new();
        fields.insert("created_at".to_string(), GeneratedValue::DateTime(dt));

        let row = InternalRow::new("events", 0, GeneratedValue::Int64(1), fields);
        let encoded = encode_row(&row, &schema).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_encode_array() {
        let schema = TableSchema {
            name: "products".to_string(),
            id: IdField {
                id_type: SyncDataType::BigInt,
                generator: GeneratorConfig::Sequential { start: 1 },
            },
            fields: vec![FieldSchema {
                name: "tags".to_string(),
                field_type: SyncDataType::Array {
                    element_type: Box::new(SyncDataType::Text),
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
            GeneratedValue::Array(vec![
                GeneratedValue::String("tag1".to_string()),
                GeneratedValue::String("tag2".to_string()),
            ]),
        );

        let row = InternalRow::new("products", 0, GeneratedValue::Int64(1), fields);
        let encoded = encode_row(&row, &schema).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_get_message_key_int() {
        let row = InternalRow::new("test", 0, GeneratedValue::Int64(42), HashMap::new());
        let key = get_message_key(&row);
        assert_eq!(key, b"42");
    }

    #[test]
    fn test_get_message_key_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let row = InternalRow::new("test", 0, GeneratedValue::Uuid(uuid), HashMap::new());
        let key = get_message_key(&row);
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_get_message_key_string() {
        let row = InternalRow::new(
            "test",
            0,
            GeneratedValue::String("my-key".to_string()),
            HashMap::new(),
        );
        let key = get_message_key(&row);
        assert_eq!(key, b"my-key");
    }
}
