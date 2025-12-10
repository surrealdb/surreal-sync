//! Reverse conversion: Protobuf → TypedValue.
//!
//! This module converts decoded protobuf messages (ProtoFieldValue) to TypedValue
//! for unified type handling across sources.

use crate::error::{KafkaTypesError, Result};
use json_types::JsonValueWithSchema;
use std::collections::HashMap;
use surreal_sync_kafka::ProtoFieldValue;
use sync_core::{TableDefinition, TypedValue, UniversalType, UniversalValue};
use tracing::debug;

/// Convert a Kafka message to TypedValue key-value pairs.
///
/// This is the unified conversion path that uses TypedValue as intermediate representation,
/// matching the CSV and JSONL source patterns.
///
/// If `table_schema` is provided, schema-aware conversion is used:
/// - JSON/Object fields encoded as strings are parsed and converted to Objects
/// - Missing array fields are filled with empty arrays
/// - Type information is preserved from the schema
pub fn message_to_typed_values(
    message: surreal_sync_kafka::Message,
    table_schema: Option<&TableDefinition>,
) -> Result<HashMap<String, TypedValue>> {
    let mut kvs = HashMap::new();

    match message.payload {
        surreal_sync_kafka::Payload::Protobuf(msg) => {
            // First, convert all fields from the message
            for (key, value) in msg.fields {
                let field_schema = table_schema.and_then(|ts| ts.get_field(&key));
                let typed_value = proto_to_typed_value_with_schema(value, field_schema)?;
                kvs.insert(key, typed_value);
            }

            // If we have schema, add missing array fields as empty arrays
            if let Some(schema) = table_schema {
                for field in &schema.fields {
                    if !kvs.contains_key(&field.name) {
                        // Check if this is an array field
                        if let UniversalType::Array { element_type } = &field.field_type {
                            debug!(
                                "Adding empty array for missing field '{}' based on schema",
                                field.name
                            );
                            kvs.insert(
                                field.name.clone(),
                                TypedValue::array(Vec::new(), (**element_type).clone()),
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(kvs)
}

/// Convert protobuf field value to TypedValue with optional schema information.
pub fn proto_to_typed_value_with_schema(
    value: ProtoFieldValue,
    field_schema: Option<&sync_core::FieldDefinition>,
) -> Result<TypedValue> {
    match value {
        ProtoFieldValue::String(s) => {
            // Check if this is actually a JSON/Object field encoded as string
            if let Some(fs) = field_schema {
                if matches!(fs.field_type, UniversalType::Json | UniversalType::Jsonb) {
                    debug!("Parsing string field '{}' as JSON based on schema", fs.name);
                    // Parse the JSON string and convert to TypedValue using json-types
                    let json_value: serde_json::Value =
                        serde_json::from_str(&s).map_err(|e| KafkaTypesError::JsonParse {
                            field: fs.name.clone(),
                            message: e.to_string(),
                        })?;
                    return Ok(JsonValueWithSchema::new(json_value, fs.field_type.clone())
                        .to_typed_value());
                }
            }
            Ok(TypedValue::text(&s))
        }
        // For all other types, delegate to the base conversion
        other => proto_to_typed_value(other),
    }
}

/// Convert protobuf field value to TypedValue (without schema context).
pub fn proto_to_typed_value(value: ProtoFieldValue) -> Result<TypedValue> {
    match value {
        ProtoFieldValue::Int32(i) => Ok(TypedValue::int32(i)),
        ProtoFieldValue::Int64(i) => Ok(TypedValue::int64(i)),
        ProtoFieldValue::Uint32(u) => Ok(TypedValue::int64(u as i64)),
        ProtoFieldValue::Uint64(u) => Ok(TypedValue::int64(u as i64)),
        ProtoFieldValue::Float(f) => Ok(TypedValue::float32(f)),
        ProtoFieldValue::Double(d) => Ok(TypedValue::float64(d)),
        ProtoFieldValue::Bool(b) => Ok(TypedValue::bool(b)),
        ProtoFieldValue::String(s) => Ok(TypedValue::text(&s)),
        ProtoFieldValue::Bytes(b) => Ok(TypedValue::bytes(b)),
        ProtoFieldValue::Message(m) => {
            match m.message_type.as_str() {
                "" => Err(KafkaTypesError::TypeConversion(
                    "Nested message has no type".to_string(),
                )),
                "google.protobuf.Timestamp" => {
                    // Special handling for google.protobuf.Timestamp
                    if let Some(ProtoFieldValue::Int64(seconds)) = m.fields.get("seconds") {
                        let nanos = if let Some(ProtoFieldValue::Int32(n)) = m.fields.get("nanos") {
                            *n as u32
                        } else {
                            0
                        };
                        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(*seconds, nanos)
                            .ok_or(KafkaTypesError::InvalidTimestamp {
                            seconds: *seconds,
                            nanos,
                        })?;
                        Ok(TypedValue::datetime(dt))
                    } else {
                        Err(KafkaTypesError::MissingField("seconds".to_string()))
                    }
                }
                t => {
                    debug!("Converting nested message of type {t} to generic object");
                    // Convert nested message to Object (JSON-like structure)
                    let mut map = serde_json::Map::new();
                    for k in m.descriptor.field_order.iter() {
                        let f = m.descriptor.fields.get(k).ok_or_else(|| {
                            KafkaTypesError::MissingField(format!(
                                "Field descriptor for '{k}' not found"
                            ))
                        })?;
                        let v = match m.fields.get(k) {
                            Some(v) => v,
                            None => match f.field_type {
                                surreal_sync_kafka::ProtoType::Bool => {
                                    &ProtoFieldValue::Bool(false)
                                }
                                surreal_sync_kafka::ProtoType::Repeated(_) => {
                                    &ProtoFieldValue::Repeated(vec![])
                                }
                                _ => {
                                    return Err(KafkaTypesError::MissingField(format!(
                                        "Field '{k}' listed in descriptor but missing in fields"
                                    )));
                                }
                            },
                        };
                        debug!("Converting nested field {k}={v:?} to TypedValue");
                        let typed_value = proto_to_typed_value(v.to_owned())?;
                        let json_value = universal_value_to_json(&typed_value.value);
                        map.insert(k.to_owned(), json_value);
                    }
                    Ok(TypedValue::json(serde_json::Value::Object(map)))
                }
            }
        }
        ProtoFieldValue::Repeated(reps) => {
            // Convert all elements and collect both values and TypedValues
            let mut typed_values: Vec<TypedValue> = Vec::new();
            for v in reps {
                typed_values.push(proto_to_typed_value(v)?);
            }

            // Infer element type from the first element, default to Text if empty
            let element_type = typed_values
                .first()
                .map(|tv| tv.sync_type.clone())
                .unwrap_or(UniversalType::Text);

            // Extract just the UniversalValues for the array
            let arr: Vec<UniversalValue> = typed_values.into_iter().map(|tv| tv.value).collect();

            Ok(TypedValue::array(arr, element_type))
        }
        ProtoFieldValue::Null => Ok(TypedValue::null(UniversalType::Text)),
    }
}

/// Convert a UniversalValue to serde_json::Value.
fn universal_value_to_json(value: &UniversalValue) -> serde_json::Value {
    use base64::Engine;
    match value {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => serde_json::json!(*b),
        UniversalValue::Int8 { value, .. } => serde_json::json!(*value),
        UniversalValue::Int16(i) => serde_json::json!(*i),
        UniversalValue::Int32(i) => serde_json::json!(*i),
        UniversalValue::Int64(i) => serde_json::json!(*i),
        UniversalValue::Float32(f) => serde_json::json!(*f),
        UniversalValue::Float64(f) => serde_json::json!(*f),
        UniversalValue::Char { value, .. } => serde_json::json!(value),
        UniversalValue::VarChar { value, .. } => serde_json::json!(value),
        UniversalValue::Text(s) => serde_json::json!(s),
        UniversalValue::Blob(b) | UniversalValue::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::json!(encoded)
        }
        UniversalValue::Uuid(u) => serde_json::json!(u.to_string()),
        UniversalValue::Date(dt) => serde_json::json!(dt.format("%Y-%m-%d").to_string()),
        UniversalValue::Time(dt) => serde_json::json!(dt.format("%H:%M:%S").to_string()),
        UniversalValue::LocalDateTime(dt)
        | UniversalValue::LocalDateTimeNano(dt)
        | UniversalValue::ZonedDateTime(dt) => serde_json::json!(dt.to_rfc3339()),
        UniversalValue::Decimal { value, .. } => serde_json::json!(value),
        UniversalValue::Array { elements, .. } => {
            serde_json::json!(elements
                .iter()
                .map(universal_value_to_json)
                .collect::<Vec<_>>())
        }
        UniversalValue::Set { elements, .. } => serde_json::json!(elements),
        UniversalValue::Enum { value, .. } => serde_json::json!(value),
        UniversalValue::Json(payload) | UniversalValue::Jsonb(payload) => (**payload).clone(),
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(json) = data;
            json.clone()
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surreal_sync_kafka::{ProtoMessage, ProtoMessageDescriptor};

    fn empty_descriptor() -> ProtoMessageDescriptor {
        ProtoMessageDescriptor {
            name: String::new(),
            fields: HashMap::new(),
            field_order: Vec::new(),
        }
    }

    #[test]
    fn test_proto_to_typed_value_int32() {
        let value = ProtoFieldValue::Int32(42);
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, UniversalType::Int32);
        assert!(matches!(result.value, UniversalValue::Int32(42)));
    }

    #[test]
    fn test_proto_to_typed_value_int64() {
        let value = ProtoFieldValue::Int64(123456789);
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, UniversalType::Int64);
        assert!(matches!(result.value, UniversalValue::Int64(123456789)));
    }

    #[test]
    fn test_proto_to_typed_value_string() {
        let value = ProtoFieldValue::String("hello".to_string());
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, UniversalType::Text);
        assert!(matches!(result.value, UniversalValue::Text(s) if s == "hello"));
    }

    #[test]
    fn test_proto_to_typed_value_bool() {
        let value = ProtoFieldValue::Bool(true);
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, UniversalType::Bool);
        assert!(matches!(result.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_proto_to_typed_value_timestamp() {
        let mut fields = HashMap::new();
        fields.insert("seconds".to_string(), ProtoFieldValue::Int64(1718451045));
        fields.insert("nanos".to_string(), ProtoFieldValue::Int32(500_000_000));

        let msg = ProtoMessage {
            message_type: "google.protobuf.Timestamp".to_string(),
            fields,
            descriptor: empty_descriptor(),
        };

        let value = ProtoFieldValue::Message(Box::new(msg));
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, UniversalType::LocalDateTime);
        assert!(matches!(result.value, UniversalValue::LocalDateTime(_)));
    }

    #[test]
    fn test_proto_to_typed_value_repeated() {
        let value = ProtoFieldValue::Repeated(vec![
            ProtoFieldValue::String("a".to_string()),
            ProtoFieldValue::String("b".to_string()),
        ]);
        let result = proto_to_typed_value(value).unwrap();
        assert!(matches!(result.sync_type, UniversalType::Array { .. }));
        assert!(
            matches!(result.value, UniversalValue::Array { ref elements, .. } if elements.len() == 2)
        );
    }

    #[test]
    fn test_proto_to_typed_value_null() {
        let value = ProtoFieldValue::Null;
        let result = proto_to_typed_value(value).unwrap();
        assert!(matches!(result.value, UniversalValue::Null));
    }

    #[test]
    fn test_proto_to_typed_value_with_schema_json() {
        let field_schema = sync_core::FieldDefinition {
            name: "metadata".to_string(),
            field_type: UniversalType::Json,
            generator: sync_core::GeneratorConfig::Pattern {
                pattern: "{}".to_string(),
            },
            nullable: true,
        };

        let value = ProtoFieldValue::String(r#"{"key": "value"}"#.to_string());
        let result = proto_to_typed_value_with_schema(value, Some(&field_schema)).unwrap();
        assert_eq!(result.sync_type, UniversalType::Json);
        assert!(matches!(result.value, UniversalValue::Json(_)));
    }
}
