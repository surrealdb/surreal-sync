//! Reverse conversion: Protobuf → TypedValue.
//!
//! This module converts decoded protobuf messages (ProtoFieldValue) to TypedValue
//! for unified type handling across sources.

use crate::error::{KafkaTypesError, Result};
use crate::proto::{ProtoFieldValue, ProtoType};
use crate::{Message, Payload};
use json_types::JsonValueWithSchema;
use std::collections::HashMap;
use sync_core::{ColumnDefinition, TableDefinition, Type, TypedValue, Value};
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
    message: Message,
    table_schema: Option<&TableDefinition>,
) -> Result<HashMap<String, TypedValue>> {
    let mut kvs = HashMap::new();

    match message.payload {
        Payload::Protobuf(msg) => {
            // First, convert all fields from the message
            for (key, value) in msg.fields.clone() {
                let column_schema = table_schema.and_then(|ts| ts.get_column(&key));
                let typed_value = proto_to_typed_value_with_schema(value, column_schema)?;
                kvs.insert(key, typed_value);
            }

            // Apply Proto3 default semantics for missing scalar fields.
            //
            // ## Proto3 Wire Format Behavior
            //
            // Proto3 omits scalar fields with default values from the wire to save space.
            // For example, `bool active = 9;` with value `false` is not written.
            // The Proto3 spec states: "In proto3, scalar fields don't have presence."
            //
            // ## Fix Options Considered
            //
            // Option A: Change proto schema to use `optional` keyword
            //   - e.g., `optional bool active = 9;` forces presence tracking
            //   - Requires schema changes and regenerating protobuf code
            //   - Not chosen: would require coordinating schema updates across producers
            //
            // Option B: Apply Proto3 default semantics on the receiver side
            //   - Proto3 spec expects receivers to apply defaults for missing scalars
            //   - Minimal changes, follows protocol specification
            //   - Matches how protobuf libraries in other languages behave
            //
            // This implementation uses Option B - applying the documented Proto3 defaults
            // for missing scalar fields based on schema information.
            //
            // We use the proto message descriptor (always available) as the primary source
            // for field type information. The TableDefinition is used as a fallback or for
            // additional type information when available.

            // Apply defaults using proto descriptor (always available from the message)
            for field_name in msg.descriptor.field_order.iter() {
                if !kvs.contains_key(field_name) {
                    if let Some(field_desc) = msg.descriptor.fields.get(field_name) {
                        match &field_desc.field_type {
                            ProtoType::Bool => {
                                // Proto3 default: false
                                debug!(
                                    "Adding default 'false' for missing bool field '{}' (Proto3 default)",
                                    field_name
                                );
                                kvs.insert(field_name.clone(), TypedValue::bool(false));
                            }
                            ProtoType::Repeated(_) => {
                                // Proto3 default: empty repeated field
                                debug!(
                                    "Adding empty array for missing repeated field '{}' based on proto descriptor",
                                    field_name
                                );
                                // For repeated fields without TableDefinition, default to Text element type
                                kvs.insert(
                                    field_name.clone(),
                                    TypedValue::array(Vec::new(), Type::Text),
                                );
                            }
                            // Note: Other scalar types (int, string, etc.) default to 0/""
                            // but are typically not missing unless truly optional.
                            _ => {}
                        }
                    }
                }
            }

            // If TableDefinition is also available, use it for more precise type information
            if let Some(schema) = table_schema {
                for column in &schema.columns {
                    if !kvs.contains_key(&column.name) {
                        match &column.column_type {
                            Type::Array { element_type } => {
                                // Override with more precise element type from TableDefinition
                                debug!(
                                    "Adding empty array for missing field '{}' based on TableDefinition schema",
                                    column.name
                                );
                                kvs.insert(
                                    column.name.clone(),
                                    TypedValue::array(Vec::new(), (**element_type).clone()),
                                );
                            }
                            Type::Bool => {
                                // Proto3 default: false
                                debug!(
                                    "Adding default 'false' for missing bool field '{}' (Proto3 default via TableDefinition)",
                                    column.name
                                );
                                kvs.insert(column.name.clone(), TypedValue::bool(false));
                            }
                            _ => {}
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
    column_schema: Option<&ColumnDefinition>,
) -> Result<TypedValue> {
    match value {
        ProtoFieldValue::String(s) => {
            // Check if this is actually a JSON/Object field encoded as string
            if let Some(col) = column_schema {
                if matches!(col.column_type, Type::Json | Type::Jsonb) {
                    debug!(
                        "Parsing string field '{}' as JSON based on schema",
                        col.name
                    );
                    // Parse the JSON string and convert to TypedValue using json-types
                    let json_value: serde_json::Value =
                        serde_json::from_str(&s).map_err(|e| KafkaTypesError::JsonParse {
                            field: col.name.clone(),
                            message: e.to_string(),
                        })?;
                    return Ok(
                        JsonValueWithSchema::new(json_value, col.column_type.clone())
                            .to_typed_value(),
                    );
                }

                // Check if this is a Decimal field encoded as string (protobuf doesn't have native decimal)
                if let Type::Decimal { precision, scale } = &col.column_type {
                    debug!(
                        "Parsing string field '{}' as Decimal based on schema (precision={}, scale={})",
                        col.name, precision, scale
                    );
                    // Pass through to TypedValue::decimal - actual conversion happens in the sink layer
                    // which handles values that exceed rust_decimal's MAX by storing as Float
                    return Ok(TypedValue::decimal(s, *precision, *scale));
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
                                ProtoType::Bool => &ProtoFieldValue::Bool(false),
                                ProtoType::Repeated(_) => &ProtoFieldValue::Repeated(vec![]),
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
                .unwrap_or(Type::Text);

            // Extract just the Values for the array
            let arr: Vec<Value> = typed_values.into_iter().map(|tv| tv.value).collect();

            Ok(TypedValue::array(arr, element_type))
        }
        ProtoFieldValue::Null => Ok(TypedValue::null(Type::Text)),
    }
}

/// Convert a Value to serde_json::Value.
fn universal_value_to_json(value: &Value) -> serde_json::Value {
    use base64::Engine;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::json!(*b),
        Value::Int8 { value, .. } => serde_json::json!(*value),
        Value::Int16(i) => serde_json::json!(*i),
        Value::Int32(i) => serde_json::json!(*i),
        Value::Int64(i) => serde_json::json!(*i),
        Value::Float32(f) => serde_json::json!(*f),
        Value::Float64(f) => serde_json::json!(*f),
        Value::Char { value, .. } => serde_json::json!(value),
        Value::VarChar { value, .. } => serde_json::json!(value),
        Value::Text(s) => serde_json::json!(s),
        Value::Blob(b) | Value::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::json!(encoded)
        }
        Value::Uuid(u) => serde_json::json!(u.to_string()),
        Value::Ulid(u) => serde_json::json!(u.to_string()),
        Value::Date(dt) => serde_json::json!(dt.format("%Y-%m-%d").to_string()),
        Value::Time(dt) => serde_json::json!(dt.format("%H:%M:%S%.f").to_string()),
        Value::LocalDateTime(dt) | Value::LocalDateTimeNano(dt) | Value::ZonedDateTime(dt) => {
            serde_json::json!(dt.to_rfc3339())
        }
        Value::TimeTz(s) => serde_json::json!(s),
        Value::Decimal { value, .. } => serde_json::json!(value),
        Value::Array { elements, .. } => {
            serde_json::json!(elements
                .iter()
                .map(universal_value_to_json)
                .collect::<Vec<_>>())
        }
        Value::Set { elements, .. } => serde_json::json!(elements),
        Value::Enum { value, .. } => serde_json::json!(value),
        Value::Json(payload) | Value::Jsonb(payload) => (**payload).clone(),
        Value::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(json) = data;
            json.clone()
        }
        Value::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                serde_json::json!(format!("PT{secs}S"))
            } else {
                serde_json::json!(format!("PT{secs}.{nanos:09}S"))
            }
        }
        Value::Thing { table, id } => {
            let id_str = match id.as_ref() {
                Value::Text(s) => s.clone(),
                Value::Int32(i) => i.to_string(),
                Value::Int64(i) => i.to_string(),
                Value::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for Kafka: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            serde_json::json!(format!("{table}:{id_str}"))
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::ZeroTemporal {
            intended_type,
            source,
        } => {
            let s = source
                .as_deref()
                .unwrap_or_else(|| Value::canonical_zero_literal(intended_type));
            serde_json::Value::String(s.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ProtoMessage, ProtoMessageDescriptor};

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
        assert_eq!(result.sync_type, Type::Int32);
        assert!(matches!(result.value, Value::Int32(42)));
    }

    #[test]
    fn test_proto_to_typed_value_int64() {
        let value = ProtoFieldValue::Int64(123456789);
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, Type::Int64);
        assert!(matches!(result.value, Value::Int64(123456789)));
    }

    #[test]
    fn test_proto_to_typed_value_string() {
        let value = ProtoFieldValue::String("hello".to_string());
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, Type::Text);
        assert!(matches!(result.value, Value::Text(s) if s == "hello"));
    }

    #[test]
    fn test_proto_to_typed_value_bool() {
        let value = ProtoFieldValue::Bool(true);
        let result = proto_to_typed_value(value).unwrap();
        assert_eq!(result.sync_type, Type::Bool);
        assert!(matches!(result.value, Value::Bool(true)));
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
        assert_eq!(result.sync_type, Type::LocalDateTime);
        assert!(matches!(result.value, Value::LocalDateTime(_)));
    }

    #[test]
    fn test_proto_to_typed_value_repeated() {
        let value = ProtoFieldValue::Repeated(vec![
            ProtoFieldValue::String("a".to_string()),
            ProtoFieldValue::String("b".to_string()),
        ]);
        let result = proto_to_typed_value(value).unwrap();
        assert!(matches!(result.sync_type, Type::Array { .. }));
        assert!(matches!(result.value, Value::Array { ref elements, .. } if elements.len() == 2));
    }

    #[test]
    fn test_proto_to_typed_value_null() {
        let value = ProtoFieldValue::Null;
        let result = proto_to_typed_value(value).unwrap();
        assert!(matches!(result.value, Value::Null));
    }

    #[test]
    fn test_proto_to_typed_value_with_schema_json() {
        let column_schema = sync_core::ColumnDefinition::nullable("metadata", Type::Json);

        let value = ProtoFieldValue::String(r#"{"key": "value"}"#.to_string());
        let result = proto_to_typed_value_with_schema(value, Some(&column_schema)).unwrap();
        assert_eq!(result.sync_type, Type::Json);
        assert!(matches!(result.value, Value::Json(_)));
    }
}
