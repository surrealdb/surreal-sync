use crate::surreal::{json_to_surreal_without_schema, SurrealValue};
use std::collections::HashMap;
use surreal_sync_kafka::ProtoFieldValue;
use sync_core::{SyncDataType, TableSchema};
use tracing::debug;

/// Convert a Kafka message to SurrealDB key-value pairs.
///
/// If `table_schema` is provided, schema-aware conversion is used:
/// - JSON/Object fields encoded as strings are parsed and converted to native Objects
/// - Missing array fields are filled with empty arrays
pub fn message_to_keys_and_surreal_values(
    message: surreal_sync_kafka::Message,
    table_schema: Option<&TableSchema>,
) -> anyhow::Result<HashMap<String, SurrealValue>> {
    let mut kvs = HashMap::new();

    match message.payload {
        surreal_sync_kafka::Payload::Protobuf(msg) => {
            // First, convert all fields from the message
            for (key, value) in msg.fields {
                let field_schema = table_schema.and_then(|ts| ts.get_field(&key));
                let surreal_value = proto_to_surreal_with_schema(value, field_schema)?;
                kvs.insert(key, surreal_value);
            }

            // If we have schema, add missing array fields as empty arrays
            if let Some(schema) = table_schema {
                for field in &schema.fields {
                    if !kvs.contains_key(&field.name) {
                        // Check if this is an array field
                        if matches!(field.field_type, SyncDataType::Array { .. }) {
                            debug!(
                                "Adding empty array for missing field '{}' based on schema",
                                field.name
                            );
                            kvs.insert(field.name.clone(), SurrealValue::Array(Vec::new()));
                        }
                    }
                }
            }
        }
    }

    Ok(kvs)
}

/// Convert protobuf field value to SurrealValue with optional schema information.
fn proto_to_surreal_with_schema(
    value: ProtoFieldValue,
    field_schema: Option<&sync_core::FieldSchema>,
) -> anyhow::Result<SurrealValue> {
    match value {
        ProtoFieldValue::String(s) => {
            // Check if this is actually a JSON/Object field encoded as string
            if let Some(fs) = field_schema {
                if matches!(fs.field_type, SyncDataType::Json | SyncDataType::Jsonb) {
                    debug!("Parsing string field '{}' as JSON based on schema", fs.name);
                    // Parse the JSON string and convert to SurrealValue::Object
                    let json_value: serde_json::Value = serde_json::from_str(&s).map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to parse JSON string for field '{}': {}",
                            fs.name,
                            e
                        )
                    })?;
                    return json_to_surreal_without_schema(json_value);
                }
            }
            Ok(SurrealValue::String(s))
        }
        // For all other types, delegate to the existing conversion
        other => proto_to_surreal(other),
    }
}

fn proto_to_surreal(value: surreal_sync_kafka::ProtoFieldValue) -> anyhow::Result<SurrealValue> {
    match value {
        surreal_sync_kafka::ProtoFieldValue::Int32(i) => Ok(SurrealValue::Int(i as i64)),
        surreal_sync_kafka::ProtoFieldValue::Int64(i) => Ok(SurrealValue::Int(i)),
        surreal_sync_kafka::ProtoFieldValue::Uint32(u) => Ok(SurrealValue::Int(u as i64)),
        surreal_sync_kafka::ProtoFieldValue::Uint64(u) => Ok(SurrealValue::Int(u as i64)),
        surreal_sync_kafka::ProtoFieldValue::Float(f) => Ok(SurrealValue::Float(f as f64)),
        surreal_sync_kafka::ProtoFieldValue::Double(d) => Ok(SurrealValue::Float(d)),
        surreal_sync_kafka::ProtoFieldValue::Bool(b) => Ok(SurrealValue::Bool(b)),
        surreal_sync_kafka::ProtoFieldValue::String(s) => Ok(SurrealValue::String(s)),
        surreal_sync_kafka::ProtoFieldValue::Bytes(b) => Ok(SurrealValue::Bytes(b)),
        surreal_sync_kafka::ProtoFieldValue::Message(m) => {
            match m.message_type.as_str() {
                "" => anyhow::bail!("Nested message has no type"),
                "google.protobuf.Timestamp" => {
                    // Special handling for google.protobuf.Timestamp
                    if let Some(surreal_sync_kafka::ProtoFieldValue::Int64(seconds)) =
                        m.fields.get("seconds")
                    {
                        let nanos = if let Some(surreal_sync_kafka::ProtoFieldValue::Int32(n)) =
                            m.fields.get("nanos")
                        {
                            *n as u32
                        } else {
                            0
                        };
                        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(*seconds, nanos)
                            .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Invalid timestamp with seconds: {seconds} and nanos: {nanos}"
                            )
                        })?;
                        return Ok(SurrealValue::DateTime(dt));
                    } else {
                        anyhow::bail!("Timestamp message missing 'seconds' field");
                    }
                }
                t => {
                    debug!("Converting nested message of type {t} to generic SurrealDB object");
                }
            }
            let mut map = HashMap::new();
            for k in m.descriptor.field_order.iter() {
                let f = m
                    .descriptor
                    .fields
                    .get(k)
                    .ok_or_else(|| anyhow::anyhow!("Field descriptor for '{k}' not found"))?;
                let v = match m.fields.get(k) {
                    Some(v) => v,
                    None => match f.field_type {
                        surreal_sync_kafka::ProtoType::Bool => &ProtoFieldValue::Bool(false),
                        surreal_sync_kafka::ProtoType::Repeated(_) => {
                            &ProtoFieldValue::Repeated(vec![])
                        }
                        _ => {
                            anyhow::bail!("Field '{k}' listed in descriptor but missing in fields");
                        }
                    },
                };
                debug!("Converting nested field {k}={v:?} to SurrealDB value");
                map.insert(k.to_owned(), proto_to_surreal(v.to_owned())?);
            }
            Ok(SurrealValue::Object(map))
        }
        surreal_sync_kafka::ProtoFieldValue::Repeated(reps) => {
            let mut arr = Vec::new();
            for v in reps {
                arr.push(proto_to_surreal(v)?);
            }
            Ok(SurrealValue::Array(arr))
        }
        surreal_sync_kafka::ProtoFieldValue::Null => Ok(SurrealValue::Null),
    }
}
