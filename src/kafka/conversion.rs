use crate::surreal::SurrealValue;
use std::collections::HashMap;
use surreal_sync_kafka::ProtoFieldValue;
use tracing::debug;

pub fn message_to_keys_and_surreal_values(
    message: surreal_sync_kafka::Message,
) -> anyhow::Result<HashMap<String, SurrealValue>> {
    let mut kvs = HashMap::new();

    match message.payload {
        surreal_sync_kafka::Payload::Protobuf(msg) => {
            for (key, value) in msg.fields {
                let surreal_value = proto_to_surreal(value)?;
                kvs.insert(key, surreal_value);
            }
        }
    }

    Ok(kvs)
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
