use crate::surreal::SurrealValue;
use std::collections::HashMap;

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
            let mut map = HashMap::new();
            for (k, v) in m.fields {
                map.insert(k, proto_to_surreal(v)?);
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
