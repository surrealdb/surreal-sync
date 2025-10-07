//! Core data types for surreal-sync
//!
//! This module provides the fundamental data types used throughout surreal-sync
//! for representing data that can be bound to SurrealDB queries.

use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Represents a value that can be bound to a SurrealDB query parameter
///
/// This enum provides a type-safe way to represent data from various source databases
/// in a format that can be efficiently serialized to SurrealDB using CBOR.
#[derive(Debug, Clone, PartialEq)]
pub enum SurrealValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Array(Vec<SurrealValue>), // For arrays, we'll use JSON since SurrealDB accepts Vec<T> where T: Into<Value>
    Object(HashMap<String, SurrealValue>), // For objects, use JSON
    Duration(std::time::Duration),
    Bytes(Vec<u8>),
    Decimal(surrealdb::sql::Number),
    Thing(surrealdb::sql::Thing),
    Geometry(surrealdb::sql::Geometry),
    Null,
    None,
}

impl SurrealValue {
    /// Attempt to convert the BindableValue to a SurrealDB Id
    ///
    /// This is useful for cases where the value represents a record ID
    /// that can be used in SurrealDB operations.
    pub fn to_id(&self) -> anyhow::Result<surrealdb::sql::Id> {
        match self {
            SurrealValue::Int(i) => Ok(surrealdb::sql::Id::from(*i)),
            SurrealValue::String(s) => Ok(surrealdb::sql::Id::from(s.clone())),
            _ => Err(anyhow::anyhow!("Cannot convert {:?} to SurrealDB Id", self)),
        }
    }
}

/// Convert a JSON value to BindableValue HashMap for CBOR serialization to SurrealDB
pub fn json_to_bindable_map(
    json: serde_json::Value,
) -> anyhow::Result<std::collections::HashMap<String, SurrealValue>> {
    log::debug!("🧩 json_to_bindable_map called with: {json:?}");
    match json {
        serde_json::Value::Object(map) => {
            let mut bindable_map = std::collections::HashMap::new();
            for (key, value) in map {
                log::debug!("🔧 Processing field '{key}' with value: {value:?}");
                let bindable_value = json_value_to_bindable(value)?;
                log::debug!("🔧 Field '{key}' converted to: {bindable_value:?}");
                bindable_map.insert(key, bindable_value);
            }
            Ok(bindable_map)
        }
        _ => Err(anyhow::anyhow!(
            "Expected JSON object for conversion to BindableValue map"
        )),
    }
}

/// Convert a single JSON value to BindableValue
pub fn json_value_to_bindable(value: serde_json::Value) -> anyhow::Result<SurrealValue> {
    log::debug!("🔧 json_value_to_bindable called with: {value:?}");
    match value {
        serde_json::Value::Null => Ok(SurrealValue::Null),
        serde_json::Value::Bool(b) => Ok(SurrealValue::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(SurrealValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(SurrealValue::Float(f))
            } else {
                Ok(SurrealValue::String(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(SurrealValue::String(s)),
        serde_json::Value::Array(arr) => {
            let mut bindable_arr = Vec::new();
            for item in arr {
                bindable_arr.push(json_value_to_bindable(item)?);
            }
            Ok(SurrealValue::Array(bindable_arr))
        }
        serde_json::Value::Object(map) => {
            let mut bindable_obj = std::collections::HashMap::new();
            for (key, val) in map {
                bindable_obj.insert(key, json_value_to_bindable(val)?);
            }
            Ok(SurrealValue::Object(bindable_obj))
        }
    }
}

/// Convert a HashMap of BindableValue to serde_json::Value
///
/// This function exists primarily for debugging and development purposes, such as
/// logging migration progress or inspecting data during development. Using this
/// function for actual data insertion into SurrealDB bypasses the intended design
/// of working directly with BindableValue, which maintains better type fidelity
/// and performance through SurrealDB's native CBOR serialization.
pub fn serialize_bindable_map_to_json(
    map: &HashMap<String, SurrealValue>,
) -> anyhow::Result<serde_json::Value> {
    let mut json_map = serde_json::Map::new();
    for (key, value) in map {
        json_map.insert(key.clone(), serialize_bindable_value_to_json(value)?);
    }
    Ok(serde_json::Value::Object(json_map))
}

/// Convert a BindableValue to serde_json::Value
pub fn serialize_bindable_value_to_json(value: &SurrealValue) -> anyhow::Result<serde_json::Value> {
    match value {
        SurrealValue::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        SurrealValue::Int(i) => Ok(serde_json::Value::Number(serde_json::Number::from(*i))),
        SurrealValue::Float(f) => Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(*f).ok_or_else(|| anyhow::anyhow!("Invalid float"))?,
        )),
        SurrealValue::String(s) => Ok(serde_json::Value::String(s.clone())),
        SurrealValue::DateTime(dt) => Ok(serde_json::Value::String(dt.to_rfc3339())),
        SurrealValue::Array(arr) => {
            let json_arr: Result<Vec<_>, _> =
                arr.iter().map(serialize_bindable_value_to_json).collect();
            Ok(serde_json::Value::Array(json_arr?))
        }
        SurrealValue::Object(obj) => serialize_bindable_map_to_json(obj),
        SurrealValue::Duration(dur) => {
            Ok(serde_json::Value::String(format!("{}ms", dur.as_millis())))
        }
        SurrealValue::Bytes(bytes) => {
            use base64::{engine::general_purpose, Engine as _};
            Ok(serde_json::Value::String(
                general_purpose::STANDARD.encode(bytes),
            ))
        }
        SurrealValue::Decimal(num) => Ok(serde_json::Value::String(num.to_string())),
        SurrealValue::Thing(thing) => Ok(serde_json::Value::String(thing.to_string())),
        SurrealValue::Geometry(geom) => Ok(serde_json::Value::String(geom.to_string())),
        SurrealValue::Null => Ok(serde_json::Value::Null),
        SurrealValue::None => Ok(serde_json::Value::Null),
    }
}

/// Convert BindableValue to SurrealDB Value for query binding
pub fn bindable_to_surrealdb_value(bindable: &SurrealValue) -> surrealdb::sql::Value {
    match bindable {
        SurrealValue::Bool(b) => surrealdb::sql::Value::Bool(*b),
        SurrealValue::Int(i) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*i)),
        SurrealValue::Float(f) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*f)),
        SurrealValue::String(s) => {
            surrealdb::sql::Value::Strand(surrealdb::sql::Strand::from(s.clone()))
        }
        SurrealValue::DateTime(dt) => {
            surrealdb::sql::Value::Datetime(surrealdb::sql::Datetime::from(*dt))
        }
        SurrealValue::Array(bindables) => {
            let mut arr = Vec::new();
            for item in bindables {
                let v = bindable_to_surrealdb_value(item);
                arr.push(v);
            }
            surrealdb::sql::Value::Array(surrealdb::sql::Array::from(arr))
        }
        SurrealValue::Object(obj) => {
            let mut map = std::collections::BTreeMap::new();
            for (key, value) in obj {
                let v = bindable_to_surrealdb_value(value);
                map.insert(key.clone(), v);
            }
            surrealdb::sql::Value::Object(surrealdb::sql::Object::from(map))
        }
        SurrealValue::Duration(d) => {
            surrealdb::sql::Value::Duration(surrealdb::sql::Duration::from(*d))
        }
        SurrealValue::Bytes(b) => {
            surrealdb::sql::Value::Bytes(surrealdb::sql::Bytes::from(b.clone()))
        }
        SurrealValue::Decimal(d) => surrealdb::sql::Value::Number(*d),
        SurrealValue::Thing(t) => surrealdb::sql::Value::Thing(t.clone()),
        SurrealValue::Geometry(g) => surrealdb::sql::Value::Geometry(g.clone()),
        SurrealValue::Null => surrealdb::sql::Value::Null,
        SurrealValue::None => surrealdb::sql::Value::None,
    }
}
