//! Core data types for surreal-sync
//!
//! This module provides the fundamental data types used throughout surreal-sync
//! for representing data that can be bound to SurrealDB queries.

use base64::{engine::general_purpose::STANDARD, Engine};
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
    /// Attempt to convert the SurrealValue to a SurrealDB Id
    ///
    /// This is useful for cases where the value represents a record ID
    /// that can be used in SurrealDB operations.
    pub fn to_surrealql_id(&self) -> anyhow::Result<surrealdb::sql::Id> {
        match self {
            SurrealValue::Int(i) => Ok(surrealdb::sql::Id::from(*i)),
            SurrealValue::String(s) => Ok(surrealdb::sql::Id::from(s.clone())),
            SurrealValue::Bytes(data) => {
                let b64 = STANDARD.encode(data);
                Ok(surrealdb::sql::Id::from(b64))
            }
            _ => Err(anyhow::anyhow!("Cannot convert {self:?} to SurrealDB Id")),
        }
    }

    /// Convert SurrealValue to SurrealDB Value for query binding
    pub fn to_surrealql_value(&self) -> surrealdb::sql::Value {
        match self {
            SurrealValue::Bool(b) => surrealdb::sql::Value::Bool(*b),
            SurrealValue::Int(i) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*i)),
            SurrealValue::Float(f) => {
                surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*f))
            }
            SurrealValue::String(s) => {
                surrealdb::sql::Value::Strand(surrealdb::sql::Strand::from(s.clone()))
            }
            SurrealValue::DateTime(dt) => {
                surrealdb::sql::Value::Datetime(surrealdb::sql::Datetime::from(*dt))
            }
            SurrealValue::Array(vs) => {
                let mut arr = Vec::new();
                for item in vs {
                    let v = item.to_surrealql_value();
                    arr.push(v);
                }
                surrealdb::sql::Value::Array(surrealdb::sql::Array::from(arr))
            }
            SurrealValue::Object(obj) => {
                let mut map = std::collections::BTreeMap::new();
                for (key, value) in obj {
                    let v = value.to_surrealql_value();
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
}

/// Convert a single JSON value to SurrealValue
pub fn json_to_surreal_without_schema(value: serde_json::Value) -> anyhow::Result<SurrealValue> {
    log::debug!("🔧 json_to_surreal_without_schema called with: {value:?}");
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
            let mut vs = Vec::new();
            for item in arr {
                vs.push(json_to_surreal_without_schema(item)?);
            }
            Ok(SurrealValue::Array(vs))
        }
        serde_json::Value::Object(map) => {
            let mut kvs = std::collections::HashMap::new();
            for (key, val) in map {
                kvs.insert(key, json_to_surreal_without_schema(val)?);
            }
            Ok(SurrealValue::Object(kvs))
        }
    }
}
