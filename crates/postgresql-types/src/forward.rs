//! Forward conversion: TypedValue → PostgreSQL value
//!
//! This module implements `From<TypedValue>` for `PostgreSQLValue`, converting
//! sync-core's type-safe values into PostgreSQL-compatible values for INSERT operations.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde_json::json;
use std::str::FromStr;
use sync_core::{GeneratedValue, GeometryType, SyncDataType, TypedValue};
use uuid::Uuid;

/// PostgreSQL value wrapper for type-safe conversions.
///
/// This enum wraps various PostgreSQL-compatible types that can be used
/// with tokio-postgres queries.
#[derive(Debug, Clone)]
pub enum PostgreSQLValue {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// 16-bit signed integer
    Int16(i16),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 32-bit floating point
    Float32(f32),
    /// 64-bit floating point
    Float64(f64),
    /// Decimal value
    Decimal(Decimal),
    /// Text/string value
    Text(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// UUID value
    Uuid(Uuid),
    /// Date value (no time)
    Date(NaiveDate),
    /// Time value (no date)
    Time(NaiveTime),
    /// Timestamp without timezone
    Timestamp(NaiveDateTime),
    /// Timestamp with timezone
    TimestampTz(DateTime<Utc>),
    /// JSON value
    Json(serde_json::Value),
    /// Array of text values (PostgreSQL text[])
    TextArray(Vec<String>),
    /// Array of i32 values (PostgreSQL integer[])
    Int32Array(Vec<i32>),
    /// Array of i64 values (PostgreSQL bigint[])
    Int64Array(Vec<i64>),
    /// Array of f64 values (PostgreSQL double precision[])
    Float64Array(Vec<f64>),
    /// Array of boolean values (PostgreSQL boolean[])
    BoolArray(Vec<bool>),
    /// Point geometry (x, y)
    Point(f64, f64),
}

impl From<TypedValue> for PostgreSQLValue {
    fn from(tv: TypedValue) -> Self {
        match (&tv.sync_type, tv.value) {
            // Null
            (_, GeneratedValue::Null) => PostgreSQLValue::Null,

            // Boolean
            (SyncDataType::Bool, GeneratedValue::Bool(b)) => PostgreSQLValue::Bool(b),

            // Integer types
            (SyncDataType::TinyInt { .. }, GeneratedValue::Int32(i)) => {
                PostgreSQLValue::Int16(i as i16)
            }
            (SyncDataType::SmallInt, GeneratedValue::Int32(i)) => PostgreSQLValue::Int16(i as i16),
            (SyncDataType::Int, GeneratedValue::Int32(i)) => PostgreSQLValue::Int32(i),
            (SyncDataType::Int, GeneratedValue::Int64(i)) => PostgreSQLValue::Int32(i as i32),
            (SyncDataType::BigInt, GeneratedValue::Int64(i)) => PostgreSQLValue::Int64(i),

            // Floating point
            (SyncDataType::Float, GeneratedValue::Float64(f)) => PostgreSQLValue::Float32(f as f32),
            (SyncDataType::Double, GeneratedValue::Float64(f)) => PostgreSQLValue::Float64(f),

            // Decimal
            (SyncDataType::Decimal { .. }, GeneratedValue::Decimal { value, .. }) => {
                match Decimal::from_str(&value) {
                    Ok(d) => PostgreSQLValue::Decimal(d),
                    Err(_) => PostgreSQLValue::Text(value),
                }
            }
            (SyncDataType::Decimal { .. }, GeneratedValue::String(s)) => {
                match Decimal::from_str(&s) {
                    Ok(d) => PostgreSQLValue::Decimal(d),
                    Err(_) => PostgreSQLValue::Text(s),
                }
            }

            // String types
            (SyncDataType::Char { .. }, GeneratedValue::String(s)) => PostgreSQLValue::Text(s),
            (SyncDataType::VarChar { .. }, GeneratedValue::String(s)) => PostgreSQLValue::Text(s),
            (SyncDataType::Text, GeneratedValue::String(s)) => PostgreSQLValue::Text(s),

            // Binary types
            (SyncDataType::Blob, GeneratedValue::Bytes(b)) => PostgreSQLValue::Bytes(b),
            (SyncDataType::Bytes, GeneratedValue::Bytes(b)) => PostgreSQLValue::Bytes(b),

            // UUID
            (SyncDataType::Uuid, GeneratedValue::Uuid(u)) => PostgreSQLValue::Uuid(u),
            (SyncDataType::Uuid, GeneratedValue::String(s)) => match Uuid::parse_str(&s) {
                Ok(u) => PostgreSQLValue::Uuid(u),
                Err(_) => PostgreSQLValue::Text(s),
            },

            // Date
            (SyncDataType::Date, GeneratedValue::DateTime(dt)) => {
                PostgreSQLValue::Date(dt.date_naive())
            }
            (SyncDataType::Date, GeneratedValue::String(s)) => {
                match NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                    Ok(d) => PostgreSQLValue::Date(d),
                    Err(_) => PostgreSQLValue::Text(s),
                }
            }

            // Time
            (SyncDataType::Time, GeneratedValue::DateTime(dt)) => PostgreSQLValue::Time(dt.time()),
            (SyncDataType::Time, GeneratedValue::String(s)) => {
                match NaiveTime::parse_from_str(&s, "%H:%M:%S") {
                    Ok(t) => PostgreSQLValue::Time(t),
                    Err(_) => PostgreSQLValue::Text(s),
                }
            }

            // DateTime (without timezone)
            (SyncDataType::DateTime, GeneratedValue::DateTime(dt)) => {
                PostgreSQLValue::Timestamp(dt.naive_utc())
            }
            (SyncDataType::DateTimeNano, GeneratedValue::DateTime(dt)) => {
                PostgreSQLValue::Timestamp(dt.naive_utc())
            }

            // Timestamp with timezone
            (SyncDataType::TimestampTz, GeneratedValue::DateTime(dt)) => {
                PostgreSQLValue::TimestampTz(dt)
            }

            // JSON
            (SyncDataType::Json, GeneratedValue::Object(obj)) => {
                let json = generated_object_to_json(obj);
                PostgreSQLValue::Json(json)
            }
            (SyncDataType::Json, GeneratedValue::String(s)) => match serde_json::from_str(&s) {
                Ok(v) => PostgreSQLValue::Json(v),
                Err(_) => PostgreSQLValue::Text(s),
            },
            (SyncDataType::Jsonb, GeneratedValue::Object(obj)) => {
                let json = generated_object_to_json(obj);
                PostgreSQLValue::Json(json)
            }
            (SyncDataType::Jsonb, GeneratedValue::String(s)) => match serde_json::from_str(&s) {
                Ok(v) => PostgreSQLValue::Json(v),
                Err(_) => PostgreSQLValue::Text(s),
            },

            // Arrays
            (SyncDataType::Array { element_type }, GeneratedValue::Array(arr)) => {
                convert_array_to_postgresql(element_type, arr)
            }

            // Set - PostgreSQL stores as text[]
            (SyncDataType::Set { .. }, GeneratedValue::Array(arr)) => {
                let values: Vec<String> = arr
                    .into_iter()
                    .filter_map(|v| {
                        if let GeneratedValue::String(s) = v {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .collect();
                PostgreSQLValue::TextArray(values)
            }

            // Enum - PostgreSQL stores as text
            (SyncDataType::Enum { .. }, GeneratedValue::String(s)) => PostgreSQLValue::Text(s),

            // Geometry - Point for now
            (SyncDataType::Geometry { geometry_type }, GeneratedValue::Bytes(b)) => {
                // For now, store as bytes; could parse WKB
                match geometry_type {
                    GeometryType::Point => {
                        // Try to interpret as point (x, y)
                        if b.len() >= 16 {
                            let x = f64::from_le_bytes(b[0..8].try_into().unwrap_or([0; 8]));
                            let y = f64::from_le_bytes(b[8..16].try_into().unwrap_or([0; 8]));
                            PostgreSQLValue::Point(x, y)
                        } else {
                            PostgreSQLValue::Bytes(b)
                        }
                    }
                    _ => PostgreSQLValue::Bytes(b),
                }
            }
            (SyncDataType::Geometry { .. }, GeneratedValue::String(s)) => PostgreSQLValue::Text(s),

            // Fallback conversions for type mismatches
            (_, GeneratedValue::Bool(b)) => PostgreSQLValue::Bool(b),
            (_, GeneratedValue::Int32(i)) => PostgreSQLValue::Int32(i),
            (_, GeneratedValue::Int64(i)) => PostgreSQLValue::Int64(i),
            (_, GeneratedValue::Float64(f)) => PostgreSQLValue::Float64(f),
            (_, GeneratedValue::String(s)) => PostgreSQLValue::Text(s),
            (_, GeneratedValue::Bytes(b)) => PostgreSQLValue::Bytes(b),
            (_, GeneratedValue::Uuid(u)) => PostgreSQLValue::Uuid(u),
            (_, GeneratedValue::DateTime(dt)) => PostgreSQLValue::TimestampTz(dt),
            (_, GeneratedValue::Decimal { value, .. }) => match Decimal::from_str(&value) {
                Ok(d) => PostgreSQLValue::Decimal(d),
                Err(_) => PostgreSQLValue::Text(value),
            },
            (_, GeneratedValue::Array(arr)) => {
                // Default to text array
                let values: Vec<String> = arr.into_iter().map(generated_to_string).collect();
                PostgreSQLValue::TextArray(values)
            }
            (_, GeneratedValue::Object(obj)) => {
                let json = generated_object_to_json(obj);
                PostgreSQLValue::Json(json)
            }
        }
    }
}

/// Convert a GeneratedValue array to the appropriate PostgreSQL array type.
fn convert_array_to_postgresql(
    element_type: &SyncDataType,
    arr: Vec<GeneratedValue>,
) -> PostgreSQLValue {
    match element_type {
        SyncDataType::Bool => {
            let values: Vec<bool> = arr
                .into_iter()
                .filter_map(|v| {
                    if let GeneratedValue::Bool(b) = v {
                        Some(b)
                    } else {
                        None
                    }
                })
                .collect();
            PostgreSQLValue::BoolArray(values)
        }
        SyncDataType::Int | SyncDataType::SmallInt | SyncDataType::TinyInt { .. } => {
            let values: Vec<i32> = arr
                .into_iter()
                .filter_map(|v| match v {
                    GeneratedValue::Int32(i) => Some(i),
                    GeneratedValue::Int64(i) => Some(i as i32),
                    _ => None,
                })
                .collect();
            PostgreSQLValue::Int32Array(values)
        }
        SyncDataType::BigInt => {
            let values: Vec<i64> = arr
                .into_iter()
                .filter_map(|v| match v {
                    GeneratedValue::Int32(i) => Some(i as i64),
                    GeneratedValue::Int64(i) => Some(i),
                    _ => None,
                })
                .collect();
            PostgreSQLValue::Int64Array(values)
        }
        SyncDataType::Float | SyncDataType::Double => {
            let values: Vec<f64> = arr
                .into_iter()
                .filter_map(|v| {
                    if let GeneratedValue::Float64(f) = v {
                        Some(f)
                    } else {
                        None
                    }
                })
                .collect();
            PostgreSQLValue::Float64Array(values)
        }
        _ => {
            // Default to text array for complex types
            let values: Vec<String> = arr.into_iter().map(generated_to_string).collect();
            PostgreSQLValue::TextArray(values)
        }
    }
}

/// Convert GeneratedValue to string representation.
fn generated_to_string(gv: GeneratedValue) -> String {
    match gv {
        GeneratedValue::Null => String::new(),
        GeneratedValue::Bool(b) => b.to_string(),
        GeneratedValue::Int32(i) => i.to_string(),
        GeneratedValue::Int64(i) => i.to_string(),
        GeneratedValue::Float64(f) => f.to_string(),
        GeneratedValue::String(s) => s,
        GeneratedValue::Bytes(b) => base64_encode(&b),
        GeneratedValue::Uuid(u) => u.to_string(),
        GeneratedValue::DateTime(dt) => dt.to_rfc3339(),
        GeneratedValue::Decimal { value, .. } => value,
        GeneratedValue::Array(arr) => {
            let json = generated_array_to_json(arr);
            json.to_string()
        }
        GeneratedValue::Object(obj) => {
            let json = generated_object_to_json(obj);
            json.to_string()
        }
    }
}

/// Hex encode bytes (simple encoding for binary data).
fn base64_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Convert a GeneratedValue array to serde_json::Value.
fn generated_array_to_json(arr: Vec<GeneratedValue>) -> serde_json::Value {
    let values: Vec<serde_json::Value> = arr.into_iter().map(generated_to_json).collect();
    serde_json::Value::Array(values)
}

/// Convert a GeneratedValue object to serde_json::Value.
fn generated_object_to_json(
    obj: std::collections::HashMap<String, GeneratedValue>,
) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = obj
        .into_iter()
        .map(|(k, v)| (k, generated_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

/// Convert any GeneratedValue to serde_json::Value.
fn generated_to_json(gv: GeneratedValue) -> serde_json::Value {
    match gv {
        GeneratedValue::Null => serde_json::Value::Null,
        GeneratedValue::Bool(b) => serde_json::Value::Bool(b),
        GeneratedValue::Int32(i) => json!(i),
        GeneratedValue::Int64(i) => json!(i),
        GeneratedValue::Float64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        GeneratedValue::String(s) => serde_json::Value::String(s),
        GeneratedValue::Bytes(b) => serde_json::Value::String(base64_encode(&b)),
        GeneratedValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        GeneratedValue::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        GeneratedValue::Decimal { value, .. } => serde_json::Value::String(value),
        GeneratedValue::Array(arr) => generated_array_to_json(arr),
        GeneratedValue::Object(obj) => generated_object_to_json(obj),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(pg_val, PostgreSQLValue::Bool(true)));

        let tv = TypedValue::bool(false);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(pg_val, PostgreSQLValue::Bool(false)));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int(42);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(pg_val, PostgreSQLValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::bigint(9_223_372_036_854_775_807i64);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(
            pg_val,
            PostgreSQLValue::Int64(9_223_372_036_854_775_807)
        ));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float(1.234);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Float32(f) = pg_val {
            assert!((f - 1.234).abs() < 0.01);
        } else {
            panic!("Expected Float32 value");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::double(1.23456789012345);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Float64(d) = pg_val {
            assert!((d - 1.23456789012345).abs() < 0.0001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_string_conversion() {
        let tv = TypedValue::text("hello world");
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Text(s) = pg_val {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(uuid);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Uuid(u) = pg_val {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid value");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        use chrono::TimeZone;
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 45).unwrap();
        let tv = TypedValue::datetime(dt);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Timestamp(ts) = pg_val {
            assert_eq!(ts.and_utc().year(), 2024);
        } else {
            panic!("Expected Timestamp value");
        }
    }

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(SyncDataType::Text);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(pg_val, PostgreSQLValue::Null));
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.456", 10, 3);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Decimal(d) = pg_val {
            assert_eq!(d.to_string(), "123.456");
        } else {
            panic!("Expected Decimal value");
        }
    }

    #[test]
    fn test_array_conversion() {
        let arr = vec![
            GeneratedValue::Int64(1),
            GeneratedValue::Int64(2),
            GeneratedValue::Int64(3),
        ];
        let tv = TypedValue::array(arr, SyncDataType::BigInt);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Int64Array(values) = pg_val {
            assert_eq!(values, vec![1, 2, 3]);
        } else {
            panic!("Expected Int64Array value");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "name".to_string(),
            GeneratedValue::String("Alice".to_string()),
        );
        obj.insert("age".to_string(), GeneratedValue::Int64(30));
        let tv = TypedValue::json_object(obj);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Json(json) = pg_val {
            assert_eq!(json["name"], "Alice");
            assert_eq!(json["age"], 30);
        } else {
            panic!("Expected Json value");
        }
    }
}
