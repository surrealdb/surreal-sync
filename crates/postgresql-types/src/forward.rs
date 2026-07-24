//! Forward conversion: Value/TypedValue → PostgreSQL value
//!
//! This module implements `From<Value>` and `From<TypedValue>` for `PostgreSQLValue`,
//! converting sync-core's values into PostgreSQL-compatible values for INSERT operations.
//! The TypedValue implementation delegates to Value for most cases, but keeps
//! special handling for typed arrays and JSON text parsing where type info is needed.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde_json::json;
use std::str::FromStr;
use sync_core::{Type, TypedValue, Value};
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

impl From<Value> for PostgreSQLValue {
    fn from(value: Value) -> Self {
        match value {
            // Null
            Value::Null => PostgreSQLValue::Null,

            // Boolean
            Value::Bool(b) => PostgreSQLValue::Bool(b),

            // Integer types
            Value::Int8 { value, .. } => PostgreSQLValue::Int16(value as i16),
            Value::Int16(i) => PostgreSQLValue::Int16(i),
            Value::Int32(i) => PostgreSQLValue::Int32(i),
            Value::Int64(i) => PostgreSQLValue::Int64(i),

            // Floating point
            Value::Float32(f) => PostgreSQLValue::Float32(f),
            Value::Float64(f) => PostgreSQLValue::Float64(f),

            // Decimal - try to parse as Decimal, fallback to Text
            Value::Decimal { value, .. } => match Decimal::from_str(&value) {
                Ok(d) => PostgreSQLValue::Decimal(d),
                Err(_) => PostgreSQLValue::Text(value),
            },

            // String types
            Value::Char { value, .. } => PostgreSQLValue::Text(value),
            Value::VarChar { value, .. } => PostgreSQLValue::Text(value),
            Value::Text(s) => PostgreSQLValue::Text(s),

            // Binary types
            Value::Blob(b) => PostgreSQLValue::Bytes(b),
            Value::Bytes(b) => PostgreSQLValue::Bytes(b),

            // UUID
            Value::Uuid(u) => PostgreSQLValue::Uuid(u),

            // ULID - convert to text
            Value::Ulid(u) => PostgreSQLValue::Text(u.to_string()),

            // Date
            Value::Date(dt) => PostgreSQLValue::Date(dt.date_naive()),

            // Time
            Value::Time(dt) => PostgreSQLValue::Time(dt.time()),

            // DateTime (without timezone)
            Value::LocalDateTime(dt) => PostgreSQLValue::Timestamp(dt.naive_utc()),
            Value::LocalDateTimeNano(dt) => PostgreSQLValue::Timestamp(dt.naive_utc()),

            // Timestamp with timezone
            Value::ZonedDateTime(dt) => PostgreSQLValue::TimestampTz(dt),

            // TimeTz - store as text to preserve timezone format
            // Note: We intentionally do NOT use a datetime type because time and datetime
            // are fundamentally different types.
            Value::TimeTz(s) => PostgreSQLValue::Text(s),

            // JSON
            Value::Json(json_val) => PostgreSQLValue::Json((*json_val).clone()),
            Value::Jsonb(json_val) => PostgreSQLValue::Json((*json_val).clone()),

            // Array - default to text array without element_type info
            Value::Array { elements, .. } => {
                let values: Vec<String> = elements.into_iter().map(generated_to_string).collect();
                PostgreSQLValue::TextArray(values)
            }

            // Set - PostgreSQL stores as text[]
            Value::Set { elements, .. } => PostgreSQLValue::TextArray(elements),

            // Enum - PostgreSQL stores as text
            Value::Enum { value, .. } => PostgreSQLValue::Text(value),

            // Geometry - store as GeoJSON
            Value::Geometry { data, .. } => {
                use sync_core::values::GeometryData;
                let GeometryData(json_val) = data;
                PostgreSQLValue::Json(json_val)
            }

            // Duration - store as text in ISO 8601 format
            Value::Duration(d) => {
                let secs = d.as_secs();
                let nanos = d.subsec_nanos();
                if nanos == 0 {
                    PostgreSQLValue::Text(format!("PT{secs}S"))
                } else {
                    PostgreSQLValue::Text(format!("PT{secs}.{nanos:09}S"))
                }
            }

            // Thing - record reference as "table:id" format
            Value::Thing { table, id } => {
                let id_str = match id.as_ref() {
                    Value::Text(s) => s.clone(),
                    Value::Int32(i) => i.to_string(),
                    Value::Int64(i) => i.to_string(),
                    Value::Uuid(u) => u.to_string(),
                    other => panic!(
                        "Unsupported Thing ID type: {other:?}. \
                         Supported types: Text, Int32, Int64, Uuid"
                    ),
                };
                PostgreSQLValue::Text(format!("{table}:{id_str}"))
            }

            // Object - nested document as JSONB
            Value::Object(map) => {
                let obj: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, universal_value_to_json(v)))
                    .collect();
                PostgreSQLValue::Json(serde_json::Value::Object(obj))
            }

            Value::ZeroTemporal {
                intended_type,
                source,
            } => {
                let s = source
                    .unwrap_or_else(|| Value::canonical_zero_literal(&intended_type).to_string());
                PostgreSQLValue::Text(s)
            }
        }
    }
}

impl From<TypedValue> for PostgreSQLValue {
    fn from(tv: TypedValue) -> Self {
        match (&tv.sync_type, &tv.value) {
            // Special case: UUID type with text value - parse the UUID string
            (Type::Uuid, Value::Text(s)) => match Uuid::parse_str(s) {
                Ok(u) => PostgreSQLValue::Uuid(u),
                Err(_) => PostgreSQLValue::Text(s.clone()),
            },

            // Special case: Decimal type with text value - parse as decimal
            (Type::Decimal { .. }, Value::Text(s)) => match Decimal::from_str(s) {
                Ok(d) => PostgreSQLValue::Decimal(d),
                Err(_) => PostgreSQLValue::Text(s.clone()),
            },

            // Special case: JSON/Jsonb type with text value - parse JSON string
            (Type::Json, Value::Text(s)) => match serde_json::from_str(s) {
                Ok(v) => PostgreSQLValue::Json(v),
                Err(_) => PostgreSQLValue::Text(s.clone()),
            },
            (Type::Jsonb, Value::Text(s)) => match serde_json::from_str(s) {
                Ok(v) => PostgreSQLValue::Json(v),
                Err(_) => PostgreSQLValue::Text(s.clone()),
            },

            // Special case: Typed arrays - use element_type for proper array conversion
            (Type::Array { element_type }, Value::Array { elements, .. }) => {
                convert_array_to_postgresql(element_type, elements)
            }

            // All other cases delegate to From<Value>
            _ => PostgreSQLValue::from(tv.value),
        }
    }
}

/// Convert a Value array to the appropriate PostgreSQL array type.
fn convert_array_to_postgresql(element_type: &Type, arr: &[Value]) -> PostgreSQLValue {
    let arr = arr.to_vec();
    match element_type {
        Type::Bool => {
            let values: Vec<bool> = arr
                .into_iter()
                .filter_map(|v| {
                    if let Value::Bool(b) = v {
                        Some(b)
                    } else {
                        None
                    }
                })
                .collect();
            PostgreSQLValue::BoolArray(values)
        }
        Type::Int32 | Type::Int16 | Type::Int8 { .. } => {
            let values: Vec<i32> = arr
                .into_iter()
                .filter_map(|v| match v {
                    Value::Int32(i) => Some(i),
                    Value::Int64(i) => Some(i as i32),
                    _ => None,
                })
                .collect();
            PostgreSQLValue::Int32Array(values)
        }
        Type::Int64 => {
            let values: Vec<i64> = arr
                .into_iter()
                .filter_map(|v| match v {
                    Value::Int32(i) => Some(i as i64),
                    Value::Int64(i) => Some(i),
                    _ => None,
                })
                .collect();
            PostgreSQLValue::Int64Array(values)
        }
        Type::Float32 | Type::Float64 => {
            let values: Vec<f64> = arr
                .into_iter()
                .filter_map(|v| {
                    if let Value::Float64(f) = v {
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

/// Convert Value to string representation.
fn generated_to_string(gv: Value) -> String {
    match gv {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(s) => s,
        Value::Bytes(b) => base64_encode(&b),
        Value::Uuid(u) => u.to_string(),
        Value::LocalDateTime(dt) => dt.to_rfc3339(),
        Value::Decimal { value, .. } => value,
        Value::Array { elements, .. } => {
            let json = generated_array_to_json(elements);
            json.to_string()
        }
        Value::Json(json_val) => (*json_val).to_string(),
        _ => format!("{gv:?}"),
    }
}

/// Hex encode bytes (simple encoding for binary data).
fn base64_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Convert a Value array to serde_json::Value.
fn generated_array_to_json(arr: Vec<Value>) -> serde_json::Value {
    let values: Vec<serde_json::Value> = arr.into_iter().map(generated_to_json).collect();
    serde_json::Value::Array(values)
}

/// Convert any Value to serde_json::Value.
fn generated_to_json(gv: Value) -> serde_json::Value {
    match gv {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int32(i) => json!(i),
        Value::Int64(i) => json!(i),
        Value::Float64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => serde_json::Value::String(s),
        Value::Bytes(b) => serde_json::Value::String(base64_encode(&b)),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::LocalDateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Decimal { value, .. } => serde_json::Value::String(value),
        Value::Array { elements, .. } => generated_array_to_json(elements),
        Value::Json(json_val) => *json_val,
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, generated_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::String(format!("{gv:?}")),
    }
}

/// Convert Value by reference to serde_json::Value for Object conversion.
fn universal_value_to_json(value: Value) -> serde_json::Value {
    generated_to_json(value)
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
        let tv = TypedValue::int32(42);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(pg_val, PostgreSQLValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9_223_372_036_854_775_807i64);
        let pg_val: PostgreSQLValue = tv.into();
        assert!(matches!(
            pg_val,
            PostgreSQLValue::Int64(9_223_372_036_854_775_807)
        ));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.234);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Float32(f) = pg_val {
            assert!((f - 1.234).abs() < 0.01);
        } else {
            panic!("Expected Float32 value");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::float64(1.23456789012345);
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
        let tv = TypedValue::null(Type::Text);
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
        let arr = vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)];
        let tv = TypedValue::array(arr, Type::Int64);
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Int64Array(values) = pg_val {
            assert_eq!(values, vec![1, 2, 3]);
        } else {
            panic!("Expected Int64Array value");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "name".to_string(),
            serde_json::Value::String("Alice".to_string()),
        );
        obj.insert(
            "age".to_string(),
            serde_json::Value::Number(serde_json::Number::from(30)),
        );
        let tv = TypedValue::json(serde_json::Value::Object(obj));
        let pg_val: PostgreSQLValue = tv.into();
        if let PostgreSQLValue::Json(json) = pg_val {
            assert_eq!(json["name"], "Alice");
            assert_eq!(json["age"], 30);
        } else {
            panic!("Expected Json value");
        }
    }
}
