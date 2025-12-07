//! Forward conversion: TypedValue → MySQLValue
//!
//! This module implements `From<TypedValue>` for `MySQLValue`, converting
//! sync-core's type-safe values into MySQL-compatible values for INSERT operations.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{Datelike, Timelike};
use mysql_async::Value;
use sync_core::{TypedValue, UniversalType, UniversalValue};

/// MySQL value wrapper for type-safe conversions.
#[derive(Debug, Clone)]
pub struct MySQLValue(pub Value);

impl MySQLValue {
    /// Get the inner mysql_async::Value.
    pub fn into_inner(self) -> Value {
        self.0
    }

    /// Get a reference to the inner value.
    pub fn as_inner(&self) -> &Value {
        &self.0
    }
}

impl From<TypedValue> for MySQLValue {
    fn from(tv: TypedValue) -> Self {
        match (&tv.sync_type, tv.value) {
            // Boolean - MySQL uses TINYINT(1)
            (UniversalType::Bool, UniversalValue::Bool(b)) => {
                MySQLValue(Value::Int(if b { 1 } else { 0 }))
            }

            // Integer types
            (UniversalType::TinyInt { .. }, UniversalValue::Int32(i)) => {
                MySQLValue(Value::Int(i as i64))
            }
            (UniversalType::SmallInt, UniversalValue::Int32(i)) => MySQLValue(Value::Int(i as i64)),
            (UniversalType::Int, UniversalValue::Int32(i)) => MySQLValue(Value::Int(i as i64)),
            (UniversalType::Int, UniversalValue::Int64(i)) => MySQLValue(Value::Int(i)),
            (UniversalType::BigInt, UniversalValue::Int64(i)) => MySQLValue(Value::Int(i)),

            // Floating point
            (UniversalType::Float, UniversalValue::Float64(f)) => {
                MySQLValue(Value::Float(f as f32))
            }
            (UniversalType::Double, UniversalValue::Float64(f)) => MySQLValue(Value::Double(f)),

            // Decimal - stored as string in MySQL for precision
            (UniversalType::Decimal { .. }, UniversalValue::Decimal { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }
            (UniversalType::Decimal { .. }, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // String types
            (UniversalType::Char { .. }, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }
            (UniversalType::VarChar { .. }, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }
            (UniversalType::Text, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Binary types
            (UniversalType::Blob, UniversalValue::Bytes(b)) => MySQLValue(Value::Bytes(b)),
            (UniversalType::Bytes, UniversalValue::Bytes(b)) => MySQLValue(Value::Bytes(b)),

            // UUID - MySQL stores as CHAR(36)
            (UniversalType::Uuid, UniversalValue::Uuid(u)) => {
                MySQLValue(Value::Bytes(u.to_string().into_bytes()))
            }
            (UniversalType::Uuid, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // DateTime - MySQL DATETIME(6)
            (UniversalType::DateTime, UniversalValue::DateTime(dt)) => MySQLValue(Value::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000, // MySQL uses microseconds
            )),

            // DateTimeNano - Same as DateTime but with full nanosecond precision
            // MySQL only supports microseconds, so we truncate
            (UniversalType::DateTimeNano, UniversalValue::DateTime(dt)) => MySQLValue(Value::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000,
            )),

            // TimestampTz - MySQL TIMESTAMP
            (UniversalType::TimestampTz, UniversalValue::DateTime(dt)) => MySQLValue(Value::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000,
            )),

            // Date - MySQL DATE
            (UniversalType::Date, UniversalValue::DateTime(dt)) => MySQLValue(Value::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                0,
                0,
                0,
                0,
            )),
            (UniversalType::Date, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Time - MySQL TIME
            (UniversalType::Time, UniversalValue::DateTime(dt)) => MySQLValue(Value::Time(
                false, // not negative
                0,     // days
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000,
            )),
            (UniversalType::Time, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // JSON - MySQL JSON type
            (UniversalType::Json, UniversalValue::Object(obj)) => {
                let json = generated_object_to_json(obj);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }
            (UniversalType::Json, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }
            (UniversalType::Jsonb, UniversalValue::Object(obj)) => {
                let json = generated_object_to_json(obj);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }
            (UniversalType::Jsonb, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Array - MySQL stores as JSON
            (UniversalType::Array { .. }, UniversalValue::Array(arr)) => {
                let json = generated_array_to_json(arr);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }

            // Set - MySQL SET type
            (UniversalType::Set { .. }, UniversalValue::Array(arr)) => {
                let values: Vec<String> = arr
                    .into_iter()
                    .filter_map(|v| {
                        if let UniversalValue::String(s) = v {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .collect();
                MySQLValue(Value::Bytes(values.join(",").into_bytes()))
            }

            // Enum - MySQL ENUM
            (UniversalType::Enum { .. }, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Geometry - stored as WKB (Well-Known Binary)
            (UniversalType::Geometry { .. }, UniversalValue::Bytes(b)) => {
                MySQLValue(Value::Bytes(b))
            }
            (UniversalType::Geometry { .. }, UniversalValue::String(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Null
            (_, UniversalValue::Null) => MySQLValue(Value::NULL),

            // Fallback for type mismatches - try to do reasonable conversion
            (_, UniversalValue::Bool(b)) => MySQLValue(Value::Int(if b { 1 } else { 0 })),
            (_, UniversalValue::Int32(i)) => MySQLValue(Value::Int(i as i64)),
            (_, UniversalValue::Int64(i)) => MySQLValue(Value::Int(i)),
            (_, UniversalValue::Float64(f)) => MySQLValue(Value::Double(f)),
            (_, UniversalValue::String(s)) => MySQLValue(Value::Bytes(s.into_bytes())),
            (_, UniversalValue::Bytes(b)) => MySQLValue(Value::Bytes(b)),
            (_, UniversalValue::Uuid(u)) => MySQLValue(Value::Bytes(u.to_string().into_bytes())),
            (_, UniversalValue::DateTime(dt)) => MySQLValue(Value::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000,
            )),
            (_, UniversalValue::Decimal { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }
            (_, UniversalValue::Array(arr)) => {
                let json = generated_array_to_json(arr);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }
            (_, UniversalValue::Object(obj)) => {
                let json = generated_object_to_json(obj);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }
        }
    }
}

/// Convert a UniversalValue array to a serde_json::Value array.
fn generated_array_to_json(arr: Vec<UniversalValue>) -> serde_json::Value {
    let values: Vec<serde_json::Value> = arr.into_iter().map(generated_to_json).collect();
    serde_json::Value::Array(values)
}

/// Convert a UniversalValue object to a serde_json::Value object.
fn generated_object_to_json(
    obj: std::collections::HashMap<String, UniversalValue>,
) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = obj
        .into_iter()
        .map(|(k, v)| (k, generated_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

/// Convert any UniversalValue to serde_json::Value.
fn generated_to_json(gv: UniversalValue) -> serde_json::Value {
    match gv {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => serde_json::Value::Bool(b),
        UniversalValue::Int32(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        UniversalValue::Int64(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        UniversalValue::Float64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        UniversalValue::String(s) => serde_json::Value::String(s),
        UniversalValue::Bytes(b) => serde_json::Value::String(BASE64.encode(&b)),
        UniversalValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        UniversalValue::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        UniversalValue::Decimal { value, .. } => serde_json::Value::String(value),
        UniversalValue::Array(arr) => generated_array_to_json(arr),
        UniversalValue::Object(obj) => generated_object_to_json(obj),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, Value::Int(1)));

        let tv = TypedValue::bool(false);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, Value::Int(0)));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int(42);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, Value::Int(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::bigint(9_223_372_036_854_775_807i64);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, Value::Int(9_223_372_036_854_775_807)));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float(1.234);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Float(f) = mysql_val.0 {
            assert!((f - 1.234).abs() < 0.01);
        } else {
            panic!("Expected Float value");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::double(1.23456789012345);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Double(d) = mysql_val.0 {
            assert!((d - 1.23456789012345).abs() < 0.0001);
        } else {
            panic!("Expected Double value");
        }
    }

    #[test]
    fn test_string_conversion() {
        let tv = TypedValue::text("hello world".to_string());
        let mysql_val: MySQLValue = tv.into();
        if let Value::Bytes(b) = mysql_val.0 {
            assert_eq!(String::from_utf8(b).unwrap(), "hello world");
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(uuid);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Bytes(b) = mysql_val.0 {
            assert_eq!(
                String::from_utf8(b).unwrap(),
                "550e8400-e29b-41d4-a716-446655440000"
            );
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 45).unwrap();
        let tv = TypedValue::datetime(dt);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Date(year, month, day, hour, min, sec, _) = mysql_val.0 {
            assert_eq!(year, 2024);
            assert_eq!(month, 6);
            assert_eq!(day, 15);
            assert_eq!(hour, 10);
            assert_eq!(min, 30);
            assert_eq!(sec, 45);
        } else {
            panic!("Expected Date value");
        }
    }

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(UniversalType::Text);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, Value::NULL));
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.456".to_string(), 10, 3);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Bytes(b) = mysql_val.0 {
            assert_eq!(String::from_utf8(b).unwrap(), "123.456");
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_array_conversion() {
        let arr = vec![
            UniversalValue::Int64(1),
            UniversalValue::Int64(2),
            UniversalValue::Int64(3),
        ];
        let tv = TypedValue::array(arr, UniversalType::BigInt);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Bytes(b) = mysql_val.0 {
            let json: serde_json::Value = serde_json::from_slice(&b).unwrap();
            assert_eq!(json, serde_json::json!([1, 2, 3]));
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "name".to_string(),
            UniversalValue::String("Alice".to_string()),
        );
        obj.insert("age".to_string(), UniversalValue::Int64(30));
        let tv = TypedValue::json_object(obj);
        let mysql_val: MySQLValue = tv.into();
        if let Value::Bytes(b) = mysql_val.0 {
            let json: serde_json::Value = serde_json::from_slice(&b).unwrap();
            assert_eq!(json["name"], "Alice");
            assert_eq!(json["age"], 30);
        } else {
            panic!("Expected Bytes value");
        }
    }
}
