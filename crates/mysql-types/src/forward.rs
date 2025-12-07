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
            (UniversalType::TinyInt { .. }, UniversalValue::TinyInt { value, .. }) => {
                MySQLValue(Value::Int(value as i64))
            }
            (UniversalType::SmallInt, UniversalValue::SmallInt(i)) => {
                MySQLValue(Value::Int(i as i64))
            }
            (UniversalType::Int, UniversalValue::Int(i)) => MySQLValue(Value::Int(i as i64)),
            (UniversalType::BigInt, UniversalValue::BigInt(i)) => MySQLValue(Value::Int(i)),

            // Floating point
            (UniversalType::Float, UniversalValue::Float(f)) => MySQLValue(Value::Float(f)),
            (UniversalType::Double, UniversalValue::Double(f)) => MySQLValue(Value::Double(f)),

            // Decimal - stored as string in MySQL for precision
            (UniversalType::Decimal { .. }, UniversalValue::Decimal { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }
            (UniversalType::Decimal { .. }, UniversalValue::Text(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // String types
            (UniversalType::Char { .. }, UniversalValue::Char { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }
            (UniversalType::VarChar { .. }, UniversalValue::VarChar { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }
            (UniversalType::Text, UniversalValue::Text(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Binary types
            (UniversalType::Blob, UniversalValue::Bytes(b)) => MySQLValue(Value::Bytes(b)),
            (UniversalType::Bytes, UniversalValue::Bytes(b)) => MySQLValue(Value::Bytes(b)),

            // UUID - MySQL stores as CHAR(36)
            (UniversalType::Uuid, UniversalValue::Uuid(u)) => {
                MySQLValue(Value::Bytes(u.to_string().into_bytes()))
            }
            (UniversalType::Uuid, UniversalValue::Text(s)) => {
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
            (UniversalType::Date, UniversalValue::Date(dt)) => MySQLValue(Value::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                0,
                0,
                0,
                0,
            )),
            (UniversalType::Date, UniversalValue::Text(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Time - MySQL TIME
            (UniversalType::Time, UniversalValue::Time(dt)) => MySQLValue(Value::Time(
                false, // not negative
                0,     // days
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000,
            )),
            (UniversalType::Time, UniversalValue::Text(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // JSON - MySQL JSON type
            (UniversalType::Json, UniversalValue::Json(json_val)) => {
                MySQLValue(Value::Bytes(json_val.to_string().into_bytes()))
            }
            (UniversalType::Json, UniversalValue::Text(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }
            (UniversalType::Jsonb, UniversalValue::Jsonb(json_val)) => {
                MySQLValue(Value::Bytes(json_val.to_string().into_bytes()))
            }
            (UniversalType::Jsonb, UniversalValue::Text(s)) => {
                MySQLValue(Value::Bytes(s.into_bytes()))
            }

            // Array - MySQL stores as JSON
            (UniversalType::Array { .. }, UniversalValue::Array { elements, .. }) => {
                let json = generated_array_to_json(elements);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }

            // Set - MySQL SET type
            (UniversalType::Set { .. }, UniversalValue::Set { elements, .. }) => {
                MySQLValue(Value::Bytes(elements.join(",").into_bytes()))
            }

            // Enum - MySQL ENUM
            (UniversalType::Enum { .. }, UniversalValue::Enum { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }

            // Geometry - stored as GeoJSON string
            (UniversalType::Geometry { .. }, UniversalValue::Geometry { data, .. }) => {
                use sync_core::values::GeometryData;
                let GeometryData(json_val) = data;
                MySQLValue(Value::Bytes(json_val.to_string().into_bytes()))
            }

            // Null
            (_, UniversalValue::Null) => MySQLValue(Value::NULL),

            // Fallback for type mismatches - try to do reasonable conversion
            (_, UniversalValue::Bool(b)) => MySQLValue(Value::Int(if b { 1 } else { 0 })),
            (_, UniversalValue::TinyInt { value, .. }) => MySQLValue(Value::Int(value as i64)),
            (_, UniversalValue::SmallInt(i)) => MySQLValue(Value::Int(i as i64)),
            (_, UniversalValue::Int(i)) => MySQLValue(Value::Int(i as i64)),
            (_, UniversalValue::BigInt(i)) => MySQLValue(Value::Int(i)),
            (_, UniversalValue::Float(f)) => MySQLValue(Value::Float(f)),
            (_, UniversalValue::Double(f)) => MySQLValue(Value::Double(f)),
            (_, UniversalValue::Char { value, .. }) => MySQLValue(Value::Bytes(value.into_bytes())),
            (_, UniversalValue::VarChar { value, .. }) => {
                MySQLValue(Value::Bytes(value.into_bytes()))
            }
            (_, UniversalValue::Text(s)) => MySQLValue(Value::Bytes(s.into_bytes())),
            (_, UniversalValue::Blob(b)) => MySQLValue(Value::Bytes(b)),
            (_, UniversalValue::Bytes(b)) => MySQLValue(Value::Bytes(b)),
            (_, UniversalValue::Uuid(u)) => MySQLValue(Value::Bytes(u.to_string().into_bytes())),
            (_, UniversalValue::Date(dt))
            | (_, UniversalValue::Time(dt))
            | (_, UniversalValue::DateTime(dt))
            | (_, UniversalValue::DateTimeNano(dt))
            | (_, UniversalValue::TimestampTz(dt)) => MySQLValue(Value::Date(
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
            (_, UniversalValue::Array { elements, .. }) => {
                let json = generated_array_to_json(elements);
                MySQLValue(Value::Bytes(json.to_string().into_bytes()))
            }
            (_, UniversalValue::Set { elements, .. }) => {
                MySQLValue(Value::Bytes(elements.join(",").into_bytes()))
            }
            (_, UniversalValue::Enum { value, .. }) => MySQLValue(Value::Bytes(value.into_bytes())),
            (_, UniversalValue::Json(json_val)) | (_, UniversalValue::Jsonb(json_val)) => {
                MySQLValue(Value::Bytes(json_val.to_string().into_bytes()))
            }
            (_, UniversalValue::Geometry { data, .. }) => {
                use sync_core::values::GeometryData;
                let GeometryData(json_val) = data;
                MySQLValue(Value::Bytes(json_val.to_string().into_bytes()))
            }
        }
    }
}

/// Convert a UniversalValue array to a serde_json::Value array.
fn generated_array_to_json(arr: Vec<UniversalValue>) -> serde_json::Value {
    let values: Vec<serde_json::Value> = arr.into_iter().map(generated_to_json).collect();
    serde_json::Value::Array(values)
}

/// Convert any UniversalValue to serde_json::Value.
fn generated_to_json(gv: UniversalValue) -> serde_json::Value {
    match gv {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => serde_json::Value::Bool(b),
        UniversalValue::TinyInt { value, .. } => {
            serde_json::Value::Number(serde_json::Number::from(value))
        }
        UniversalValue::SmallInt(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        UniversalValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        UniversalValue::BigInt(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        UniversalValue::Float(f) => serde_json::Number::from_f64(f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        UniversalValue::Double(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        UniversalValue::Char { value, .. } => serde_json::Value::String(value),
        UniversalValue::VarChar { value, .. } => serde_json::Value::String(value),
        UniversalValue::Text(s) => serde_json::Value::String(s),
        UniversalValue::Blob(b) => serde_json::Value::String(BASE64.encode(&b)),
        UniversalValue::Bytes(b) => serde_json::Value::String(BASE64.encode(&b)),
        UniversalValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        UniversalValue::Date(dt)
        | UniversalValue::Time(dt)
        | UniversalValue::DateTime(dt)
        | UniversalValue::DateTimeNano(dt)
        | UniversalValue::TimestampTz(dt) => serde_json::Value::String(dt.to_rfc3339()),
        UniversalValue::Decimal { value, .. } => serde_json::Value::String(value),
        UniversalValue::Array { elements, .. } => generated_array_to_json(elements),
        UniversalValue::Set { elements, .. } => {
            let arr: Vec<serde_json::Value> = elements
                .into_iter()
                .map(serde_json::Value::String)
                .collect();
            serde_json::Value::Array(arr)
        }
        UniversalValue::Enum { value, .. } => serde_json::Value::String(value),
        UniversalValue::Json(json_val) | UniversalValue::Jsonb(json_val) => *json_val,
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(json_val) = data;
            json_val
        }
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
            UniversalValue::BigInt(1),
            UniversalValue::BigInt(2),
            UniversalValue::BigInt(3),
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
            serde_json::Value::String("Alice".to_string()),
        );
        obj.insert(
            "age".to_string(),
            serde_json::Value::Number(serde_json::Number::from(30)),
        );
        let tv = TypedValue::json(serde_json::Value::Object(serde_json::Map::from_iter(obj)));
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
