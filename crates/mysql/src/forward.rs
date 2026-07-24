//! Forward conversion: Value/TypedValue → MySQLValue
//!
//! This module implements `From<Value>` and `From<TypedValue>` for `MySQLValue`,
//! converting sync-core's values into MySQL-compatible values for INSERT operations.
//! The TypedValue implementation delegates to Value since MySQL doesn't need
//! type metadata for conversion.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{Datelike, Timelike};
use mysql_async::Value as MysqlAsyncValue;
use surreal_sync_core::{TypedValue, Value};

#[cfg(test)]
use surreal_sync_core::Type;

/// MySQL value wrapper for type-safe conversions.
#[derive(Debug, Clone)]
pub struct MySQLValue(pub MysqlAsyncValue);

impl MySQLValue {
    /// Get the inner mysql_async::Value.
    pub fn into_inner(self) -> MysqlAsyncValue {
        self.0
    }

    /// Get a reference to the inner value.
    pub fn as_inner(&self) -> &MysqlAsyncValue {
        &self.0
    }
}

impl From<Value> for MySQLValue {
    fn from(value: Value) -> Self {
        match value {
            // Null
            Value::Null => MySQLValue(MysqlAsyncValue::NULL),

            // Boolean - MySQL uses TINYINT(1)
            Value::Bool(b) => MySQLValue(MysqlAsyncValue::Int(if b { 1 } else { 0 })),

            // Integer types
            Value::Int8 { value, .. } => MySQLValue(MysqlAsyncValue::Int(value as i64)),
            Value::Int16(i) => MySQLValue(MysqlAsyncValue::Int(i as i64)),
            Value::Int32(i) => MySQLValue(MysqlAsyncValue::Int(i as i64)),
            Value::Int64(i) => MySQLValue(MysqlAsyncValue::Int(i)),

            // Floating point
            Value::Float32(f) => MySQLValue(MysqlAsyncValue::Float(f)),
            Value::Float64(f) => MySQLValue(MysqlAsyncValue::Double(f)),

            // Decimal - stored as string in MySQL for precision
            Value::Decimal { value, .. } => MySQLValue(MysqlAsyncValue::Bytes(value.into_bytes())),

            // String types
            Value::Char { value, .. } => MySQLValue(MysqlAsyncValue::Bytes(value.into_bytes())),
            Value::VarChar { value, .. } => MySQLValue(MysqlAsyncValue::Bytes(value.into_bytes())),
            Value::Text(s) => MySQLValue(MysqlAsyncValue::Bytes(s.into_bytes())),

            // Binary types
            Value::Blob(b) => MySQLValue(MysqlAsyncValue::Bytes(b)),
            Value::Bytes(b) => MySQLValue(MysqlAsyncValue::Bytes(b)),

            // UUID - MySQL stores as CHAR(36)
            Value::Uuid(u) => MySQLValue(MysqlAsyncValue::Bytes(u.to_string().into_bytes())),

            // ULID - MySQL stores as CHAR(26)
            Value::Ulid(u) => MySQLValue(MysqlAsyncValue::Bytes(u.to_string().into_bytes())),

            // Date - MySQL DATE (only date part, no time)
            Value::Date(dt) => MySQLValue(MysqlAsyncValue::Date(
                dt.year() as u16,
                dt.month() as u8,
                dt.day() as u8,
                0,
                0,
                0,
                0,
            )),

            // Time - MySQL TIME
            Value::Time(dt) => MySQLValue(MysqlAsyncValue::Time(
                false, // not negative
                0,     // days
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1000, // MySQL uses microseconds
            )),

            // DateTime variants - MySQL DATETIME(6) and TIMESTAMP
            Value::LocalDateTime(dt) | Value::LocalDateTimeNano(dt) | Value::ZonedDateTime(dt) => {
                MySQLValue(MysqlAsyncValue::Date(
                    dt.year() as u16,
                    dt.month() as u8,
                    dt.day() as u8,
                    dt.hour() as u8,
                    dt.minute() as u8,
                    dt.second() as u8,
                    dt.nanosecond() / 1000, // MySQL uses microseconds
                ))
            }

            // TimeTz - MySQL doesn't have native TIMETZ, store as VARCHAR
            // Note: We intentionally do NOT use a datetime type because time and datetime
            // are fundamentally different types.
            Value::TimeTz(s) => MySQLValue(MysqlAsyncValue::Bytes(s.into_bytes())),

            // JSON - MySQL JSON type
            Value::Json(json_val) | Value::Jsonb(json_val) => {
                MySQLValue(MysqlAsyncValue::Bytes(json_val.to_string().into_bytes()))
            }

            // Array - MySQL stores as JSON
            Value::Array { elements, .. } => {
                let json = generated_array_to_json(elements);
                MySQLValue(MysqlAsyncValue::Bytes(json.to_string().into_bytes()))
            }

            // Set - MySQL SET type (comma-separated values)
            Value::Set { elements, .. } => {
                MySQLValue(MysqlAsyncValue::Bytes(elements.join(",").into_bytes()))
            }

            // Enum - MySQL ENUM
            Value::Enum { value, .. } => MySQLValue(MysqlAsyncValue::Bytes(value.into_bytes())),

            // Geometry - stored as GeoJSON string
            Value::Geometry { data, .. } => {
                use surreal_sync_core::values::GeometryData;
                let GeometryData(json_val) = data;
                MySQLValue(MysqlAsyncValue::Bytes(json_val.to_string().into_bytes()))
            }

            // Duration - store as ISO 8601 duration string
            Value::Duration(d) => {
                let secs = d.as_secs();
                let nanos = d.subsec_nanos();
                let duration_str = if nanos == 0 {
                    format!("PT{secs}S")
                } else {
                    format!("PT{secs}.{nanos:09}S")
                };
                MySQLValue(MysqlAsyncValue::Bytes(duration_str.into_bytes()))
            }

            // Thing - record reference as "table:id" format
            Value::Thing { table, id } => {
                let id_str = match id.as_ref() {
                    Value::Text(s) => s.clone(),
                    Value::Int32(i) => i.to_string(),
                    Value::Int64(i) => i.to_string(),
                    Value::Uuid(u) => u.to_string(),
                    other => panic!(
                        "Unsupported Thing ID type for MySQL: {other:?}. \
                         Supported types: Text, Int32, Int64, Uuid"
                    ),
                };
                MySQLValue(MysqlAsyncValue::Bytes(
                    format!("{table}:{id_str}").into_bytes(),
                ))
            }

            // Object - nested document as JSON
            Value::Object(map) => {
                let obj: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, generated_to_json(v)))
                    .collect();
                let json_str =
                    serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default();
                MySQLValue(MysqlAsyncValue::Bytes(json_str.into_bytes()))
            }

            Value::ZeroTemporal {
                intended_type,
                source,
            } => {
                let s = source
                    .unwrap_or_else(|| Value::canonical_zero_literal(&intended_type).to_string());
                MySQLValue(MysqlAsyncValue::Bytes(s.into_bytes()))
            }
        }
    }
}

impl From<TypedValue> for MySQLValue {
    fn from(tv: TypedValue) -> Self {
        MySQLValue::from(tv.value)
    }
}

/// Convert a Value array to a serde_json::Value array.
fn generated_array_to_json(arr: Vec<Value>) -> serde_json::Value {
    let values: Vec<serde_json::Value> = arr.into_iter().map(generated_to_json).collect();
    serde_json::Value::Array(values)
}

/// Convert any Value to serde_json::Value.
fn generated_to_json(gv: Value) -> serde_json::Value {
    match gv {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int8 { value, .. } => serde_json::Value::Number(serde_json::Number::from(value)),
        Value::Int16(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        Value::Int32(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        Value::Int64(i) => serde_json::Value::Number(serde_json::Number::from(i)),
        Value::Float32(f) => serde_json::Number::from_f64(f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Float64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Char { value, .. } => serde_json::Value::String(value),
        Value::VarChar { value, .. } => serde_json::Value::String(value),
        Value::Text(s) => serde_json::Value::String(s),
        Value::Blob(b) => serde_json::Value::String(BASE64.encode(&b)),
        Value::Bytes(b) => serde_json::Value::String(BASE64.encode(&b)),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Ulid(u) => serde_json::Value::String(u.to_string()),
        Value::Date(dt) => serde_json::Value::String(dt.format("%Y-%m-%d").to_string()),
        Value::Time(dt) => serde_json::Value::String(dt.format("%H:%M:%S%.f").to_string()),
        Value::LocalDateTime(dt) | Value::LocalDateTimeNano(dt) | Value::ZonedDateTime(dt) => {
            serde_json::Value::String(dt.to_rfc3339())
        }
        Value::TimeTz(s) => serde_json::Value::String(s),
        Value::Decimal { value, .. } => serde_json::Value::String(value),
        Value::Array { elements, .. } => generated_array_to_json(elements),
        Value::Set { elements, .. } => {
            let arr: Vec<serde_json::Value> = elements
                .into_iter()
                .map(serde_json::Value::String)
                .collect();
            serde_json::Value::Array(arr)
        }
        Value::Enum { value, .. } => serde_json::Value::String(value),
        Value::Json(json_val) | Value::Jsonb(json_val) => *json_val,
        Value::Geometry { data, .. } => {
            use surreal_sync_core::values::GeometryData;
            let GeometryData(json_val) = data;
            json_val
        }
        Value::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                serde_json::Value::String(format!("PT{secs}S"))
            } else {
                serde_json::Value::String(format!("PT{secs}.{nanos:09}S"))
            }
        }
        Value::Thing { table, id } => {
            let id_str = match id.as_ref() {
                Value::Text(s) => s.clone(),
                Value::Int32(i) => i.to_string(),
                Value::Int64(i) => i.to_string(),
                Value::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for MySQL JSON: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            serde_json::Value::String(format!("{table}:{id_str}"))
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, generated_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::ZeroTemporal {
            intended_type,
            source,
        } => {
            let s =
                source.unwrap_or_else(|| Value::canonical_zero_literal(&intended_type).to_string());
            serde_json::Value::String(s)
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
        assert!(matches!(mysql_val.0, MysqlAsyncValue::Int(1)));

        let tv = TypedValue::bool(false);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, MysqlAsyncValue::Int(0)));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int32(42);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, MysqlAsyncValue::Int(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9_223_372_036_854_775_807i64);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(
            mysql_val.0,
            MysqlAsyncValue::Int(9_223_372_036_854_775_807)
        ));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.234);
        let mysql_val: MySQLValue = tv.into();
        if let MysqlAsyncValue::Float(f) = mysql_val.0 {
            assert!((f - 1.234).abs() < 0.01);
        } else {
            panic!("Expected Float value");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::float64(1.23456789012345);
        let mysql_val: MySQLValue = tv.into();
        if let MysqlAsyncValue::Double(d) = mysql_val.0 {
            assert!((d - 1.23456789012345).abs() < 0.0001);
        } else {
            panic!("Expected Double value");
        }
    }

    #[test]
    fn test_string_conversion() {
        let tv = TypedValue::text("hello world".to_string());
        let mysql_val: MySQLValue = tv.into();
        if let MysqlAsyncValue::Bytes(b) = mysql_val.0 {
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
        if let MysqlAsyncValue::Bytes(b) = mysql_val.0 {
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
        if let MysqlAsyncValue::Date(year, month, day, hour, min, sec, _) = mysql_val.0 {
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
        let tv = TypedValue::null(Type::Text);
        let mysql_val: MySQLValue = tv.into();
        assert!(matches!(mysql_val.0, MysqlAsyncValue::NULL));
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.456".to_string(), 10, 3);
        let mysql_val: MySQLValue = tv.into();
        if let MysqlAsyncValue::Bytes(b) = mysql_val.0 {
            assert_eq!(String::from_utf8(b).unwrap(), "123.456");
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_array_conversion() {
        let arr = vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)];
        let tv = TypedValue::array(arr, Type::Int64);
        let mysql_val: MySQLValue = tv.into();
        if let MysqlAsyncValue::Bytes(b) = mysql_val.0 {
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
        if let MysqlAsyncValue::Bytes(b) = mysql_val.0 {
            let json: serde_json::Value = serde_json::from_slice(&b).unwrap();
            assert_eq!(json["name"], "Alice");
            assert_eq!(json["age"], 30);
        } else {
            panic!("Expected Bytes value");
        }
    }
}
