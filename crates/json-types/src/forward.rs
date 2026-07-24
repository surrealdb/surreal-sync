//! Forward conversion: TypedValue → JSON value.
//!
//! This module provides conversion from sync-core's `TypedValue` to JSON values.

use base64::Engine;
use serde_json::json;
use sync_core::{GeometryType, TypedValue, Value};

/// Wrapper for JSON values.
#[derive(Debug, Clone)]
pub struct JsonValue(pub serde_json::Value);

impl JsonValue {
    /// Get the inner JSON value.
    pub fn into_inner(self) -> serde_json::Value {
        self.0
    }

    /// Get a reference to the inner JSON value.
    pub fn as_inner(&self) -> &serde_json::Value {
        &self.0
    }
}

impl From<TypedValue> for JsonValue {
    fn from(tv: TypedValue) -> Self {
        JsonValue::from(tv.value)
    }
}

impl From<Value> for JsonValue {
    fn from(value: Value) -> Self {
        match value {
            // Null
            Value::Null => JsonValue(serde_json::Value::Null),

            // Boolean
            Value::Bool(b) => JsonValue(json!(b)),

            // Integer types
            Value::Int8 { value, .. } => JsonValue(json!(value)),
            Value::Int16(i) => JsonValue(json!(i)),
            Value::Int32(i) => JsonValue(json!(i)),
            Value::Int64(i) => JsonValue(json!(i)),

            // Floating point
            Value::Float32(f) => JsonValue(json!(f)),
            Value::Float64(f) => JsonValue(json!(f)),

            // Decimal - store as string to preserve precision
            Value::Decimal { value, .. } => JsonValue(json!(value)),

            // String types
            Value::Char { value, .. } => JsonValue(json!(value)),
            Value::VarChar { value, .. } => JsonValue(json!(value)),
            Value::Text(s) => JsonValue(json!(s)),

            // Binary types - base64 encode
            Value::Blob(b) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                JsonValue(json!(encoded))
            }
            Value::Bytes(b) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                JsonValue(json!(encoded))
            }

            // Date/time types - ISO 8601 format
            Value::Date(dt) => JsonValue(json!(dt.format("%Y-%m-%d").to_string())),
            // Note: Using "%H:%M:%S%.f" to preserve fractional seconds from PostgreSQL TIME type.
            Value::Time(dt) => JsonValue(json!(dt.format("%H:%M:%S%.f").to_string())),
            Value::LocalDateTime(dt) => JsonValue(json!(dt.to_rfc3339())),
            Value::LocalDateTimeNano(dt) => JsonValue(json!(dt.to_rfc3339())),
            Value::ZonedDateTime(dt) => JsonValue(json!(dt.to_rfc3339())),
            // TIMETZ - stored as string to preserve timezone format
            Value::TimeTz(s) => JsonValue(json!(s)),

            // UUID
            Value::Uuid(u) => JsonValue(json!(u.to_string())),

            // ULID
            Value::Ulid(u) => JsonValue(json!(u.to_string())),

            // JSON types - already serde_json::Value
            Value::Json(payload) => JsonValue((*payload).clone()),
            Value::Jsonb(payload) => JsonValue((*payload).clone()),

            // Array types - recursively convert elements
            Value::Array { elements, .. } => {
                let json_arr: Vec<serde_json::Value> = elements
                    .into_iter()
                    .map(|v| JsonValue::from(v).into_inner())
                    .collect();
                JsonValue(json!(json_arr))
            }

            // Set - stored as array of strings
            Value::Set { elements, .. } => {
                let json_arr: Vec<serde_json::Value> = elements.iter().map(|s| json!(s)).collect();
                JsonValue(json!(json_arr))
            }

            // Enum - stored as string
            Value::Enum { value, .. } => JsonValue(json!(value)),

            // Geometry types - GeoJSON format
            Value::Geometry {
                data,
                geometry_type,
            } => {
                use sync_core::values::GeometryData;
                let GeometryData(json_obj) = data;
                if let serde_json::Value::Object(mut obj) = json_obj.clone() {
                    let type_str = match geometry_type {
                        GeometryType::Point => "Point",
                        GeometryType::LineString => "LineString",
                        GeometryType::Polygon => "Polygon",
                        GeometryType::MultiPoint => "MultiPoint",
                        GeometryType::MultiLineString => "MultiLineString",
                        GeometryType::MultiPolygon => "MultiPolygon",
                        GeometryType::GeometryCollection => "GeometryCollection",
                    };
                    obj.insert("type".to_string(), json!(type_str));
                    JsonValue(serde_json::Value::Object(obj))
                } else {
                    JsonValue(json_obj.clone())
                }
            }

            // Duration - ISO 8601 format
            Value::Duration(d) => {
                let secs = d.as_secs();
                let nanos = d.subsec_nanos();
                if nanos == 0 {
                    JsonValue(json!(format!("PT{secs}S")))
                } else {
                    JsonValue(json!(format!("PT{secs}.{nanos:09}S")))
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
                JsonValue(json!(format!("{table}:{id_str}")))
            }

            // Object - nested document
            Value::Object(map) => {
                let obj: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, JsonValue::from(v).into_inner()))
                    .collect();
                JsonValue(serde_json::Value::Object(obj))
            }

            // Zero temporal - preserve as MySQL-style literal string
            Value::ZeroTemporal {
                intended_type,
                source,
            } => {
                let s = source
                    .unwrap_or_else(|| Value::canonical_zero_literal(&intended_type).to_string());
                JsonValue(json!(s))
            }
        }
    }
}

/// Convert a Value to a JSON value (without type context).
#[allow(dead_code)]
fn generated_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => json!(*b),
        Value::Int8 { value, .. } => json!(*value),
        Value::Int16(i) => json!(*i),
        Value::Int32(i) => json!(*i),
        Value::Int64(i) => json!(*i),
        Value::Float32(f) => json!(*f),
        Value::Float64(f) => json!(*f),
        Value::Char { value, .. } => json!(value),
        Value::VarChar { value, .. } => json!(value),
        Value::Text(s) => json!(s),
        Value::Blob(b) | Value::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            json!(encoded)
        }
        Value::Uuid(u) => json!(u.to_string()),
        Value::Ulid(u) => json!(u.to_string()),
        Value::Date(dt) => json!(dt.format("%Y-%m-%d").to_string()),
        Value::Time(dt) => json!(dt.format("%H:%M:%S%.f").to_string()),
        Value::LocalDateTime(dt) | Value::LocalDateTimeNano(dt) | Value::ZonedDateTime(dt) => {
            json!(dt.to_rfc3339())
        }
        Value::TimeTz(s) => json!(s),
        Value::Decimal { value, .. } => json!(value),
        Value::Array { elements, .. } => {
            json!(elements
                .iter()
                .map(generated_value_to_json)
                .collect::<Vec<_>>())
        }
        Value::Set { elements, .. } => {
            json!(elements)
        }
        Value::Enum { value, .. } => json!(value),
        Value::Json(payload) | Value::Jsonb(payload) => (**payload).clone(),
        Value::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(value) = data;
            value.clone()
        }
        Value::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                json!(format!("PT{secs}S"))
            } else {
                json!(format!("PT{secs}.{nanos:09}S"))
            }
        }
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
            json!(format!("{table}:{id_str}"))
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), generated_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::ZeroTemporal {
            intended_type,
            source,
        } => {
            let s = source
                .as_deref()
                .unwrap_or_else(|| Value::canonical_zero_literal(intended_type));
            json!(s)
        }
    }
}

/// Convert a complete row to a JSON object.
pub fn typed_values_to_json<I>(fields: I) -> serde_json::Value
where
    I: IntoIterator<Item = (String, TypedValue)>,
{
    let mut obj = serde_json::Map::new();
    for (name, tv) in fields {
        obj.insert(name, JsonValue::from(tv).into_inner());
    }
    serde_json::Value::Object(obj)
}

/// Convert a complete row to a JSONL line (JSON string with newline).
pub fn typed_values_to_jsonl<I>(fields: I) -> String
where
    I: IntoIterator<Item = (String, TypedValue)>,
{
    let json = typed_values_to_json(fields);
    format!("{}\n", serde_json::to_string(&json).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use sync_core::Type;

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(Type::Text);
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_null());
    }

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(true));

        let tv = TypedValue::bool(false);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(false));
    }

    #[test]
    fn test_tinyint_conversion() {
        let tv = TypedValue::int8(127, 4);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(127));
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::int16(32000);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(32000));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int32(12345);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(12345));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9876543210i64);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(9876543210i64));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.234);
        let json_val: JsonValue = tv.into();
        if let Some(f) = json_val.0.as_f64() {
            assert!((f - 1.234f64).abs() < 0.001);
        } else {
            panic!("Expected number");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::float64(1.23456);
        let json_val: JsonValue = tv.into();
        if let Some(f) = json_val.0.as_f64() {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected number");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.45", 10, 2);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("123.45"));
    }

    #[test]
    fn test_char_conversion() {
        let tv = TypedValue::char_type("test", 10);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("test"));
    }

    #[test]
    fn test_text_conversion() {
        let tv = TypedValue::text("hello world");
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("hello world"));
    }

    #[test]
    fn test_bytes_conversion() {
        let tv = TypedValue::bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let json_val: JsonValue = tv.into();
        // Base64 encoded
        let expected =
            base64::engine::general_purpose::STANDARD.encode(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(json_val.0, json!(expected));
    }

    #[test]
    fn test_uuid_conversion() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(u);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let tv = TypedValue::datetime(dt);
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_string());
        let s = json_val.0.as_str().unwrap();
        assert!(s.starts_with("2024-06-15T10:30:00"));
    }

    #[test]
    fn test_date_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap();
        let tv = TypedValue::date(dt);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("2024-06-15"));
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue::time(dt);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("14:30:45"));
    }

    #[test]
    fn test_json_object_conversion() {
        let obj = serde_json::json!({"name": "test", "count": 42});
        let tv = TypedValue::json(obj);
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_object());
        assert_eq!(json_val.0["name"], json!("test"));
        assert_eq!(json_val.0["count"], json!(42));
    }

    #[test]
    fn test_array_int_conversion() {
        let tv = TypedValue::array(
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
            Type::Int32,
        );
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!([1, 2, 3]));
    }

    #[test]
    fn test_array_text_conversion() {
        let tv = TypedValue::array(
            vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
            Type::Text,
        );
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(["a", "b"]));
    }

    #[test]
    fn test_set_conversion() {
        let tv = TypedValue::set(
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(["a", "b"]));
    }

    #[test]
    fn test_enum_conversion() {
        let tv =
            TypedValue::enum_type("active", vec!["active".to_string(), "inactive".to_string()]);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("active"));
    }

    #[test]
    fn test_geometry_point_conversion() {
        let coords = serde_json::json!({"coordinates": [-73.97, 40.77]});
        let tv = TypedValue::geometry_geojson(coords, GeometryType::Point);
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_object());
        assert_eq!(json_val.0["type"], json!("Point"));
    }

    #[test]
    fn test_typed_values_to_json() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Alice")),
            ("age".to_string(), TypedValue::int32(30)),
            ("active".to_string(), TypedValue::bool(true)),
        ];

        let json = typed_values_to_json(fields);
        assert!(json.is_object());
        assert_eq!(json["name"], json!("Alice"));
        assert_eq!(json["age"], json!(30));
        assert_eq!(json["active"], json!(true));
    }

    #[test]
    fn test_typed_values_to_jsonl() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Bob")),
            ("age".to_string(), TypedValue::int32(25)),
        ];

        let jsonl = typed_values_to_jsonl(fields);
        assert!(jsonl.ends_with('\n'));
        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(jsonl.trim()).unwrap();
        assert_eq!(parsed["name"], json!("Bob"));
    }
}
