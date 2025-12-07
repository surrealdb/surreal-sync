//! Forward conversion: TypedValue → JSON value.
//!
//! This module provides conversion from sync-core's `TypedValue` to JSON values.

use base64::Engine;
use serde_json::json;
use sync_core::{GeometryType, TypedValue, UniversalType, UniversalValue};

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
        match (&tv.sync_type, &tv.value) {
            // Null
            (_, UniversalValue::Null) => JsonValue(serde_json::Value::Null),

            // Boolean
            (UniversalType::Bool, UniversalValue::Bool(b)) => JsonValue(json!(*b)),

            // Integer types - handle both Int32 and Int64 since generators may produce either
            (UniversalType::TinyInt { .. }, UniversalValue::Int32(i)) => JsonValue(json!(*i)),
            (UniversalType::TinyInt { .. }, UniversalValue::Int64(i)) => JsonValue(json!(*i)),
            (UniversalType::SmallInt, UniversalValue::Int32(i)) => JsonValue(json!(*i)),
            (UniversalType::SmallInt, UniversalValue::Int64(i)) => JsonValue(json!(*i)),
            (UniversalType::Int, UniversalValue::Int32(i)) => JsonValue(json!(*i)),
            (UniversalType::Int, UniversalValue::Int64(i)) => JsonValue(json!(*i)),
            (UniversalType::BigInt, UniversalValue::Int64(i)) => JsonValue(json!(*i)),
            (UniversalType::BigInt, UniversalValue::Int32(i)) => JsonValue(json!(*i)),

            // Floating point
            (UniversalType::Float, UniversalValue::Float64(f)) => JsonValue(json!(*f)),
            (UniversalType::Double, UniversalValue::Float64(f)) => JsonValue(json!(*f)),

            // Decimal - store as string to preserve precision
            (UniversalType::Decimal { .. }, UniversalValue::Decimal { value, .. }) => {
                JsonValue(json!(value))
            }

            // String types
            (UniversalType::Char { .. }, UniversalValue::String(s)) => JsonValue(json!(s)),
            (UniversalType::VarChar { .. }, UniversalValue::String(s)) => JsonValue(json!(s)),
            (UniversalType::Text, UniversalValue::String(s)) => JsonValue(json!(s)),

            // Binary types - base64 encode
            (UniversalType::Blob, UniversalValue::Bytes(b)) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                JsonValue(json!(encoded))
            }
            (UniversalType::Bytes, UniversalValue::Bytes(b)) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                JsonValue(json!(encoded))
            }

            // Date/time types - ISO 8601 format
            (UniversalType::Date, UniversalValue::DateTime(dt)) => {
                JsonValue(json!(dt.format("%Y-%m-%d").to_string()))
            }
            (UniversalType::Time, UniversalValue::DateTime(dt)) => {
                JsonValue(json!(dt.format("%H:%M:%S").to_string()))
            }
            (UniversalType::DateTime, UniversalValue::DateTime(dt)) => {
                JsonValue(json!(dt.to_rfc3339()))
            }
            (UniversalType::DateTimeNano, UniversalValue::DateTime(dt)) => {
                JsonValue(json!(dt.to_rfc3339()))
            }
            (UniversalType::TimestampTz, UniversalValue::DateTime(dt)) => {
                JsonValue(json!(dt.to_rfc3339()))
            }

            // UUID
            (UniversalType::Uuid, UniversalValue::Uuid(u)) => JsonValue(json!(u.to_string())),

            // JSON types - convert to native JSON
            (UniversalType::Json, UniversalValue::Object(map)) => {
                let json_obj = hashmap_to_json(map);
                JsonValue(json_obj)
            }
            (UniversalType::Jsonb, UniversalValue::Object(map)) => {
                let json_obj = hashmap_to_json(map);
                JsonValue(json_obj)
            }
            // JSON can also be a string
            (UniversalType::Json, UniversalValue::String(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => JsonValue(v),
                    Err(_) => JsonValue(json!(s)),
                }
            }
            (UniversalType::Jsonb, UniversalValue::String(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => JsonValue(v),
                    Err(_) => JsonValue(json!(s)),
                }
            }

            // Array types
            (UniversalType::Array { element_type }, UniversalValue::Array(arr)) => {
                let json_arr: Vec<serde_json::Value> = arr
                    .iter()
                    .map(|v| {
                        let tv = TypedValue {
                            sync_type: (**element_type).clone(),
                            value: v.clone(),
                        };
                        JsonValue::from(tv).into_inner()
                    })
                    .collect();
                JsonValue(json!(json_arr))
            }

            // Set - stored as array of strings
            (UniversalType::Set { .. }, UniversalValue::Array(arr)) => {
                let json_arr: Vec<serde_json::Value> = arr
                    .iter()
                    .map(|v| match v {
                        UniversalValue::String(s) => json!(s),
                        _ => serde_json::Value::Null,
                    })
                    .collect();
                JsonValue(json!(json_arr))
            }

            // Enum - stored as string
            (UniversalType::Enum { .. }, UniversalValue::String(s)) => JsonValue(json!(s)),

            // Geometry types - GeoJSON format
            (UniversalType::Geometry { geometry_type }, UniversalValue::Object(map)) => {
                let mut json_obj = hashmap_to_json(map);
                if let serde_json::Value::Object(ref mut obj) = json_obj {
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
                }
                JsonValue(json_obj)
            }

            // Fallback - panic instead of silently returning null
            (sync_type, value) => panic!(
                "Unsupported type/value combination for JSON conversion: sync_type={sync_type:?}, value={value:?}"
            ),
        }
    }
}

/// Convert a HashMap of UniversalValue to a JSON object.
fn hashmap_to_json(map: &std::collections::HashMap<String, UniversalValue>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (key, value) in map {
        obj.insert(key.clone(), generated_value_to_json(value));
    }
    serde_json::Value::Object(obj)
}

/// Convert a UniversalValue to a JSON value (without type context).
fn generated_value_to_json(value: &UniversalValue) -> serde_json::Value {
    match value {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => json!(*b),
        UniversalValue::Int32(i) => json!(*i),
        UniversalValue::Int64(i) => json!(*i),
        UniversalValue::Float64(f) => json!(*f),
        UniversalValue::String(s) => json!(s),
        UniversalValue::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            json!(encoded)
        }
        UniversalValue::Uuid(u) => json!(u.to_string()),
        UniversalValue::DateTime(dt) => json!(dt.to_rfc3339()),
        UniversalValue::Decimal { value, .. } => json!(value),
        UniversalValue::Array(arr) => {
            json!(arr.iter().map(generated_value_to_json).collect::<Vec<_>>())
        }
        UniversalValue::Object(map) => hashmap_to_json(map),
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
    use std::collections::HashMap;

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(UniversalType::Text);
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
        let tv = TypedValue {
            sync_type: UniversalType::TinyInt { width: 4 },
            value: UniversalValue::Int32(127),
        };
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(127));
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::smallint(32000);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(32000));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int(12345);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(12345));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::bigint(9876543210i64);
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(9876543210i64));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float(1.234);
        let json_val: JsonValue = tv.into();
        if let Some(f) = json_val.0.as_f64() {
            assert!((f - 1.234).abs() < 0.0001);
        } else {
            panic!("Expected number");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::double(1.23456);
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
        let tv = TypedValue {
            sync_type: UniversalType::Char { length: 10 },
            value: UniversalValue::String("test".to_string()),
        };
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
        let tv = TypedValue {
            sync_type: UniversalType::Date,
            value: UniversalValue::DateTime(dt),
        };
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("2024-06-15"));
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue {
            sync_type: UniversalType::Time,
            value: UniversalValue::DateTime(dt),
        };
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("14:30:45"));
    }

    #[test]
    fn test_json_object_conversion() {
        let mut map = HashMap::new();
        map.insert(
            "name".to_string(),
            UniversalValue::String("test".to_string()),
        );
        map.insert("count".to_string(), UniversalValue::Int32(42));

        let tv = TypedValue {
            sync_type: UniversalType::Json,
            value: UniversalValue::Object(map),
        };
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_object());
        assert_eq!(json_val.0["name"], json!("test"));
        assert_eq!(json_val.0["count"], json!(42));
    }

    #[test]
    fn test_json_string_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::Json,
            value: UniversalValue::String(r#"{"key": "value"}"#.to_string()),
        };
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_object());
        assert_eq!(json_val.0["key"], json!("value"));
    }

    #[test]
    fn test_array_int_conversion() {
        let tv = TypedValue::array(
            vec![
                UniversalValue::Int32(1),
                UniversalValue::Int32(2),
                UniversalValue::Int32(3),
            ],
            UniversalType::Int,
        );
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!([1, 2, 3]));
    }

    #[test]
    fn test_array_text_conversion() {
        let tv = TypedValue::array(
            vec![
                UniversalValue::String("a".to_string()),
                UniversalValue::String("b".to_string()),
            ],
            UniversalType::Text,
        );
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(["a", "b"]));
    }

    #[test]
    fn test_set_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            value: UniversalValue::Array(vec![
                UniversalValue::String("a".to_string()),
                UniversalValue::String("b".to_string()),
            ]),
        };
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!(["a", "b"]));
    }

    #[test]
    fn test_enum_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
            value: UniversalValue::String("active".to_string()),
        };
        let json_val: JsonValue = tv.into();
        assert_eq!(json_val.0, json!("active"));
    }

    #[test]
    fn test_geometry_point_conversion() {
        let mut coords = HashMap::new();
        coords.insert(
            "coordinates".to_string(),
            UniversalValue::Array(vec![
                UniversalValue::Float64(-73.97),
                UniversalValue::Float64(40.77),
            ]),
        );

        let tv = TypedValue {
            sync_type: UniversalType::Geometry {
                geometry_type: GeometryType::Point,
            },
            value: UniversalValue::Object(coords),
        };
        let json_val: JsonValue = tv.into();
        assert!(json_val.0.is_object());
        assert_eq!(json_val.0["type"], json!("Point"));
    }

    #[test]
    fn test_typed_values_to_json() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Alice")),
            ("age".to_string(), TypedValue::int(30)),
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
            ("age".to_string(), TypedValue::int(25)),
        ];

        let jsonl = typed_values_to_jsonl(fields);
        assert!(jsonl.ends_with('\n'));
        // Should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(jsonl.trim()).unwrap();
        assert_eq!(parsed["name"], json!("Bob"));
    }
}
