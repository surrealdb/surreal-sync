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

            // Integer types - strict 1:1 matching
            (UniversalType::TinyInt { .. }, UniversalValue::TinyInt { value, .. }) => {
                JsonValue(json!(*value))
            }
            (UniversalType::SmallInt, UniversalValue::SmallInt(i)) => JsonValue(json!(*i)),
            (UniversalType::Int, UniversalValue::Int(i)) => JsonValue(json!(*i)),
            (UniversalType::BigInt, UniversalValue::BigInt(i)) => JsonValue(json!(*i)),

            // Floating point - strict 1:1 matching
            (UniversalType::Float, UniversalValue::Float(f)) => JsonValue(json!(*f)),
            (UniversalType::Double, UniversalValue::Double(f)) => JsonValue(json!(*f)),

            // Decimal - store as string to preserve precision
            (UniversalType::Decimal { .. }, UniversalValue::Decimal { value, .. }) => {
                JsonValue(json!(value))
            }

            // String types - strict 1:1 matching
            (UniversalType::Char { .. }, UniversalValue::Char { value, .. }) => {
                JsonValue(json!(value))
            }
            (UniversalType::VarChar { .. }, UniversalValue::VarChar { value, .. }) => {
                JsonValue(json!(value))
            }
            (UniversalType::Text, UniversalValue::Text(s)) => JsonValue(json!(s)),

            // Binary types - base64 encode
            (UniversalType::Blob, UniversalValue::Blob(b)) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                JsonValue(json!(encoded))
            }
            (UniversalType::Bytes, UniversalValue::Bytes(b)) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                JsonValue(json!(encoded))
            }

            // Date/time types - strict 1:1 matching with ISO 8601 format
            (UniversalType::Date, UniversalValue::Date(dt)) => {
                JsonValue(json!(dt.format("%Y-%m-%d").to_string()))
            }
            (UniversalType::Time, UniversalValue::Time(dt)) => {
                JsonValue(json!(dt.format("%H:%M:%S").to_string()))
            }
            (UniversalType::DateTime, UniversalValue::DateTime(dt)) => {
                JsonValue(json!(dt.to_rfc3339()))
            }
            (UniversalType::DateTimeNano, UniversalValue::DateTimeNano(dt)) => {
                JsonValue(json!(dt.to_rfc3339()))
            }
            (UniversalType::TimestampTz, UniversalValue::TimestampTz(dt)) => {
                JsonValue(json!(dt.to_rfc3339()))
            }

            // UUID
            (UniversalType::Uuid, UniversalValue::Uuid(u)) => JsonValue(json!(u.to_string())),

            // JSON types - already serde_json::Value
            (UniversalType::Json, UniversalValue::Json(payload)) => JsonValue((**payload).clone()),
            (UniversalType::Jsonb, UniversalValue::Jsonb(payload)) => JsonValue((**payload).clone()),

            // Array types
            (UniversalType::Array { element_type }, UniversalValue::Array { elements, .. }) => {
                let json_arr: Vec<serde_json::Value> = elements
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
            (UniversalType::Set { .. }, UniversalValue::Set { elements, .. }) => {
                let json_arr: Vec<serde_json::Value> =
                    elements.iter().map(|s| json!(s)).collect();
                JsonValue(json!(json_arr))
            }

            // Enum - stored as string
            (UniversalType::Enum { .. }, UniversalValue::Enum { value, .. }) => {
                JsonValue(json!(value))
            }

            // Geometry types - GeoJSON format
            (
                UniversalType::Geometry { geometry_type },
                UniversalValue::Geometry { data, .. },
            ) => {
                use sync_core::values::GeometryData;
                let json_obj = match data {
                    GeometryData::GeoJson(value) => value.clone(),
                    GeometryData::Wkb(bytes) => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                        json!({"wkb": encoded})
                    }
                };
                if let serde_json::Value::Object(mut obj) = json_obj {
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
                    JsonValue(json_obj)
                }
            }

            // Fallback - panic instead of silently returning null
            (sync_type, value) => panic!(
                "Unsupported type/value combination for JSON conversion: sync_type={sync_type:?}, value={value:?}"
            ),
        }
    }
}

/// Convert a UniversalValue to a JSON value (without type context).
#[allow(dead_code)]
fn generated_value_to_json(value: &UniversalValue) -> serde_json::Value {
    match value {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => json!(*b),
        UniversalValue::TinyInt { value, .. } => json!(*value),
        UniversalValue::SmallInt(i) => json!(*i),
        UniversalValue::Int(i) => json!(*i),
        UniversalValue::BigInt(i) => json!(*i),
        UniversalValue::Float(f) => json!(*f),
        UniversalValue::Double(f) => json!(*f),
        UniversalValue::Char { value, .. } => json!(value),
        UniversalValue::VarChar { value, .. } => json!(value),
        UniversalValue::Text(s) => json!(s),
        UniversalValue::Blob(b) | UniversalValue::Bytes(b) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            json!(encoded)
        }
        UniversalValue::Uuid(u) => json!(u.to_string()),
        UniversalValue::Date(dt) => json!(dt.format("%Y-%m-%d").to_string()),
        UniversalValue::Time(dt) => json!(dt.format("%H:%M:%S").to_string()),
        UniversalValue::DateTime(dt)
        | UniversalValue::DateTimeNano(dt)
        | UniversalValue::TimestampTz(dt) => json!(dt.to_rfc3339()),
        UniversalValue::Decimal { value, .. } => json!(value),
        UniversalValue::Array { elements, .. } => {
            json!(elements
                .iter()
                .map(generated_value_to_json)
                .collect::<Vec<_>>())
        }
        UniversalValue::Set { elements, .. } => {
            json!(elements)
        }
        UniversalValue::Enum { value, .. } => json!(value),
        UniversalValue::Json(payload) | UniversalValue::Jsonb(payload) => (**payload).clone(),
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            match data {
                GeometryData::GeoJson(value) => value.clone(),
                GeometryData::Wkb(b) => {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                    json!({"wkb": encoded})
                }
            }
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
        let tv = TypedValue::tinyint(127, 4);
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
            assert!((f - 1.234f64).abs() < 0.001);
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
            vec![
                UniversalValue::Int(1),
                UniversalValue::Int(2),
                UniversalValue::Int(3),
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
                UniversalValue::Text("a".to_string()),
                UniversalValue::Text("b".to_string()),
            ],
            UniversalType::Text,
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
