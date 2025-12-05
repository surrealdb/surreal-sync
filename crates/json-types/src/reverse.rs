//! Reverse conversion: JSON value → TypedValue.
//!
//! This module provides conversion from JSON values to sync-core's `TypedValue`.

use base64::Engine;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use sync_core::{GeneratedValue, SyncDataType, TypedValue};

/// JSON value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct JsonValueWithSchema {
    /// The JSON value.
    pub value: serde_json::Value,
    /// The expected sync type for conversion.
    pub sync_type: SyncDataType,
}

impl JsonValueWithSchema {
    /// Create a new JsonValueWithSchema.
    pub fn new(value: serde_json::Value, sync_type: SyncDataType) -> Self {
        Self { value, sync_type }
    }

    /// Convert to TypedValue.
    pub fn to_typed_value(&self) -> TypedValue {
        TypedValue::from(self.clone())
    }
}

impl From<JsonValueWithSchema> for TypedValue {
    fn from(jv: JsonValueWithSchema) -> Self {
        match (&jv.sync_type, &jv.value) {
            // Null
            (sync_type, serde_json::Value::Null) => TypedValue::null(sync_type.clone()),

            // Boolean
            (SyncDataType::Bool, serde_json::Value::Bool(b)) => TypedValue::bool(*b),

            // Integer types
            (SyncDataType::TinyInt { width }, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue {
                        sync_type: SyncDataType::TinyInt { width: *width },
                        value: GeneratedValue::Int32(i as i32),
                    }
                } else {
                    TypedValue::null(SyncDataType::TinyInt { width: *width })
                }
            }
            (SyncDataType::SmallInt, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::smallint(i as i32)
                } else {
                    TypedValue::null(SyncDataType::SmallInt)
                }
            }
            (SyncDataType::Int, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::int(i as i32)
                } else {
                    TypedValue::null(SyncDataType::Int)
                }
            }
            (SyncDataType::BigInt, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::bigint(i)
                } else {
                    TypedValue::null(SyncDataType::BigInt)
                }
            }

            // Floating point
            (SyncDataType::Float, serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    TypedValue::float(f)
                } else {
                    TypedValue::null(SyncDataType::Float)
                }
            }
            (SyncDataType::Double, serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    TypedValue::double(f)
                } else {
                    TypedValue::null(SyncDataType::Double)
                }
            }

            // Decimal - stored as string in JSON
            (SyncDataType::Decimal { precision, scale }, serde_json::Value::String(s)) => {
                TypedValue::decimal(s, *precision, *scale)
            }
            (SyncDataType::Decimal { precision, scale }, serde_json::Value::Number(n)) => {
                TypedValue::decimal(n.to_string(), *precision, *scale)
            }

            // String types
            (SyncDataType::Char { length }, serde_json::Value::String(s)) => TypedValue {
                sync_type: SyncDataType::Char { length: *length },
                value: GeneratedValue::String(s.clone()),
            },
            (SyncDataType::VarChar { length }, serde_json::Value::String(s)) => TypedValue {
                sync_type: SyncDataType::VarChar { length: *length },
                value: GeneratedValue::String(s.clone()),
            },
            (SyncDataType::Text, serde_json::Value::String(s)) => TypedValue::text(s),

            // Binary types - base64 encoded in JSON
            (SyncDataType::Blob, serde_json::Value::String(s)) => {
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => TypedValue {
                        sync_type: SyncDataType::Blob,
                        value: GeneratedValue::Bytes(bytes),
                    },
                    Err(_) => TypedValue::null(SyncDataType::Blob),
                }
            }
            (SyncDataType::Bytes, serde_json::Value::String(s)) => {
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => TypedValue::bytes(bytes),
                    Err(_) => TypedValue::null(SyncDataType::Bytes),
                }
            }

            // UUID
            (SyncDataType::Uuid, serde_json::Value::String(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(SyncDataType::Uuid)
                }
            }

            // Date/time types - ISO 8601 format
            (SyncDataType::DateTime, serde_json::Value::String(s)) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    TypedValue::datetime(dt.with_timezone(&Utc))
                } else {
                    TypedValue::null(SyncDataType::DateTime)
                }
            }
            (SyncDataType::DateTimeNano, serde_json::Value::String(s)) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    TypedValue {
                        sync_type: SyncDataType::DateTimeNano,
                        value: GeneratedValue::DateTime(dt.with_timezone(&Utc)),
                    }
                } else {
                    TypedValue::null(SyncDataType::DateTimeNano)
                }
            }
            (SyncDataType::TimestampTz, serde_json::Value::String(s)) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    TypedValue {
                        sync_type: SyncDataType::TimestampTz,
                        value: GeneratedValue::DateTime(dt.with_timezone(&Utc)),
                    }
                } else {
                    TypedValue::null(SyncDataType::TimestampTz)
                }
            }

            // Date stored as string
            (SyncDataType::Date, serde_json::Value::String(s)) => {
                if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let datetime = dt.and_hms_opt(0, 0, 0).unwrap();
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: SyncDataType::Date,
                        value: GeneratedValue::DateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(SyncDataType::Date)
                }
            }

            // Time stored as string
            (SyncDataType::Time, serde_json::Value::String(s)) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    let datetime = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: SyncDataType::Time,
                        value: GeneratedValue::DateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(SyncDataType::Time)
                }
            }

            // JSON types
            (SyncDataType::Json, serde_json::Value::Object(obj)) => {
                let map = json_object_to_hashmap(obj);
                TypedValue {
                    sync_type: SyncDataType::Json,
                    value: GeneratedValue::Object(map),
                }
            }
            (SyncDataType::Jsonb, serde_json::Value::Object(obj)) => {
                let map = json_object_to_hashmap(obj);
                TypedValue {
                    sync_type: SyncDataType::Jsonb,
                    value: GeneratedValue::Object(map),
                }
            }

            // Array types
            (SyncDataType::Array { element_type }, serde_json::Value::Array(arr)) => {
                let values: Vec<GeneratedValue> = arr
                    .iter()
                    .map(|v| {
                        let jv = JsonValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(jv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array
            (SyncDataType::Set { values: set_values }, serde_json::Value::Array(arr)) => {
                let values: Vec<GeneratedValue> = arr
                    .iter()
                    .filter_map(|v| {
                        if let serde_json::Value::String(s) = v {
                            Some(GeneratedValue::String(s.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                TypedValue {
                    sync_type: SyncDataType::Set {
                        values: set_values.clone(),
                    },
                    value: GeneratedValue::Array(values),
                }
            }

            // Enum - stored as string
            (
                SyncDataType::Enum {
                    values: enum_values,
                },
                serde_json::Value::String(s),
            ) => TypedValue {
                sync_type: SyncDataType::Enum {
                    values: enum_values.clone(),
                },
                value: GeneratedValue::String(s.clone()),
            },

            // Geometry types - GeoJSON format
            (SyncDataType::Geometry { geometry_type }, serde_json::Value::Object(obj)) => {
                let map = json_object_to_hashmap(obj);
                TypedValue {
                    sync_type: SyncDataType::Geometry {
                        geometry_type: geometry_type.clone(),
                    },
                    value: GeneratedValue::Object(map),
                }
            }

            // Fallback
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a JSON object to a HashMap of GeneratedValue.
fn json_object_to_hashmap(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, GeneratedValue> {
    let mut map = HashMap::new();
    for (key, value) in obj {
        map.insert(key.clone(), json_value_to_generated(value));
    }
    map
}

/// Convert a JSON value to GeneratedValue (without type context).
fn json_value_to_generated(value: &serde_json::Value) -> GeneratedValue {
    match value {
        serde_json::Value::Null => GeneratedValue::Null,
        serde_json::Value::Bool(b) => GeneratedValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                GeneratedValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                GeneratedValue::Float64(f)
            } else {
                GeneratedValue::Null
            }
        }
        serde_json::Value::String(s) => GeneratedValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            GeneratedValue::Array(arr.iter().map(json_value_to_generated).collect())
        }
        serde_json::Value::Object(obj) => GeneratedValue::Object(json_object_to_hashmap(obj)),
    }
}

/// Extract a typed value from a JSON object field.
pub fn extract_field(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    sync_type: &SyncDataType,
) -> TypedValue {
    match obj.get(field) {
        Some(value) => JsonValueWithSchema::new(value.clone(), sync_type.clone()).to_typed_value(),
        None => TypedValue::null(sync_type.clone()),
    }
}

/// Convert a complete JSON object to a map of TypedValues using schema.
pub fn json_object_to_typed_values(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &[(String, SyncDataType)],
) -> HashMap<String, TypedValue> {
    let mut result = HashMap::new();
    for (field_name, sync_type) in schema {
        let tv = extract_field(obj, field_name, sync_type);
        result.insert(field_name.clone(), tv);
    }
    result
}

/// Parse a JSONL line and convert to typed values.
pub fn parse_jsonl_line(
    line: &str,
    schema: &[(String, SyncDataType)],
) -> Result<HashMap<String, TypedValue>, serde_json::Error> {
    let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line)?;
    Ok(json_object_to_typed_values(&obj, schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};
    use serde_json::json;
    use sync_core::GeometryType;

    #[test]
    fn test_null_conversion() {
        let jv = JsonValueWithSchema::new(serde_json::Value::Null, SyncDataType::Text);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let jv = JsonValueWithSchema::new(json!(true), SyncDataType::Bool);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::Bool(true)));
    }

    #[test]
    fn test_int_conversion() {
        let jv = JsonValueWithSchema::new(json!(42), SyncDataType::Int);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let jv = JsonValueWithSchema::new(json!(9876543210i64), SyncDataType::BigInt);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let jv = JsonValueWithSchema::new(json!(1.23456), SyncDataType::Double);
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Float64(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_from_string() {
        let jv = JsonValueWithSchema::new(
            json!("123.456"),
            SyncDataType::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Decimal {
            value,
            precision,
            scale,
        } = tv.value
        {
            assert_eq!(value, "123.456");
            assert_eq!(precision, 10);
            assert_eq!(scale, 3);
        } else {
            panic!("Expected Decimal");
        }
    }

    #[test]
    fn test_string_conversion() {
        let jv = JsonValueWithSchema::new(json!("hello world"), SyncDataType::Text);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::String(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let jv = JsonValueWithSchema::new(json!("test"), SyncDataType::VarChar { length: 100 });
        let tv = TypedValue::from(jv);
        assert!(matches!(
            tv.sync_type,
            SyncDataType::VarChar { length: 100 }
        ));
        assert!(matches!(tv.value, GeneratedValue::String(ref s) if s == "test"));
    }

    #[test]
    fn test_bytes_from_base64() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(vec![0x01, 0x02, 0x03]);
        let jv = JsonValueWithSchema::new(json!(encoded), SyncDataType::Bytes);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_conversion() {
        let jv = JsonValueWithSchema::new(
            json!("550e8400-e29b-41d4-a716-446655440000"),
            SyncDataType::Uuid,
        );
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let jv = JsonValueWithSchema::new(json!(dt.to_rfc3339()), SyncDataType::DateTime);
        let tv = TypedValue::from(jv);
        if let GeneratedValue::DateTime(result_dt) = tv.value {
            assert_eq!(result_dt.year(), 2024);
            assert_eq!(result_dt.month(), 6);
            assert_eq!(result_dt.day(), 15);
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_date_from_string() {
        let jv = JsonValueWithSchema::new(json!("2024-06-15"), SyncDataType::Date);
        let tv = TypedValue::from(jv);
        if let GeneratedValue::DateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let jv = JsonValueWithSchema::new(json!("14:30:45"), SyncDataType::Time);
        let tv = TypedValue::from(jv);
        if let GeneratedValue::DateTime(dt) = tv.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let jv = JsonValueWithSchema::new(json!({"name": "test", "count": 42}), SyncDataType::Json);
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Object(map) = tv.value {
            assert!(matches!(map.get("name"), Some(GeneratedValue::String(s)) if s == "test"));
            assert!(matches!(map.get("count"), Some(GeneratedValue::Int64(42))));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let jv = JsonValueWithSchema::new(
            json!([1, 2, 3]),
            SyncDataType::Array {
                element_type: Box::new(SyncDataType::Int),
            },
        );
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Array(values) = tv.value {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], GeneratedValue::Int32(1)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let jv = JsonValueWithSchema::new(
            json!(["a", "b"]),
            SyncDataType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Array(values) = tv.value {
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let jv = JsonValueWithSchema::new(
            json!("active"),
            SyncDataType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, GeneratedValue::String(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_conversion() {
        let jv = JsonValueWithSchema::new(
            json!({"type": "Point", "coordinates": [-73.97, 40.77]}),
            SyncDataType::Geometry {
                geometry_type: GeometryType::Point,
            },
        );
        let tv = TypedValue::from(jv);
        if let GeneratedValue::Object(map) = tv.value {
            assert!(matches!(map.get("type"), Some(GeneratedValue::String(s)) if s == "Point"));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_extract_field() {
        let obj = json!({"name": "Alice", "age": 30})
            .as_object()
            .unwrap()
            .clone();

        let name = extract_field(&obj, "name", &SyncDataType::Text);
        assert!(matches!(name.value, GeneratedValue::String(ref s) if s == "Alice"));

        let age = extract_field(&obj, "age", &SyncDataType::Int);
        assert!(matches!(age.value, GeneratedValue::Int32(30)));

        let missing = extract_field(&obj, "missing", &SyncDataType::Text);
        assert!(matches!(missing.value, GeneratedValue::Null));
    }

    #[test]
    fn test_parse_jsonl_line() {
        let line = r#"{"name": "Bob", "active": true, "score": 95.5}"#;
        let schema = vec![
            ("name".to_string(), SyncDataType::Text),
            ("active".to_string(), SyncDataType::Bool),
            ("score".to_string(), SyncDataType::Double),
        ];

        let values = parse_jsonl_line(line, &schema).unwrap();
        assert!(matches!(
            values.get("name").unwrap().value,
            GeneratedValue::String(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            GeneratedValue::Bool(true)
        ));
    }
}
