//! Reverse conversion: SurrealDB value → TypedValue.
//!
//! This module provides conversion from SurrealDB values to sync-core's `TypedValue`.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use surrealdb::sql::{Number, Object, Value};
use sync_core::{TypedValue, UniversalType, UniversalValue};

/// SurrealDB value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct SurrealValueWithSchema {
    /// The SurrealDB value.
    pub value: Value,
    /// The expected sync type for conversion.
    pub sync_type: UniversalType,
}

impl SurrealValueWithSchema {
    /// Create a new SurrealValueWithSchema.
    pub fn new(value: Value, sync_type: UniversalType) -> Self {
        Self { value, sync_type }
    }

    /// Convert to TypedValue.
    pub fn to_typed_value(&self) -> TypedValue {
        TypedValue::from(self.clone())
    }
}

impl From<SurrealValueWithSchema> for TypedValue {
    fn from(sv: SurrealValueWithSchema) -> Self {
        match (&sv.sync_type, &sv.value) {
            // Null/None
            (sync_type, Value::None) => TypedValue::null(sync_type.clone()),

            // Boolean
            (UniversalType::Bool, Value::Bool(b)) => TypedValue::bool(*b),

            // Integer types
            (UniversalType::Int8 { width }, Value::Number(Number::Int(i))) => TypedValue {
                sync_type: UniversalType::Int8 { width: *width },
                value: UniversalValue::Int32(*i as i32),
            },
            (UniversalType::Int16, Value::Number(Number::Int(i))) => TypedValue::int16(*i as i16),
            (UniversalType::Int32, Value::Number(Number::Int(i))) => TypedValue::int32(*i as i32),
            (UniversalType::Int64, Value::Number(Number::Int(i))) => TypedValue::int64(*i),

            // Floating point
            (UniversalType::Float32, Value::Number(Number::Float(f))) => {
                TypedValue::float32(*f as f32)
            }
            (UniversalType::Float64, Value::Number(Number::Float(f))) => TypedValue::float64(*f),

            // Decimal
            (UniversalType::Decimal { precision, scale }, Value::Number(Number::Decimal(d))) => {
                TypedValue::decimal(d.to_string(), *precision, *scale)
            }
            (UniversalType::Decimal { precision, scale }, Value::Strand(s)) => {
                TypedValue::decimal(s.as_str(), *precision, *scale)
            }

            // String types - use as_str() to get raw string, not to_string() which may add quotes
            (UniversalType::Char { length }, Value::Strand(s)) => TypedValue {
                sync_type: UniversalType::Char { length: *length },
                value: UniversalValue::Text(s.as_str().to_string()),
            },
            (UniversalType::VarChar { length }, Value::Strand(s)) => TypedValue {
                sync_type: UniversalType::VarChar { length: *length },
                value: UniversalValue::Text(s.as_str().to_string()),
            },
            (UniversalType::Text, Value::Strand(s)) => TypedValue::text(s.as_str().to_string()),

            // Binary types
            (UniversalType::Blob, Value::Bytes(b)) => TypedValue {
                sync_type: UniversalType::Blob,
                value: UniversalValue::Bytes(b.clone().into_inner()),
            },
            (UniversalType::Bytes, Value::Bytes(b)) => TypedValue::bytes(b.clone().into_inner()),

            // UUID
            (UniversalType::Uuid, Value::Uuid(u)) => TypedValue::uuid(u.0),
            // UUID might also be stored as string
            (UniversalType::Uuid, Value::Strand(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s.as_str()) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(UniversalType::Uuid)
                }
            }

            // Date/time types
            (UniversalType::LocalDateTime, Value::Datetime(dt)) => TypedValue::datetime(dt.0),
            (UniversalType::LocalDateTimeNano, Value::Datetime(dt)) => TypedValue {
                sync_type: UniversalType::LocalDateTimeNano,
                value: UniversalValue::LocalDateTime(dt.0),
            },
            (UniversalType::ZonedDateTime, Value::Datetime(dt)) => TypedValue {
                sync_type: UniversalType::ZonedDateTime,
                value: UniversalValue::LocalDateTime(dt.0),
            },

            // Date stored as string
            (UniversalType::Date, Value::Strand(s)) => {
                if let Ok(dt) = chrono::NaiveDate::parse_from_str(s.as_str(), "%Y-%m-%d") {
                    let datetime = dt.and_hms_opt(0, 0, 0).unwrap();
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: UniversalType::Date,
                        value: UniversalValue::LocalDateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(UniversalType::Date)
                }
            }

            // Time stored as string
            (UniversalType::Time, Value::Strand(s)) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s.as_str(), "%H:%M:%S") {
                    let datetime = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: UniversalType::Time,
                        value: UniversalValue::LocalDateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(UniversalType::Time)
                }
            }

            // JSON types
            (UniversalType::Json, Value::Object(obj)) => {
                let json_val = object_to_serde_json(obj);
                TypedValue {
                    sync_type: UniversalType::Json,
                    value: UniversalValue::Json(Box::new(json_val)),
                }
            }
            (UniversalType::Jsonb, Value::Object(obj)) => {
                let json_val = object_to_serde_json(obj);
                TypedValue {
                    sync_type: UniversalType::Jsonb,
                    value: UniversalValue::Json(Box::new(json_val)),
                }
            }

            // Array types
            (UniversalType::Array { element_type }, Value::Array(arr)) => {
                let values: Vec<UniversalValue> = arr
                    .iter()
                    .map(|v| {
                        let sv = SurrealValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(sv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array
            (UniversalType::Set { values: set_values }, Value::Array(arr)) => {
                let values: Vec<UniversalValue> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Value::Strand(s) = v {
                            Some(UniversalValue::Text(s.as_str().to_string()))
                        } else {
                            None
                        }
                    })
                    .collect();
                TypedValue {
                    sync_type: UniversalType::Set {
                        values: set_values.clone(),
                    },
                    value: UniversalValue::Array {
                        elements: values,
                        element_type: Box::new(UniversalType::Text),
                    },
                }
            }

            // Enum - stored as string
            (
                UniversalType::Enum {
                    values: enum_values,
                },
                Value::Strand(s),
            ) => TypedValue {
                sync_type: UniversalType::Enum {
                    values: enum_values.clone(),
                },
                value: UniversalValue::Text(s.as_str().to_string()),
            },

            // Geometry types - stored as SurrealDB Geometry
            (UniversalType::Geometry { geometry_type }, Value::Geometry(geo)) => {
                let json_val = geometry_to_serde_json(geo);
                TypedValue {
                    sync_type: UniversalType::Geometry {
                        geometry_type: geometry_type.clone(),
                    },
                    value: UniversalValue::Json(Box::new(json_val)),
                }
            }

            // Fallback
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a SurrealDB Object to a HashMap of UniversalValue.
#[allow(dead_code)]
fn object_to_hashmap(obj: &Object) -> HashMap<String, UniversalValue> {
    let mut map = HashMap::new();
    for (key, value) in obj.iter() {
        map.insert(key.clone(), surreal_value_to_generated(value));
    }
    map
}

/// Convert a SurrealDB Value to UniversalValue (without type context).
#[allow(dead_code)]
fn surreal_value_to_generated(value: &Value) -> UniversalValue {
    match value {
        Value::None => UniversalValue::Null,
        Value::Bool(b) => UniversalValue::Bool(*b),
        Value::Number(Number::Int(i)) => UniversalValue::Int64(*i),
        Value::Number(Number::Float(f)) => UniversalValue::Float64(*f),
        Value::Number(Number::Decimal(d)) => UniversalValue::Decimal {
            value: d.to_string(),
            precision: 38,
            scale: 10,
        },
        Value::Strand(s) => UniversalValue::Text(s.as_str().to_string()),
        Value::Bytes(b) => UniversalValue::Bytes(b.clone().into_inner()),
        Value::Uuid(u) => UniversalValue::Uuid(u.0),
        Value::Datetime(dt) => UniversalValue::LocalDateTime(dt.0),
        Value::Array(arr) => {
            let elements: Vec<UniversalValue> =
                arr.iter().map(surreal_value_to_generated).collect();
            UniversalValue::Array {
                elements,
                element_type: Box::new(UniversalType::Json),
            }
        }
        Value::Object(obj) => {
            let json_val = object_to_serde_json(obj);
            UniversalValue::Json(Box::new(json_val))
        }
        // Thing (record ID) - convert to string
        Value::Thing(thing) => UniversalValue::Text(thing.to_string()),
        // Duration - convert to string
        Value::Duration(dur) => UniversalValue::Text(dur.to_string()),
        // Geometry - convert to object
        Value::Geometry(geo) => {
            let json_val = geometry_to_serde_json(geo);
            UniversalValue::Json(Box::new(json_val))
        }
        // Other types - convert to null
        _ => UniversalValue::Null,
    }
}

/// Convert a SurrealDB Geometry to a GeoJSON-like serde_json::Value.
fn geometry_to_serde_json(geo: &surrealdb::sql::Geometry) -> serde_json::Value {
    match geo {
        surrealdb::sql::Geometry::Point(p) => {
            serde_json::json!({
                "type": "Point",
                "coordinates": [p.x(), p.y()]
            })
        }
        surrealdb::sql::Geometry::Line(line) => {
            let coords: Vec<Vec<f64>> = line.points().map(|p| vec![p.x(), p.y()]).collect();
            serde_json::json!({
                "type": "LineString",
                "coordinates": coords
            })
        }
        surrealdb::sql::Geometry::Polygon(poly) => {
            let exterior: Vec<Vec<f64>> = poly
                .exterior()
                .points()
                .map(|p| vec![p.x(), p.y()])
                .collect();
            serde_json::json!({
                "type": "Polygon",
                "coordinates": [exterior]
            })
        }
        surrealdb::sql::Geometry::MultiPoint(mp) => {
            let coords: Vec<Vec<f64>> = mp.iter().map(|p| vec![p.x(), p.y()]).collect();
            serde_json::json!({
                "type": "MultiPoint",
                "coordinates": coords
            })
        }
        _ => {
            // For other geometry types, store type as string
            serde_json::json!({
                "type": "Geometry"
            })
        }
    }
}

/// Convert a SurrealDB Object to serde_json::Value.
fn object_to_serde_json(obj: &Object) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = obj
        .iter()
        .map(|(k, v)| (k.clone(), surreal_value_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

/// Convert a SurrealDB Value to serde_json::Value.
fn surreal_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::None | Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Number(n) => match n {
            Number::Int(i) => serde_json::json!(i),
            Number::Float(f) => serde_json::json!(f),
            Number::Decimal(d) => serde_json::json!(d.to_string()),
            _ => serde_json::Value::Null,
        },
        Value::Strand(s) => serde_json::Value::String(s.as_str().to_string()),
        Value::Datetime(dt) => serde_json::Value::String(dt.0.to_rfc3339()),
        Value::Uuid(u) => serde_json::Value::String(u.0.to_string()),
        Value::Array(arr) => {
            let values: Vec<serde_json::Value> = arr.iter().map(surreal_value_to_json).collect();
            serde_json::Value::Array(values)
        }
        Value::Object(obj) => object_to_serde_json(obj),
        Value::Geometry(geo) => geometry_to_serde_json(geo),
        _ => serde_json::Value::Null,
    }
}

/// Extract a typed value from a SurrealDB Object field.
pub fn extract_field(obj: &Object, field: &str, sync_type: &UniversalType) -> TypedValue {
    match obj.get(field) {
        Some(value) => {
            SurrealValueWithSchema::new(value.clone(), sync_type.clone()).to_typed_value()
        }
        None => TypedValue::null(sync_type.clone()),
    }
}

/// Convert a complete SurrealDB Object to a map of TypedValues using schema.
pub fn object_to_typed_values(
    obj: &Object,
    schema: &[(String, UniversalType)],
) -> HashMap<String, TypedValue> {
    let mut result = HashMap::new();
    for (field_name, sync_type) in schema {
        let tv = extract_field(obj, field_name, sync_type);
        result.insert(field_name.clone(), tv);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};
    use std::collections::BTreeMap;
    use surrealdb::sql::{Array, Datetime, Strand};

    #[test]
    fn test_null_conversion() {
        let sv = SurrealValueWithSchema::new(Value::None, UniversalType::Text);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let sv = SurrealValueWithSchema::new(Value::Bool(true), UniversalType::Bool);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_int_conversion() {
        let sv = SurrealValueWithSchema::new(Value::Number(Number::Int(42)), UniversalType::Int32);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let sv = SurrealValueWithSchema::new(
            Value::Number(Number::Int(9876543210)),
            UniversalType::Int64,
        );
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let sv = SurrealValueWithSchema::new(
            Value::Number(Number::Float(1.23456)),
            UniversalType::Float64,
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::Float64(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let dec = rust_decimal::Decimal::from_str_exact("123.456").unwrap();
        let sv = SurrealValueWithSchema::new(
            Value::Number(Number::Decimal(dec)),
            UniversalType::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::Decimal {
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
        let sv = SurrealValueWithSchema::new(
            Value::Strand(Strand::from("hello world")),
            UniversalType::Text,
        );
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let sv = SurrealValueWithSchema::new(
            Value::Strand(Strand::from("test")),
            UniversalType::VarChar { length: 100 },
        );
        let tv = TypedValue::from(sv);
        assert!(matches!(
            tv.sync_type,
            UniversalType::VarChar { length: 100 }
        ));
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "test"));
    }

    #[test]
    fn test_bytes_conversion() {
        let bytes = surrealdb::sql::Bytes::from(vec![0x01, 0x02, 0x03]);
        let sv = SurrealValueWithSchema::new(Value::Bytes(bytes), UniversalType::Bytes);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let sv = SurrealValueWithSchema::new(
            Value::Uuid(surrealdb::sql::Uuid::from(uuid)),
            UniversalType::Uuid,
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u, uuid);
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_uuid_from_string() {
        let sv = SurrealValueWithSchema::new(
            Value::Strand(Strand::from("550e8400-e29b-41d4-a716-446655440000")),
            UniversalType::Uuid,
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let sv = SurrealValueWithSchema::new(
            Value::Datetime(Datetime::from(dt)),
            UniversalType::LocalDateTime,
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::LocalDateTime(result_dt) = tv.value {
            assert_eq!(result_dt.year(), 2024);
            assert_eq!(result_dt.month(), 6);
            assert_eq!(result_dt.day(), 15);
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_date_from_string() {
        let sv = SurrealValueWithSchema::new(
            Value::Strand(Strand::from("2024-06-15")),
            UniversalType::Date,
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let sv = SurrealValueWithSchema::new(
            Value::Strand(Strand::from("14:30:45")),
            UniversalType::Time,
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_object_to_json() {
        let mut obj_map = BTreeMap::new();
        obj_map.insert("name".to_string(), Value::Strand(Strand::from("test")));
        obj_map.insert("count".to_string(), Value::Number(Number::Int(42)));
        let obj = Object::from(obj_map);

        let sv = SurrealValueWithSchema::new(Value::Object(obj), UniversalType::Json);
        let tv = TypedValue::from(sv);
        if let UniversalValue::Json(json_val) = tv.value {
            if let serde_json::Value::Object(map) = json_val.as_ref() {
                assert!(
                    matches!(map.get("name"), Some(serde_json::Value::String(s)) if s == "test")
                );
                assert!(
                    matches!(map.get("count"), Some(serde_json::Value::Number(n)) if n.as_i64() == Some(42))
                );
            } else {
                panic!("Expected JSON Object");
            }
        } else {
            panic!("Expected Json value");
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let arr = Array::from(vec![
            Value::Number(Number::Int(1)),
            Value::Number(Number::Int(2)),
            Value::Number(Number::Int(3)),
        ]);
        let sv = SurrealValueWithSchema::new(
            Value::Array(arr),
            UniversalType::Array {
                element_type: Box::new(UniversalType::Int32),
            },
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            // Values are converted as Int32
            assert!(matches!(elements[0], UniversalValue::Int32(1)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let arr = Array::from(vec![
            Value::Strand(Strand::from("a")),
            Value::Strand(Strand::from("b")),
        ]);
        let sv = SurrealValueWithSchema::new(
            Value::Array(arr),
            UniversalType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(sv);
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let sv = SurrealValueWithSchema::new(
            Value::Strand(Strand::from("active")),
            UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "active"));
    }

    #[test]
    fn test_extract_field() {
        let mut obj_map = BTreeMap::new();
        obj_map.insert("name".to_string(), Value::Strand(Strand::from("Alice")));
        obj_map.insert("age".to_string(), Value::Number(Number::Int(30)));
        let obj = Object::from(obj_map);

        let name = extract_field(&obj, "name", &UniversalType::Text);
        assert!(matches!(name.value, UniversalValue::Text(ref s) if s == "Alice"));

        let age = extract_field(&obj, "age", &UniversalType::Int32);
        assert!(matches!(age.value, UniversalValue::Int32(30)));

        let missing = extract_field(&obj, "missing", &UniversalType::Text);
        assert!(matches!(missing.value, UniversalValue::Null));
    }

    #[test]
    fn test_object_to_typed_values() {
        let mut obj_map = BTreeMap::new();
        obj_map.insert("name".to_string(), Value::Strand(Strand::from("Bob")));
        obj_map.insert("active".to_string(), Value::Bool(true));
        obj_map.insert("score".to_string(), Value::Number(Number::Float(95.5)));
        let obj = Object::from(obj_map);

        let schema = vec![
            ("name".to_string(), UniversalType::Text),
            ("active".to_string(), UniversalType::Bool),
            ("score".to_string(), UniversalType::Float64),
        ];

        let values = object_to_typed_values(&obj, &schema);
        assert!(matches!(
            values.get("name").unwrap().value,
            UniversalValue::Text(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            UniversalValue::Bool(true)
        ));
        if let UniversalValue::Float64(f) = values.get("score").unwrap().value {
            assert!((f - 95.5).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }
}
