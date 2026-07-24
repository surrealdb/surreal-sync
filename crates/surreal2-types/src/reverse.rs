//! Reverse conversion: SurrealDB value → TypedValue.
//!
//! This module provides conversion from SurrealDB values to sync-core's `TypedValue`.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use surrealdb::sql::{Number, Object, Value as SqlValue};
use sync_core::{Type, TypedValue, Value};

/// SurrealDB value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct SurrealValueWithSchema {
    /// The SurrealDB value.
    pub value: SqlValue,
    /// The expected sync type for conversion.
    pub sync_type: Type,
}

impl SurrealValueWithSchema {
    /// Create a new SurrealValueWithSchema.
    pub fn new(value: SqlValue, sync_type: Type) -> Self {
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
            (sync_type, SqlValue::None) => TypedValue::null(sync_type.clone()),

            // Boolean
            (Type::Bool, SqlValue::Bool(b)) => TypedValue::bool(*b),

            // Integer types
            (Type::Int8 { width }, SqlValue::Number(Number::Int(i))) => TypedValue {
                sync_type: Type::Int8 { width: *width },
                value: Value::Int32(*i as i32),
            },
            (Type::Int16, SqlValue::Number(Number::Int(i))) => TypedValue::int16(*i as i16),
            (Type::Int32, SqlValue::Number(Number::Int(i))) => TypedValue::int32(*i as i32),
            (Type::Int64, SqlValue::Number(Number::Int(i))) => TypedValue::int64(*i),

            // Floating point
            (Type::Float32, SqlValue::Number(Number::Float(f))) => TypedValue::float32(*f as f32),
            (Type::Float64, SqlValue::Number(Number::Float(f))) => TypedValue::float64(*f),

            // Decimal
            (Type::Decimal { precision, scale }, SqlValue::Number(Number::Decimal(d))) => {
                TypedValue::decimal(d.to_string(), *precision, *scale)
            }
            (Type::Decimal { precision, scale }, SqlValue::Strand(s)) => {
                TypedValue::decimal(s.as_str(), *precision, *scale)
            }

            // String types - use as_str() to get raw string, not to_string() which may add quotes
            (Type::Char { length }, SqlValue::Strand(s)) => TypedValue {
                sync_type: Type::Char { length: *length },
                value: Value::Text(s.as_str().to_string()),
            },
            (Type::VarChar { length }, SqlValue::Strand(s)) => TypedValue {
                sync_type: Type::VarChar { length: *length },
                value: Value::Text(s.as_str().to_string()),
            },
            (Type::Text, SqlValue::Strand(s)) => TypedValue::text(s.as_str().to_string()),

            // Binary types
            (Type::Blob, SqlValue::Bytes(b)) => TypedValue {
                sync_type: Type::Blob,
                value: Value::Bytes(b.clone().into_inner()),
            },
            (Type::Bytes, SqlValue::Bytes(b)) => TypedValue::bytes(b.clone().into_inner()),

            // UUID
            (Type::Uuid, SqlValue::Uuid(u)) => TypedValue::uuid(u.0),
            // UUID might also be stored as string
            (Type::Uuid, SqlValue::Strand(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s.as_str()) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(Type::Uuid)
                }
            }

            // Date/time types
            (Type::LocalDateTime, SqlValue::Datetime(dt)) => TypedValue::datetime(dt.0),
            (Type::LocalDateTimeNano, SqlValue::Datetime(dt)) => TypedValue {
                sync_type: Type::LocalDateTimeNano,
                value: Value::LocalDateTime(dt.0),
            },
            (Type::ZonedDateTime, SqlValue::Datetime(dt)) => TypedValue {
                sync_type: Type::ZonedDateTime,
                value: Value::LocalDateTime(dt.0),
            },

            // Date stored as string
            (Type::Date, SqlValue::Strand(s)) => {
                if let Ok(dt) = chrono::NaiveDate::parse_from_str(s.as_str(), "%Y-%m-%d") {
                    let datetime = dt.and_hms_opt(0, 0, 0).unwrap();
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: Type::Date,
                        value: Value::LocalDateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(Type::Date)
                }
            }

            // Time stored as string
            (Type::Time, SqlValue::Strand(s)) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s.as_str(), "%H:%M:%S") {
                    let datetime = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: Type::Time,
                        value: Value::LocalDateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(Type::Time)
                }
            }

            // JSON types
            (Type::Json, SqlValue::Object(obj)) => {
                let json_val = object_to_serde_json(obj);
                TypedValue {
                    sync_type: Type::Json,
                    value: Value::Json(Box::new(json_val)),
                }
            }
            (Type::Jsonb, SqlValue::Object(obj)) => {
                let json_val = object_to_serde_json(obj);
                TypedValue {
                    sync_type: Type::Jsonb,
                    value: Value::Json(Box::new(json_val)),
                }
            }

            // Array types
            (Type::Array { element_type }, SqlValue::Array(arr)) => {
                let values: Vec<Value> = arr
                    .iter()
                    .map(|v| {
                        let sv = SurrealValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(sv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array
            (Type::Set { values: set_values }, SqlValue::Array(arr)) => {
                let values: Vec<Value> = arr
                    .iter()
                    .filter_map(|v| {
                        if let SqlValue::Strand(s) = v {
                            Some(Value::Text(s.as_str().to_string()))
                        } else {
                            None
                        }
                    })
                    .collect();
                TypedValue {
                    sync_type: Type::Set {
                        values: set_values.clone(),
                    },
                    value: Value::Array {
                        elements: values,
                        element_type: Box::new(Type::Text),
                    },
                }
            }

            // Enum - stored as string
            (
                Type::Enum {
                    values: enum_values,
                },
                SqlValue::Strand(s),
            ) => TypedValue {
                sync_type: Type::Enum {
                    values: enum_values.clone(),
                },
                value: Value::Text(s.as_str().to_string()),
            },

            // Geometry types - stored as SurrealDB Geometry
            (Type::Geometry { geometry_type }, SqlValue::Geometry(geo)) => {
                let json_val = geometry_to_serde_json(geo);
                TypedValue {
                    sync_type: Type::Geometry {
                        geometry_type: geometry_type.clone(),
                    },
                    value: Value::Json(Box::new(json_val)),
                }
            }

            // Thing - record reference
            (Type::Thing, SqlValue::Thing(thing)) => {
                let table = thing.tb.to_string();
                let id = match &thing.id {
                    surrealdb::sql::Id::String(s) => Value::Text(s.clone()),
                    surrealdb::sql::Id::Number(n) => Value::Int64(*n),
                    surrealdb::sql::Id::Uuid(u) => Value::Uuid(u.0),
                    _ => Value::Text(thing.id.to_string()),
                };
                TypedValue {
                    sync_type: Type::Thing,
                    value: Value::Thing {
                        table,
                        id: Box::new(id),
                    },
                }
            }

            // Fallback
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a SurrealDB Object to a HashMap of Value.
#[allow(dead_code)]
fn object_to_hashmap(obj: &Object) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for (key, value) in obj.iter() {
        map.insert(key.clone(), surreal_value_to_generated(value));
    }
    map
}

/// Convert a SurrealDB SqlValue to Value (without type context).
#[allow(dead_code)]
fn surreal_value_to_generated(value: &SqlValue) -> Value {
    match value {
        SqlValue::None => Value::Null,
        SqlValue::Bool(b) => Value::Bool(*b),
        SqlValue::Number(Number::Int(i)) => Value::Int64(*i),
        SqlValue::Number(Number::Float(f)) => Value::Float64(*f),
        SqlValue::Number(Number::Decimal(d)) => Value::Decimal {
            value: d.to_string(),
            precision: 38,
            scale: 10,
        },
        SqlValue::Strand(s) => Value::Text(s.as_str().to_string()),
        SqlValue::Bytes(b) => Value::Bytes(b.clone().into_inner()),
        SqlValue::Uuid(u) => Value::Uuid(u.0),
        SqlValue::Datetime(dt) => Value::LocalDateTime(dt.0),
        SqlValue::Array(arr) => {
            let elements: Vec<Value> = arr.iter().map(surreal_value_to_generated).collect();
            Value::Array {
                elements,
                element_type: Box::new(Type::Json),
            }
        }
        SqlValue::Object(obj) => {
            // Convert SurrealDB Object to Value::Object
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj.iter() {
                map.insert(k.clone(), surreal_value_to_generated(v));
            }
            Value::Object(map)
        }
        // Thing (record ID) - convert to Value::Thing
        SqlValue::Thing(thing) => {
            let table = thing.tb.to_string();
            let id = match &thing.id {
                surrealdb::sql::Id::String(s) => Value::Text(s.clone()),
                surrealdb::sql::Id::Number(n) => Value::Int64(*n),
                surrealdb::sql::Id::Uuid(u) => Value::Uuid(u.0),
                _ => Value::Text(thing.id.to_string()),
            };
            Value::Thing {
                table,
                id: Box::new(id),
            }
        }
        // Duration - convert to string
        SqlValue::Duration(dur) => Value::Text(dur.to_string()),
        // Geometry - convert to object
        SqlValue::Geometry(geo) => {
            let json_val = geometry_to_serde_json(geo);
            Value::Json(Box::new(json_val))
        }
        // Other types - convert to null
        _ => Value::Null,
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

/// Convert a SurrealDB SqlValue to serde_json::Value.
fn surreal_value_to_json(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::None | SqlValue::Null => serde_json::Value::Null,
        SqlValue::Bool(b) => serde_json::Value::Bool(*b),
        SqlValue::Number(n) => match n {
            Number::Int(i) => serde_json::json!(i),
            Number::Float(f) => serde_json::json!(f),
            Number::Decimal(d) => serde_json::json!(d.to_string()),
            _ => serde_json::Value::Null,
        },
        SqlValue::Strand(s) => serde_json::Value::String(s.as_str().to_string()),
        SqlValue::Datetime(dt) => serde_json::Value::String(dt.0.to_rfc3339()),
        SqlValue::Uuid(u) => serde_json::Value::String(u.0.to_string()),
        SqlValue::Array(arr) => {
            let values: Vec<serde_json::Value> = arr.iter().map(surreal_value_to_json).collect();
            serde_json::Value::Array(values)
        }
        SqlValue::Object(obj) => object_to_serde_json(obj),
        SqlValue::Geometry(geo) => geometry_to_serde_json(geo),
        _ => serde_json::Value::Null,
    }
}

/// Extract a typed value from a SurrealDB Object field.
pub fn extract_field(obj: &Object, field: &str, sync_type: &Type) -> TypedValue {
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
    schema: &[(String, Type)],
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
        let sv = SurrealValueWithSchema::new(SqlValue::None, Type::Text);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let sv = SurrealValueWithSchema::new(SqlValue::Bool(true), Type::Bool);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Bool(true)));
    }

    #[test]
    fn test_int_conversion() {
        let sv = SurrealValueWithSchema::new(SqlValue::Number(Number::Int(42)), Type::Int32);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let sv =
            SurrealValueWithSchema::new(SqlValue::Number(Number::Int(9876543210)), Type::Int64);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let sv =
            SurrealValueWithSchema::new(SqlValue::Number(Number::Float(1.23456)), Type::Float64);
        let tv = TypedValue::from(sv);
        if let Value::Float64(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let dec = rust_decimal::Decimal::from_str_exact("123.456").unwrap();
        let sv = SurrealValueWithSchema::new(
            SqlValue::Number(Number::Decimal(dec)),
            Type::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(sv);
        if let Value::Decimal {
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
        let sv =
            SurrealValueWithSchema::new(SqlValue::Strand(Strand::from("hello world")), Type::Text);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Text(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let sv = SurrealValueWithSchema::new(
            SqlValue::Strand(Strand::from("test")),
            Type::VarChar { length: 100 },
        );
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.sync_type, Type::VarChar { length: 100 }));
        assert!(matches!(tv.value, Value::Text(ref s) if s == "test"));
    }

    #[test]
    fn test_bytes_conversion() {
        let bytes = surrealdb::sql::Bytes::from(vec![0x01, 0x02, 0x03]);
        let sv = SurrealValueWithSchema::new(SqlValue::Bytes(bytes), Type::Bytes);
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let sv = SurrealValueWithSchema::new(
            SqlValue::Uuid(surrealdb::sql::Uuid::from(uuid)),
            Type::Uuid,
        );
        let tv = TypedValue::from(sv);
        if let Value::Uuid(u) = tv.value {
            assert_eq!(u, uuid);
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_uuid_from_string() {
        let sv = SurrealValueWithSchema::new(
            SqlValue::Strand(Strand::from("550e8400-e29b-41d4-a716-446655440000")),
            Type::Uuid,
        );
        let tv = TypedValue::from(sv);
        if let Value::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let sv = SurrealValueWithSchema::new(
            SqlValue::Datetime(Datetime::from(dt)),
            Type::LocalDateTime,
        );
        let tv = TypedValue::from(sv);
        if let Value::LocalDateTime(result_dt) = tv.value {
            assert_eq!(result_dt.year(), 2024);
            assert_eq!(result_dt.month(), 6);
            assert_eq!(result_dt.day(), 15);
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_date_from_string() {
        let sv =
            SurrealValueWithSchema::new(SqlValue::Strand(Strand::from("2024-06-15")), Type::Date);
        let tv = TypedValue::from(sv);
        if let Value::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let sv =
            SurrealValueWithSchema::new(SqlValue::Strand(Strand::from("14:30:45")), Type::Time);
        let tv = TypedValue::from(sv);
        if let Value::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_object_to_json() {
        let mut obj_map = BTreeMap::new();
        obj_map.insert("name".to_string(), SqlValue::Strand(Strand::from("test")));
        obj_map.insert("count".to_string(), SqlValue::Number(Number::Int(42)));
        let obj = Object::from(obj_map);

        let sv = SurrealValueWithSchema::new(SqlValue::Object(obj), Type::Json);
        let tv = TypedValue::from(sv);
        if let Value::Json(json_val) = tv.value {
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
            SqlValue::Number(Number::Int(1)),
            SqlValue::Number(Number::Int(2)),
            SqlValue::Number(Number::Int(3)),
        ]);
        let sv = SurrealValueWithSchema::new(
            SqlValue::Array(arr),
            Type::Array {
                element_type: Box::new(Type::Int32),
            },
        );
        let tv = TypedValue::from(sv);
        if let Value::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            // Values are converted as Int32
            assert!(matches!(elements[0], Value::Int32(1)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let arr = Array::from(vec![
            SqlValue::Strand(Strand::from("a")),
            SqlValue::Strand(Strand::from("b")),
        ]);
        let sv = SurrealValueWithSchema::new(
            SqlValue::Array(arr),
            Type::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(sv);
        if let Value::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let sv = SurrealValueWithSchema::new(
            SqlValue::Strand(Strand::from("active")),
            Type::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(sv);
        assert!(matches!(tv.value, Value::Text(ref s) if s == "active"));
    }

    #[test]
    fn test_extract_field() {
        let mut obj_map = BTreeMap::new();
        obj_map.insert("name".to_string(), SqlValue::Strand(Strand::from("Alice")));
        obj_map.insert("age".to_string(), SqlValue::Number(Number::Int(30)));
        let obj = Object::from(obj_map);

        let name = extract_field(&obj, "name", &Type::Text);
        assert!(matches!(name.value, Value::Text(ref s) if s == "Alice"));

        let age = extract_field(&obj, "age", &Type::Int32);
        assert!(matches!(age.value, Value::Int32(30)));

        let missing = extract_field(&obj, "missing", &Type::Text);
        assert!(matches!(missing.value, Value::Null));
    }

    #[test]
    fn test_object_to_typed_values() {
        let mut obj_map = BTreeMap::new();
        obj_map.insert("name".to_string(), SqlValue::Strand(Strand::from("Bob")));
        obj_map.insert("active".to_string(), SqlValue::Bool(true));
        obj_map.insert("score".to_string(), SqlValue::Number(Number::Float(95.5)));
        let obj = Object::from(obj_map);

        let schema = vec![
            ("name".to_string(), Type::Text),
            ("active".to_string(), Type::Bool),
            ("score".to_string(), Type::Float64),
        ];

        let values = object_to_typed_values(&obj, &schema);
        assert!(matches!(
            values.get("name").unwrap().value,
            Value::Text(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            Value::Bool(true)
        ));
        if let Value::Float64(f) = values.get("score").unwrap().value {
            assert!((f - 95.5).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }
}
