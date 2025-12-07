//! Forward conversion: TypedValue → SurrealDB value.
//!
//! This module provides conversion from sync-core's `TypedValue` to SurrealDB values.

use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;
use surrealdb::sql::{Array, Datetime, Geometry, Number, Object, Strand, Thing, Value};
use sync_core::{GeometryType, TypedValue, UniversalType, UniversalValue};

/// Wrapper for SurrealDB values.
#[derive(Debug, Clone)]
pub struct SurrealValue(pub Value);

impl SurrealValue {
    /// Get the inner SurrealDB value.
    pub fn into_inner(self) -> Value {
        self.0
    }

    /// Get a reference to the inner SurrealDB value.
    pub fn as_inner(&self) -> &Value {
        &self.0
    }
}

impl From<TypedValue> for SurrealValue {
    fn from(tv: TypedValue) -> Self {
        match (&tv.sync_type, &tv.value) {
            // Null
            (_, UniversalValue::Null) => SurrealValue(Value::None),

            // Boolean
            (UniversalType::Bool, UniversalValue::Bool(b)) => SurrealValue(Value::Bool(*b)),

            // Integer types - SurrealDB uses Number
            (UniversalType::TinyInt { .. }, UniversalValue::Int32(i)) => {
                SurrealValue(Value::Number(Number::Int(*i as i64)))
            }
            (UniversalType::SmallInt, UniversalValue::Int32(i)) => {
                SurrealValue(Value::Number(Number::Int(*i as i64)))
            }
            (UniversalType::Int, UniversalValue::Int32(i)) => {
                SurrealValue(Value::Number(Number::Int(*i as i64)))
            }
            (UniversalType::BigInt, UniversalValue::Int64(i)) => {
                SurrealValue(Value::Number(Number::Int(*i)))
            }

            // Floating point
            (UniversalType::Float, UniversalValue::Float64(f)) => {
                SurrealValue(Value::Number(Number::Float(*f)))
            }
            (UniversalType::Double, UniversalValue::Float64(f)) => {
                SurrealValue(Value::Number(Number::Float(*f)))
            }

            // Decimal - SurrealDB supports decimal with up to 128-bit precision
            (UniversalType::Decimal { precision, .. }, UniversalValue::Decimal { value, .. }) => {
                if *precision <= 38 {
                    // Fits in Decimal128
                    match Decimal::from_str(value) {
                        Ok(dec) => SurrealValue(Value::Number(Number::Decimal(dec))),
                        Err(_) => SurrealValue(Value::Strand(Strand::from(value.clone()))),
                    }
                } else {
                    // Store high precision as string
                    SurrealValue(Value::Strand(Strand::from(value.clone())))
                }
            }

            // String types
            (UniversalType::Char { .. }, UniversalValue::String(s)) => {
                SurrealValue(Value::Strand(Strand::from(s.clone())))
            }
            (UniversalType::VarChar { .. }, UniversalValue::String(s)) => {
                SurrealValue(Value::Strand(Strand::from(s.clone())))
            }
            (UniversalType::Text, UniversalValue::String(s)) => {
                SurrealValue(Value::Strand(Strand::from(s.clone())))
            }

            // Binary types - SurrealDB uses Bytes
            (UniversalType::Blob, UniversalValue::Bytes(b)) => {
                SurrealValue(Value::Bytes(surrealdb::sql::Bytes::from(b.clone())))
            }
            (UniversalType::Bytes, UniversalValue::Bytes(b)) => {
                SurrealValue(Value::Bytes(surrealdb::sql::Bytes::from(b.clone())))
            }

            // Date/time types - SurrealDB uses Datetime
            (UniversalType::Date, UniversalValue::DateTime(dt)) => {
                // Store date as string in YYYY-MM-DD format
                SurrealValue(Value::Strand(Strand::from(
                    dt.format("%Y-%m-%d").to_string(),
                )))
            }
            (UniversalType::Time, UniversalValue::DateTime(dt)) => {
                // Store time as string in HH:MM:SS format
                SurrealValue(Value::Strand(Strand::from(
                    dt.format("%H:%M:%S").to_string(),
                )))
            }
            (UniversalType::DateTime, UniversalValue::DateTime(dt)) => {
                SurrealValue(Value::Datetime(Datetime::from(*dt)))
            }
            (UniversalType::DateTimeNano, UniversalValue::DateTime(dt)) => {
                SurrealValue(Value::Datetime(Datetime::from(*dt)))
            }
            (UniversalType::TimestampTz, UniversalValue::DateTime(dt)) => {
                SurrealValue(Value::Datetime(Datetime::from(*dt)))
            }

            // UUID - SurrealDB has native UUID support
            (UniversalType::Uuid, UniversalValue::Uuid(u)) => {
                SurrealValue(Value::Uuid(surrealdb::sql::Uuid::from(*u)))
            }

            // JSON types - convert to SurrealDB Object
            (UniversalType::Json, UniversalValue::Object(map)) => {
                let obj = hashmap_to_object(map);
                SurrealValue(Value::Object(obj))
            }
            (UniversalType::Jsonb, UniversalValue::Object(map)) => {
                let obj = hashmap_to_object(map);
                SurrealValue(Value::Object(obj))
            }
            // JSON can also be a string
            (UniversalType::Json, UniversalValue::String(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => SurrealValue(json_to_surreal(&v)),
                    Err(_) => SurrealValue(Value::Strand(Strand::from(s.clone()))),
                }
            }
            (UniversalType::Jsonb, UniversalValue::String(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => SurrealValue(json_to_surreal(&v)),
                    Err(_) => SurrealValue(Value::Strand(Strand::from(s.clone()))),
                }
            }
            // JSON can also be an array (e.g., from MySQL SET columns or JSON arrays)
            (UniversalType::Json, UniversalValue::Array(arr)) => {
                let surreal_arr: Vec<Value> = arr.iter().map(generated_value_to_surreal).collect();
                SurrealValue(Value::Array(surrealdb::sql::Array::from(surreal_arr)))
            }
            (UniversalType::Jsonb, UniversalValue::Array(arr)) => {
                let surreal_arr: Vec<Value> = arr.iter().map(generated_value_to_surreal).collect();
                SurrealValue(Value::Array(surrealdb::sql::Array::from(surreal_arr)))
            }

            // Array types
            (UniversalType::Array { element_type }, UniversalValue::Array(arr)) => {
                let surreal_arr: Vec<Value> = arr
                    .iter()
                    .map(|v| {
                        let tv = TypedValue {
                            sync_type: (**element_type).clone(),
                            value: v.clone(),
                        };
                        SurrealValue::from(tv).into_inner()
                    })
                    .collect();
                SurrealValue(Value::Array(Array::from(surreal_arr)))
            }

            // Set - stored as array of strings
            (UniversalType::Set { .. }, UniversalValue::Array(arr)) => {
                let surreal_arr: Vec<Value> = arr
                    .iter()
                    .map(|v| match v {
                        UniversalValue::String(s) => Value::Strand(Strand::from(s.clone())),
                        _ => Value::None,
                    })
                    .collect();
                SurrealValue(Value::Array(Array::from(surreal_arr)))
            }

            // Enum - stored as string
            (UniversalType::Enum { .. }, UniversalValue::String(s)) => {
                SurrealValue(Value::Strand(Strand::from(s.clone())))
            }

            // Geometry types - use SurrealDB's native Geometry
            (UniversalType::Geometry { geometry_type }, UniversalValue::Object(map)) => {
                let geometry = match geometry_type {
                    GeometryType::Point => {
                        if let Some(coords) = extract_coordinates(map) {
                            if coords.len() >= 2 {
                                Geometry::Point((coords[0], coords[1]).into())
                            } else {
                                return SurrealValue(Value::None);
                            }
                        } else {
                            return SurrealValue(Value::None);
                        }
                    }
                    GeometryType::LineString => {
                        if let Some(coords) = extract_line_coordinates(map) {
                            Geometry::Line(geo_types::LineString::new(coords))
                        } else {
                            return SurrealValue(Value::None);
                        }
                    }
                    GeometryType::Polygon => {
                        if let Some(coords) = extract_polygon_coordinates(map) {
                            Geometry::Polygon(geo_types::Polygon::new(coords, vec![]))
                        } else {
                            return SurrealValue(Value::None);
                        }
                    }
                    GeometryType::MultiPoint => {
                        if let Some(coords) = extract_multi_point_coordinates(map) {
                            Geometry::MultiPoint(geo_types::MultiPoint::new(coords))
                        } else {
                            return SurrealValue(Value::None);
                        }
                    }
                    _ => {
                        // For complex geometries, store as Object
                        let obj = hashmap_to_object(map);
                        return SurrealValue(Value::Object(obj));
                    }
                };
                SurrealValue(Value::Geometry(geometry))
            }

            // Explicit failure for unexpected type combinations
            (sync_type, value) => {
                panic!(
                    "Unsupported type combination in SurrealValue::from(TypedValue): \
                    sync_type={sync_type:?}, value={value:?}. \
                    This is a bug - please add handling for this type combination."
                )
            }
        }
    }
}

/// Convert a HashMap of UniversalValue to a SurrealDB Object.
fn hashmap_to_object(map: &std::collections::HashMap<String, UniversalValue>) -> Object {
    let mut obj = BTreeMap::new();
    for (key, value) in map {
        obj.insert(key.clone(), generated_value_to_surreal(value));
    }
    Object::from(obj)
}

/// Convert a UniversalValue to a SurrealDB Value (without type context).
fn generated_value_to_surreal(value: &UniversalValue) -> Value {
    match value {
        UniversalValue::Null => Value::None,
        UniversalValue::Bool(b) => Value::Bool(*b),
        UniversalValue::Int32(i) => Value::Number(Number::Int(*i as i64)),
        UniversalValue::Int64(i) => Value::Number(Number::Int(*i)),
        UniversalValue::Float64(f) => Value::Number(Number::Float(*f)),
        UniversalValue::String(s) => Value::Strand(Strand::from(s.clone())),
        UniversalValue::Bytes(b) => Value::Bytes(surrealdb::sql::Bytes::from(b.clone())),
        UniversalValue::Uuid(u) => Value::Uuid(surrealdb::sql::Uuid::from(*u)),
        UniversalValue::DateTime(dt) => Value::Datetime(Datetime::from(*dt)),
        UniversalValue::Decimal { value, .. } => match Decimal::from_str(value) {
            Ok(dec) => Value::Number(Number::Decimal(dec)),
            Err(_) => Value::Strand(Strand::from(value.clone())),
        },
        UniversalValue::Array(arr) => Value::Array(Array::from(
            arr.iter()
                .map(generated_value_to_surreal)
                .collect::<Vec<_>>(),
        )),
        UniversalValue::Object(map) => Value::Object(hashmap_to_object(map)),
    }
}

/// Convert a serde_json::Value to SurrealDB Value.
fn json_to_surreal(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::None,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(Number::Int(i))
            } else if let Some(f) = n.as_f64() {
                Value::Number(Number::Float(f))
            } else {
                Value::None
            }
        }
        serde_json::Value::String(s) => Value::Strand(Strand::from(s.clone())),
        serde_json::Value::Array(arr) => Value::Array(Array::from(
            arr.iter().map(json_to_surreal).collect::<Vec<_>>(),
        )),
        serde_json::Value::Object(map) => {
            let mut obj = BTreeMap::new();
            for (k, v) in map {
                obj.insert(k.clone(), json_to_surreal(v));
            }
            Value::Object(Object::from(obj))
        }
    }
}

/// Extract coordinates from a GeoJSON-like object.
fn extract_coordinates(
    map: &std::collections::HashMap<String, UniversalValue>,
) -> Option<Vec<f64>> {
    match map.get("coordinates") {
        Some(UniversalValue::Array(arr)) => {
            let coords: Vec<f64> = arr
                .iter()
                .filter_map(|v| match v {
                    UniversalValue::Float64(f) => Some(*f),
                    UniversalValue::Int32(i) => Some(*i as f64),
                    UniversalValue::Int64(i) => Some(*i as f64),
                    _ => None,
                })
                .collect();
            if coords.is_empty() {
                None
            } else {
                Some(coords)
            }
        }
        _ => None,
    }
}

/// Extract line coordinates from a GeoJSON-like object.
fn extract_line_coordinates(
    map: &std::collections::HashMap<String, UniversalValue>,
) -> Option<Vec<geo_types::Coord<f64>>> {
    match map.get("coordinates") {
        Some(UniversalValue::Array(arr)) => {
            let coords: Vec<geo_types::Coord<f64>> = arr
                .iter()
                .filter_map(|v| {
                    if let UniversalValue::Array(point) = v {
                        if point.len() >= 2 {
                            let x = match &point[0] {
                                UniversalValue::Float64(f) => *f,
                                UniversalValue::Int64(i) => *i as f64,
                                _ => return None,
                            };
                            let y = match &point[1] {
                                UniversalValue::Float64(f) => *f,
                                UniversalValue::Int64(i) => *i as f64,
                                _ => return None,
                            };
                            Some(geo_types::Coord { x, y })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();
            if coords.is_empty() {
                None
            } else {
                Some(coords)
            }
        }
        _ => None,
    }
}

/// Extract polygon exterior ring coordinates.
fn extract_polygon_coordinates(
    map: &std::collections::HashMap<String, UniversalValue>,
) -> Option<geo_types::LineString<f64>> {
    match map.get("coordinates") {
        Some(UniversalValue::Array(rings)) => {
            // First ring is the exterior ring
            if let Some(UniversalValue::Array(ring)) = rings.first() {
                let coords: Vec<geo_types::Coord<f64>> = ring
                    .iter()
                    .filter_map(|v| {
                        if let UniversalValue::Array(point) = v {
                            if point.len() >= 2 {
                                let x = match &point[0] {
                                    UniversalValue::Float64(f) => *f,
                                    UniversalValue::Int64(i) => *i as f64,
                                    _ => return None,
                                };
                                let y = match &point[1] {
                                    UniversalValue::Float64(f) => *f,
                                    UniversalValue::Int64(i) => *i as f64,
                                    _ => return None,
                                };
                                Some(geo_types::Coord { x, y })
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect();
                if coords.is_empty() {
                    None
                } else {
                    Some(geo_types::LineString::new(coords))
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract multi-point coordinates.
fn extract_multi_point_coordinates(
    map: &std::collections::HashMap<String, UniversalValue>,
) -> Option<Vec<geo_types::Point<f64>>> {
    match map.get("coordinates") {
        Some(UniversalValue::Array(arr)) => {
            let points: Vec<geo_types::Point<f64>> = arr
                .iter()
                .filter_map(|v| {
                    if let UniversalValue::Array(point) = v {
                        if point.len() >= 2 {
                            let x = match &point[0] {
                                UniversalValue::Float64(f) => *f,
                                UniversalValue::Int64(i) => *i as f64,
                                _ => return None,
                            };
                            let y = match &point[1] {
                                UniversalValue::Float64(f) => *f,
                                UniversalValue::Int64(i) => *i as f64,
                                _ => return None,
                            };
                            Some(geo_types::Point::new(x, y))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();
            if points.is_empty() {
                None
            } else {
                Some(points)
            }
        }
        _ => None,
    }
}

/// Create a SurrealDB Thing (record ID).
pub fn create_thing(table: &str, id: &UniversalValue) -> Option<Thing> {
    let id_part = match id {
        UniversalValue::String(s) => surrealdb::sql::Id::String(s.clone()),
        UniversalValue::Int32(i) => surrealdb::sql::Id::Number(*i as i64),
        UniversalValue::Int64(i) => surrealdb::sql::Id::Number(*i),
        UniversalValue::Uuid(u) => surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(*u)),
        _ => return None,
    };
    Some(Thing::from((table, id_part)))
}

/// Convert a complete row to a SurrealDB Object for insertion.
pub fn typed_values_to_object<I>(fields: I) -> Object
where
    I: IntoIterator<Item = (String, TypedValue)>,
{
    let mut obj = BTreeMap::new();
    for (name, tv) in fields {
        obj.insert(name, SurrealValue::from(tv).into_inner());
    }
    Object::from(obj)
}

/// Convert a HashMap of TypedValue to a HashMap of surrealdb::sql::Value.
///
/// This is useful for sources that need to build record data as a HashMap
/// before creating SurrealDB records.
pub fn typed_values_to_surreal_map(
    typed_values: std::collections::HashMap<String, TypedValue>,
) -> std::collections::HashMap<String, Value> {
    typed_values
        .into_iter()
        .map(|(k, v)| (k, SurrealValue::from(v).into_inner()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};
    use std::collections::HashMap;

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(UniversalType::Text);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::None));
    }

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::Bool(true)));

        let tv = TypedValue::bool(false);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::Bool(false)));
    }

    #[test]
    fn test_tinyint_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::TinyInt { width: 4 },
            value: UniversalValue::Int32(127),
        };
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::Number(Number::Int(127))));
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::smallint(32000);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::Number(Number::Int(32000))));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int(12345);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::Number(Number::Int(12345))));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::bigint(9876543210);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(
            surreal_val.0,
            Value::Number(Number::Int(9876543210))
        ));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float(1.234);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Number(Number::Float(f)) = surreal_val.0 {
            assert!((f - 1.234).abs() < 0.0001);
        } else {
            panic!("Expected Float");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::double(1.23456);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Number(Number::Float(f)) = surreal_val.0 {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.45", 10, 2);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Number(Number::Decimal(d)) = surreal_val.0 {
            assert_eq!(d.to_string(), "123.45");
        } else {
            panic!("Expected Decimal");
        }
    }

    #[test]
    fn test_high_precision_decimal_conversion() {
        // Precision > 38 should be stored as string
        let tv = TypedValue::decimal("12345678901234567890123456789012345678901234567890", 50, 0);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(
                s.as_str(),
                "12345678901234567890123456789012345678901234567890"
            );
        } else {
            panic!("Expected Strand for high precision decimal");
        }
    }

    #[test]
    fn test_char_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::Char { length: 10 },
            value: UniversalValue::String("test".to_string()),
        };
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "test");
        } else {
            panic!("Expected Strand");
        }
    }

    #[test]
    fn test_text_conversion() {
        let tv = TypedValue::text("hello world");
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "hello world");
        } else {
            panic!("Expected Strand");
        }
    }

    #[test]
    fn test_bytes_conversion() {
        let tv = TypedValue::bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Bytes(b) = surreal_val.0 {
            assert_eq!(b.as_slice(), &[0xDE, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("Expected Bytes");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(u);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Uuid(uuid) = surreal_val.0 {
            assert_eq!(uuid.0.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let tv = TypedValue::datetime(dt);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Datetime(sdt) = surreal_val.0 {
            assert_eq!(sdt.0.year(), 2024);
            assert_eq!(sdt.0.month(), 6);
            assert_eq!(sdt.0.day(), 15);
        } else {
            panic!("Expected Datetime");
        }
    }

    #[test]
    fn test_date_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap();
        let tv = TypedValue {
            sync_type: UniversalType::Date,
            value: UniversalValue::DateTime(dt),
        };
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "2024-06-15");
        } else {
            panic!("Expected Strand");
        }
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue {
            sync_type: UniversalType::Time,
            value: UniversalValue::DateTime(dt),
        };
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "14:30:45");
        } else {
            panic!("Expected Strand");
        }
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
        let surreal_val: SurrealValue = tv.into();
        if let Value::Object(obj) = surreal_val.0 {
            assert!(obj.get("name").is_some());
            assert!(obj.get("count").is_some());
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_json_string_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::Json,
            value: UniversalValue::String(r#"{"key": "value"}"#.to_string()),
        };
        let surreal_val: SurrealValue = tv.into();
        if let Value::Object(obj) = surreal_val.0 {
            assert!(obj.get("key").is_some());
        } else {
            panic!("Expected Object");
        }
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
        let surreal_val: SurrealValue = tv.into();
        if let Value::Array(arr) = surreal_val.0 {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected Array");
        }
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
        let surreal_val: SurrealValue = tv.into();
        if let Value::Array(arr) = surreal_val.0 {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
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
        let surreal_val: SurrealValue = tv.into();
        if let Value::Array(arr) = surreal_val.0 {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let tv = TypedValue {
            sync_type: UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
            value: UniversalValue::String("active".to_string()),
        };
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "active");
        } else {
            panic!("Expected Strand");
        }
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
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, Value::Geometry(_)));
    }

    #[test]
    fn test_create_thing_string() {
        let id = UniversalValue::String("user123".to_string());
        let thing = create_thing("users", &id);
        assert!(thing.is_some());
        let t = thing.unwrap();
        assert_eq!(t.tb, "users");
    }

    #[test]
    fn test_create_thing_int() {
        let id = UniversalValue::Int64(42);
        let thing = create_thing("users", &id);
        assert!(thing.is_some());
        let t = thing.unwrap();
        assert_eq!(t.tb, "users");
    }

    #[test]
    fn test_create_thing_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = UniversalValue::Uuid(u);
        let thing = create_thing("users", &id);
        assert!(thing.is_some());
        let t = thing.unwrap();
        assert_eq!(t.tb, "users");
    }

    #[test]
    fn test_typed_values_to_object() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Alice")),
            ("age".to_string(), TypedValue::int(30)),
            ("active".to_string(), TypedValue::bool(true)),
        ];

        let obj = typed_values_to_object(fields);
        assert!(obj.get("name").is_some());
        assert!(obj.get("age").is_some());
        assert!(obj.get("active").is_some());
    }
}
