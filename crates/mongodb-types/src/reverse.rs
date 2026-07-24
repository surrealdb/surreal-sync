//! Reverse conversion: BSON value → TypedValue.
//!
//! This module provides conversion from MongoDB BSON values to sync-core's `TypedValue`.

use bson::Bson;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use sync_core::{Type, TypedValue, Value};

/// Parse an ISO 8601 duration string (PTxS or PTx.xxxxxxxxxS format).
///
/// Supports:
/// - Simple seconds: "PT181S" (181 seconds)
/// - Seconds with nanoseconds: "PT60.123456789S" (60 seconds + 123456789 nanoseconds)
fn parse_iso8601_duration(s: &str) -> Option<std::time::Duration> {
    let trimmed = s.trim();
    // Only accept "PTxS" or "PTx.xxxxxxxxxS" format
    if let Some(secs_str) = trimmed.strip_prefix("PT").and_then(|s| s.strip_suffix('S')) {
        if let Some(dot_pos) = secs_str.find('.') {
            // Has fractional seconds
            let secs: u64 = secs_str[..dot_pos].parse().ok()?;
            let nanos_str = &secs_str[dot_pos + 1..];
            let nanos: u32 = nanos_str.parse().ok()?;
            Some(std::time::Duration::new(secs, nanos))
        } else {
            let secs: u64 = secs_str.parse().ok()?;
            Some(std::time::Duration::from_secs(secs))
        }
    } else {
        None
    }
}

/// BSON value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct BsonValueWithSchema {
    /// The BSON value from MongoDB.
    pub value: Bson,
    /// The expected sync type for conversion.
    pub sync_type: Type,
}

impl BsonValueWithSchema {
    /// Create a new BsonValueWithSchema.
    pub fn new(value: Bson, sync_type: Type) -> Self {
        Self { value, sync_type }
    }

    /// Convert to TypedValue.
    pub fn to_typed_value(&self) -> TypedValue {
        TypedValue::from(self.clone())
    }
}

impl From<BsonValueWithSchema> for TypedValue {
    fn from(bv: BsonValueWithSchema) -> Self {
        match (&bv.sync_type, &bv.value) {
            // Null
            (sync_type, Bson::Null) => TypedValue {
                sync_type: sync_type.clone(),
                value: Value::Null,
            },

            // Boolean
            (Type::Bool, Bson::Boolean(b)) => TypedValue::bool(*b),

            // Integer types
            (Type::Int8 { width }, Bson::Int32(i)) => TypedValue {
                sync_type: Type::Int8 { width: *width },
                value: Value::Int32(*i),
            },
            (Type::Int16, Bson::Int32(i)) => TypedValue::int16(*i as i16),
            (Type::Int32, Bson::Int32(i)) => TypedValue::int32(*i),
            (Type::Int64, Bson::Int64(i)) => TypedValue::int64(*i),
            // Handle Int64 for Int (MongoDB might return Int64)
            (Type::Int32, Bson::Int64(i)) => TypedValue::int32(*i as i32),
            // Handle Int32 for BigInt
            (Type::Int64, Bson::Int32(i)) => TypedValue::int64(*i as i64),

            // Floating point
            (Type::Float32, Bson::Double(f)) => TypedValue::float32(*f as f32),
            (Type::Float64, Bson::Double(f)) => TypedValue::float64(*f),

            // Decimal - stored as string in MongoDB for precision
            (Type::Decimal { precision, scale }, Bson::String(s)) => {
                TypedValue::decimal(s, *precision, *scale)
            }
            // Also handle Decimal128 if present
            (Type::Decimal { precision, scale }, Bson::Decimal128(d)) => {
                TypedValue::decimal(d.to_string(), *precision, *scale)
            }

            // String types
            (Type::Char { length }, Bson::String(s)) => TypedValue {
                sync_type: Type::Char { length: *length },
                value: Value::Text(s.clone()),
            },
            (Type::VarChar { length }, Bson::String(s)) => TypedValue {
                sync_type: Type::VarChar { length: *length },
                value: Value::Text(s.clone()),
            },
            (Type::Text, Bson::String(s)) => TypedValue::text(s),

            // Binary types
            (Type::Blob, Bson::Binary(bin)) => TypedValue {
                sync_type: Type::Blob,
                value: Value::Bytes(bin.bytes.clone()),
            },
            (Type::Bytes, Bson::Binary(bin)) => TypedValue::bytes(bin.bytes.clone()),

            // UUID
            (Type::Uuid, Bson::Binary(bin)) => {
                if bin.subtype == bson::spec::BinarySubtype::Uuid
                    || bin.subtype == bson::spec::BinarySubtype::UuidOld
                {
                    if let Ok(uuid) = uuid::Uuid::from_slice(&bin.bytes) {
                        TypedValue::uuid(uuid)
                    } else {
                        TypedValue::null(Type::Uuid)
                    }
                } else {
                    TypedValue::null(Type::Uuid)
                }
            }
            // UUID might also be stored as string
            (Type::Uuid, Bson::String(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(Type::Uuid)
                }
            }

            // Date/time types
            (Type::LocalDateTime, Bson::DateTime(dt)) => TypedValue::datetime(dt.to_chrono()),
            (Type::LocalDateTimeNano, Bson::DateTime(dt)) => TypedValue {
                sync_type: Type::LocalDateTimeNano,
                value: Value::LocalDateTime(dt.to_chrono()),
            },
            (Type::ZonedDateTime, Bson::DateTime(dt)) => TypedValue {
                sync_type: Type::ZonedDateTime,
                value: Value::LocalDateTime(dt.to_chrono()),
            },

            // Date stored as string
            (Type::Date, Bson::String(s)) => {
                if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
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
            (Type::Time, Bson::String(s)) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
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
            (Type::Json, Bson::Document(doc)) => {
                let json = bson_doc_to_json(doc);
                TypedValue {
                    sync_type: Type::Json,
                    value: Value::Json(Box::new(json)),
                }
            }
            (Type::Jsonb, Bson::Document(doc)) => {
                let json = bson_doc_to_json(doc);
                TypedValue {
                    sync_type: Type::Jsonb,
                    value: Value::Json(Box::new(json)),
                }
            }

            // Array types
            (Type::Array { element_type }, Bson::Array(arr)) => {
                let values: Vec<Value> = arr
                    .iter()
                    .map(|v| {
                        let bv = BsonValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(bv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array in MongoDB
            (Type::Set { values: set_values }, Bson::Array(arr)) => {
                let values: Vec<Value> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Bson::String(s) = v {
                            Some(Value::Text(s.clone()))
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
                Bson::String(s),
            ) => TypedValue {
                sync_type: Type::Enum {
                    values: enum_values.clone(),
                },
                value: Value::Text(s.clone()),
            },

            // Geometry types - stored as GeoJSON document
            (Type::Geometry { geometry_type }, Bson::Document(doc)) => {
                let json = bson_doc_to_json(doc);
                TypedValue {
                    sync_type: Type::Geometry {
                        geometry_type: geometry_type.clone(),
                    },
                    value: Value::Json(Box::new(json)),
                }
            }

            // Duration - parse ISO 8601 duration string (PTxS or PTx.xxxxxxxxxS format)
            (Type::Duration, Bson::String(s)) => {
                if let Some(duration) = parse_iso8601_duration(s) {
                    TypedValue::duration(duration)
                } else {
                    TypedValue::null(Type::Duration)
                }
            }

            // Fallback for unhandled combinations
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a BSON document to a HashMap of Value.
#[allow(dead_code)]
fn bson_doc_to_hashmap(doc: &bson::Document) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for (key, value) in doc {
        map.insert(key.clone(), bson_to_generated_value(value));
    }
    map
}

/// Convert a BSON document to serde_json::Value.
fn bson_doc_to_json(doc: &bson::Document) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, value) in doc {
        map.insert(key.clone(), bson_to_json(value));
    }
    serde_json::Value::Object(map)
}

/// Convert a BSON value to serde_json::Value.
fn bson_to_json(value: &Bson) -> serde_json::Value {
    match value {
        Bson::Null => serde_json::Value::Null,
        Bson::Boolean(b) => serde_json::Value::Bool(*b),
        Bson::Int32(i) => serde_json::json!(*i),
        Bson::Int64(i) => serde_json::json!(*i),
        Bson::Double(f) => serde_json::json!(*f),
        Bson::String(s) => serde_json::Value::String(s.clone()),
        Bson::Array(arr) => serde_json::Value::Array(arr.iter().map(bson_to_json).collect()),
        Bson::Document(doc) => bson_doc_to_json(doc),
        _ => serde_json::Value::Null,
    }
}

/// Convert a BSON value to Value (without type context).
#[allow(dead_code)]
fn bson_to_generated_value(value: &Bson) -> Value {
    match value {
        Bson::Null => Value::Null,
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::Int32(i) => Value::Int32(*i),
        Bson::Int64(i) => Value::Int64(*i),
        Bson::Double(f) => Value::Float64(*f),
        Bson::String(s) => Value::Text(s.clone()),
        Bson::Binary(bin) => {
            if bin.subtype == bson::spec::BinarySubtype::Uuid
                || bin.subtype == bson::spec::BinarySubtype::UuidOld
            {
                if let Ok(uuid) = uuid::Uuid::from_slice(&bin.bytes) {
                    Value::Uuid(uuid)
                } else {
                    Value::Bytes(bin.bytes.clone())
                }
            } else {
                Value::Bytes(bin.bytes.clone())
            }
        }
        Bson::DateTime(dt) => Value::LocalDateTime(dt.to_chrono()),
        Bson::Decimal128(d) => Value::Decimal {
            value: d.to_string(),
            precision: 38,
            scale: 10,
        },
        Bson::Array(arr) => Value::Array {
            elements: arr.iter().map(bson_to_generated_value).collect(),
            element_type: Box::new(Type::Text),
        },
        Bson::Document(doc) => Value::Json(Box::new(bson_doc_to_json(doc))),
        // ObjectId - convert to string
        Bson::ObjectId(oid) => Value::Text(oid.to_hex()),
        // Timestamp - convert to DateTime
        Bson::Timestamp(ts) => {
            let secs = ts.time as i64;
            if let Some(dt) = DateTime::from_timestamp(secs, 0) {
                Value::LocalDateTime(dt)
            } else {
                Value::Null
            }
        }
        // Regex - convert to string pattern
        Bson::RegularExpression(regex) => {
            Value::Text(format!("/{}/{}", regex.pattern, regex.options))
        }
        // JavaScript - convert to string
        Bson::JavaScriptCode(code) => Value::Text(code.clone()),
        Bson::JavaScriptCodeWithScope(code_scope) => Value::Text(code_scope.code.clone()),
        // Symbol - convert to string
        Bson::Symbol(s) => Value::Text(s.clone()),
        // Undefined - treat as null
        Bson::Undefined => Value::Null,
        // MinKey/MaxKey - convert to string
        Bson::MinKey => Value::Text("$minKey".to_string()),
        Bson::MaxKey => Value::Text("$maxKey".to_string()),
        // DbPointer is deprecated
        Bson::DbPointer(_) => Value::Null,
    }
}

/// Extract a typed value from a BSON document field.
pub fn extract_field(doc: &bson::Document, field: &str, sync_type: &Type) -> TypedValue {
    match doc.get(field) {
        Some(value) => BsonValueWithSchema::new(value.clone(), sync_type.clone()).to_typed_value(),
        None => TypedValue::null(sync_type.clone()),
    }
}

/// Convert a complete BSON document to a map of TypedValues using schema.
pub fn document_to_typed_values(
    doc: &bson::Document,
    schema: &[(String, Type)],
) -> HashMap<String, TypedValue> {
    let mut result = HashMap::new();
    for (field_name, sync_type) in schema {
        let tv = extract_field(doc, field_name, sync_type);
        result.insert(field_name.clone(), tv);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, DateTime as BsonDateTime};
    use chrono::{Datelike, TimeZone, Utc};
    use sync_core::GeometryType;

    #[test]
    fn test_null_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Null, Type::Text);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Boolean(true), Type::Bool);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Bool(true)));
    }

    #[test]
    fn test_int32_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Int32(42), Type::Int32);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Int32(42)));
    }

    #[test]
    fn test_int64_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Int64(9876543210), Type::Int64);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Int64(9876543210)));
    }

    #[test]
    fn test_double_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Double(1.23456), Type::Float64);
        let tv = TypedValue::from(bv);
        if let Value::Float64(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_from_string() {
        let bv = BsonValueWithSchema::new(
            Bson::String("123.456".to_string()),
            Type::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(bv);
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
        let bv = BsonValueWithSchema::new(Bson::String("hello world".to_string()), Type::Text);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Text(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let bv = BsonValueWithSchema::new(
            Bson::String("test".to_string()),
            Type::VarChar { length: 100 },
        );
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.sync_type, Type::VarChar { length: 100 }));
        assert!(matches!(tv.value, Value::Text(ref s) if s == "test"));
    }

    #[test]
    fn test_binary_conversion() {
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0x01, 0x02, 0x03],
        };
        let bv = BsonValueWithSchema::new(Bson::Binary(bin), Type::Bytes);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_from_binary() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: uuid.as_bytes().to_vec(),
        };
        let bv = BsonValueWithSchema::new(Bson::Binary(bin), Type::Uuid);
        let tv = TypedValue::from(bv);
        if let Value::Uuid(u) = tv.value {
            assert_eq!(u, uuid);
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_uuid_from_string() {
        let bv = BsonValueWithSchema::new(
            Bson::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
            Type::Uuid,
        );
        let tv = TypedValue::from(bv);
        if let Value::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let bson_dt = BsonDateTime::from_chrono(dt);
        let bv = BsonValueWithSchema::new(Bson::DateTime(bson_dt), Type::LocalDateTime);
        let tv = TypedValue::from(bv);
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
        let bv = BsonValueWithSchema::new(Bson::String("2024-06-15".to_string()), Type::Date);
        let tv = TypedValue::from(bv);
        if let Value::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let bv = BsonValueWithSchema::new(Bson::String("14:30:45".to_string()), Type::Time);
        let tv = TypedValue::from(bv);
        if let Value::LocalDateTime(dt) = tv.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_document_to_json() {
        let doc = doc! {
            "name": "test",
            "count": 42
        };
        let bv = BsonValueWithSchema::new(Bson::Document(doc), Type::Json);
        let tv = TypedValue::from(bv);
        if let Value::Json(json_val) = tv.value {
            if let serde_json::Value::Object(map) = json_val.as_ref() {
                assert!(
                    matches!(map.get("name"), Some(serde_json::Value::String(s)) if s == "test")
                );
                assert!(
                    matches!(map.get("count"), Some(serde_json::Value::Number(n)) if n.as_i64() == Some(42))
                );
            } else {
                panic!("Expected Object");
            }
        } else {
            panic!("Expected Json");
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let arr = vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)];
        let bv = BsonValueWithSchema::new(
            Bson::Array(arr),
            Type::Array {
                element_type: Box::new(Type::Int32),
            },
        );
        let tv = TypedValue::from(bv);
        if let Value::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], Value::Int32(1)));
            assert!(matches!(elements[1], Value::Int32(2)));
            assert!(matches!(elements[2], Value::Int32(3)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let arr = vec![Bson::String("a".to_string()), Bson::String("b".to_string())];
        let bv = BsonValueWithSchema::new(
            Bson::Array(arr),
            Type::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(bv);
        if let Value::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let bv = BsonValueWithSchema::new(
            Bson::String("active".to_string()),
            Type::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Text(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_conversion() {
        let doc = doc! {
            "type": "Point",
            "coordinates": [-73.97, 40.77]
        };
        let bv = BsonValueWithSchema::new(
            Bson::Document(doc),
            Type::Geometry {
                geometry_type: GeometryType::Point,
            },
        );
        let tv = TypedValue::from(bv);
        if let Value::Json(json_val) = tv.value {
            if let serde_json::Value::Object(map) = json_val.as_ref() {
                assert!(
                    matches!(map.get("type"), Some(serde_json::Value::String(s)) if s == "Point")
                );
            } else {
                panic!("Expected Object");
            }
        } else {
            panic!("Expected Json");
        }
    }

    #[test]
    fn test_extract_field() {
        let doc = doc! {
            "name": "Alice",
            "age": 30
        };
        let name = extract_field(&doc, "name", &Type::Text);
        assert!(matches!(name.value, Value::Text(ref s) if s == "Alice"));

        let age = extract_field(&doc, "age", &Type::Int32);
        assert!(matches!(age.value, Value::Int32(30)));

        let missing = extract_field(&doc, "missing", &Type::Text);
        assert!(matches!(missing.value, Value::Null));
    }

    #[test]
    fn test_document_to_typed_values() {
        let doc = doc! {
            "name": "Bob",
            "active": true,
            "score": 95.5
        };
        let schema = vec![
            ("name".to_string(), Type::Text),
            ("active".to_string(), Type::Bool),
            ("score".to_string(), Type::Float64),
        ];

        let values = document_to_typed_values(&doc, &schema);
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

    #[test]
    fn test_int64_to_int_conversion() {
        // MongoDB might return Int64 when we expect Int32
        let bv = BsonValueWithSchema::new(Bson::Int64(100), Type::Int32);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Int32(100)));
    }

    #[test]
    fn test_int32_to_bigint_conversion() {
        // MongoDB might return Int32 when we expect Int64
        let bv = BsonValueWithSchema::new(Bson::Int32(100), Type::Int64);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, Value::Int64(100)));
    }

    #[test]
    fn test_objectid_in_document() {
        let oid = bson::oid::ObjectId::new();
        let doc = doc! {
            "_id": oid
        };
        let map = bson_doc_to_hashmap(&doc);
        if let Some(Value::Text(s)) = map.get("_id") {
            assert_eq!(s, &oid.to_hex());
        } else {
            panic!("Expected String for ObjectId");
        }
    }
}
