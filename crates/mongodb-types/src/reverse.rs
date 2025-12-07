//! Reverse conversion: BSON value → TypedValue.
//!
//! This module provides conversion from MongoDB BSON values to sync-core's `TypedValue`.

use bson::Bson;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use sync_core::{TypedValue, UniversalType, UniversalValue};

/// BSON value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct BsonValueWithSchema {
    /// The BSON value from MongoDB.
    pub value: Bson,
    /// The expected sync type for conversion.
    pub sync_type: UniversalType,
}

impl BsonValueWithSchema {
    /// Create a new BsonValueWithSchema.
    pub fn new(value: Bson, sync_type: UniversalType) -> Self {
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
                value: UniversalValue::Null,
            },

            // Boolean
            (UniversalType::Bool, Bson::Boolean(b)) => TypedValue::bool(*b),

            // Integer types
            (UniversalType::TinyInt { width }, Bson::Int32(i)) => TypedValue {
                sync_type: UniversalType::TinyInt { width: *width },
                value: UniversalValue::Int(*i),
            },
            (UniversalType::SmallInt, Bson::Int32(i)) => TypedValue::smallint(*i as i16),
            (UniversalType::Int, Bson::Int32(i)) => TypedValue::int(*i),
            (UniversalType::BigInt, Bson::Int64(i)) => TypedValue::bigint(*i),
            // Handle Int64 for Int (MongoDB might return Int64)
            (UniversalType::Int, Bson::Int64(i)) => TypedValue::int(*i as i32),
            // Handle Int32 for BigInt
            (UniversalType::BigInt, Bson::Int32(i)) => TypedValue::bigint(*i as i64),

            // Floating point
            (UniversalType::Float, Bson::Double(f)) => TypedValue::float(*f as f32),
            (UniversalType::Double, Bson::Double(f)) => TypedValue::double(*f),

            // Decimal - stored as string in MongoDB for precision
            (UniversalType::Decimal { precision, scale }, Bson::String(s)) => {
                TypedValue::decimal(s, *precision, *scale)
            }
            // Also handle Decimal128 if present
            (UniversalType::Decimal { precision, scale }, Bson::Decimal128(d)) => {
                TypedValue::decimal(d.to_string(), *precision, *scale)
            }

            // String types
            (UniversalType::Char { length }, Bson::String(s)) => TypedValue {
                sync_type: UniversalType::Char { length: *length },
                value: UniversalValue::Text(s.clone()),
            },
            (UniversalType::VarChar { length }, Bson::String(s)) => TypedValue {
                sync_type: UniversalType::VarChar { length: *length },
                value: UniversalValue::Text(s.clone()),
            },
            (UniversalType::Text, Bson::String(s)) => TypedValue::text(s),

            // Binary types
            (UniversalType::Blob, Bson::Binary(bin)) => TypedValue {
                sync_type: UniversalType::Blob,
                value: UniversalValue::Bytes(bin.bytes.clone()),
            },
            (UniversalType::Bytes, Bson::Binary(bin)) => TypedValue::bytes(bin.bytes.clone()),

            // UUID
            (UniversalType::Uuid, Bson::Binary(bin)) => {
                if bin.subtype == bson::spec::BinarySubtype::Uuid
                    || bin.subtype == bson::spec::BinarySubtype::UuidOld
                {
                    if let Ok(uuid) = uuid::Uuid::from_slice(&bin.bytes) {
                        TypedValue::uuid(uuid)
                    } else {
                        TypedValue::null(UniversalType::Uuid)
                    }
                } else {
                    TypedValue::null(UniversalType::Uuid)
                }
            }
            // UUID might also be stored as string
            (UniversalType::Uuid, Bson::String(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(UniversalType::Uuid)
                }
            }

            // Date/time types
            (UniversalType::DateTime, Bson::DateTime(dt)) => TypedValue::datetime(dt.to_chrono()),
            (UniversalType::DateTimeNano, Bson::DateTime(dt)) => TypedValue {
                sync_type: UniversalType::DateTimeNano,
                value: UniversalValue::DateTime(dt.to_chrono()),
            },
            (UniversalType::TimestampTz, Bson::DateTime(dt)) => TypedValue {
                sync_type: UniversalType::TimestampTz,
                value: UniversalValue::DateTime(dt.to_chrono()),
            },

            // Date stored as string
            (UniversalType::Date, Bson::String(s)) => {
                if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let datetime = dt.and_hms_opt(0, 0, 0).unwrap();
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: UniversalType::Date,
                        value: UniversalValue::DateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(UniversalType::Date)
                }
            }

            // Time stored as string
            (UniversalType::Time, Bson::String(s)) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    let datetime = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue {
                        sync_type: UniversalType::Time,
                        value: UniversalValue::DateTime(utc_dt),
                    }
                } else {
                    TypedValue::null(UniversalType::Time)
                }
            }

            // JSON types
            (UniversalType::Json, Bson::Document(doc)) => {
                let json = bson_doc_to_json(doc);
                TypedValue {
                    sync_type: UniversalType::Json,
                    value: UniversalValue::Json(Box::new(json)),
                }
            }
            (UniversalType::Jsonb, Bson::Document(doc)) => {
                let json = bson_doc_to_json(doc);
                TypedValue {
                    sync_type: UniversalType::Jsonb,
                    value: UniversalValue::Json(Box::new(json)),
                }
            }

            // Array types
            (UniversalType::Array { element_type }, Bson::Array(arr)) => {
                let values: Vec<UniversalValue> = arr
                    .iter()
                    .map(|v| {
                        let bv = BsonValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(bv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array in MongoDB
            (UniversalType::Set { values: set_values }, Bson::Array(arr)) => {
                let values: Vec<UniversalValue> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Bson::String(s) = v {
                            Some(UniversalValue::Text(s.clone()))
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
                Bson::String(s),
            ) => TypedValue {
                sync_type: UniversalType::Enum {
                    values: enum_values.clone(),
                },
                value: UniversalValue::Text(s.clone()),
            },

            // Geometry types - stored as GeoJSON document
            (UniversalType::Geometry { geometry_type }, Bson::Document(doc)) => {
                let json = bson_doc_to_json(doc);
                TypedValue {
                    sync_type: UniversalType::Geometry {
                        geometry_type: geometry_type.clone(),
                    },
                    value: UniversalValue::Json(Box::new(json)),
                }
            }

            // Fallback for unhandled combinations
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a BSON document to a HashMap of UniversalValue.
#[allow(dead_code)]
fn bson_doc_to_hashmap(doc: &bson::Document) -> HashMap<String, UniversalValue> {
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

/// Convert a BSON value to UniversalValue (without type context).
#[allow(dead_code)]
fn bson_to_generated_value(value: &Bson) -> UniversalValue {
    match value {
        Bson::Null => UniversalValue::Null,
        Bson::Boolean(b) => UniversalValue::Bool(*b),
        Bson::Int32(i) => UniversalValue::Int(*i),
        Bson::Int64(i) => UniversalValue::BigInt(*i),
        Bson::Double(f) => UniversalValue::Double(*f),
        Bson::String(s) => UniversalValue::Text(s.clone()),
        Bson::Binary(bin) => {
            if bin.subtype == bson::spec::BinarySubtype::Uuid
                || bin.subtype == bson::spec::BinarySubtype::UuidOld
            {
                if let Ok(uuid) = uuid::Uuid::from_slice(&bin.bytes) {
                    UniversalValue::Uuid(uuid)
                } else {
                    UniversalValue::Bytes(bin.bytes.clone())
                }
            } else {
                UniversalValue::Bytes(bin.bytes.clone())
            }
        }
        Bson::DateTime(dt) => UniversalValue::DateTime(dt.to_chrono()),
        Bson::Decimal128(d) => UniversalValue::Decimal {
            value: d.to_string(),
            precision: 38,
            scale: 10,
        },
        Bson::Array(arr) => UniversalValue::Array {
            elements: arr.iter().map(bson_to_generated_value).collect(),
            element_type: Box::new(UniversalType::Text),
        },
        Bson::Document(doc) => UniversalValue::Json(Box::new(bson_doc_to_json(doc))),
        // ObjectId - convert to string
        Bson::ObjectId(oid) => UniversalValue::Text(oid.to_hex()),
        // Timestamp - convert to DateTime
        Bson::Timestamp(ts) => {
            let secs = ts.time as i64;
            if let Some(dt) = DateTime::from_timestamp(secs, 0) {
                UniversalValue::DateTime(dt)
            } else {
                UniversalValue::Null
            }
        }
        // Regex - convert to string pattern
        Bson::RegularExpression(regex) => {
            UniversalValue::Text(format!("/{}/{}", regex.pattern, regex.options))
        }
        // JavaScript - convert to string
        Bson::JavaScriptCode(code) => UniversalValue::Text(code.clone()),
        Bson::JavaScriptCodeWithScope(code_scope) => UniversalValue::Text(code_scope.code.clone()),
        // Symbol - convert to string
        Bson::Symbol(s) => UniversalValue::Text(s.clone()),
        // Undefined - treat as null
        Bson::Undefined => UniversalValue::Null,
        // MinKey/MaxKey - convert to string
        Bson::MinKey => UniversalValue::Text("$minKey".to_string()),
        Bson::MaxKey => UniversalValue::Text("$maxKey".to_string()),
        // DbPointer is deprecated
        Bson::DbPointer(_) => UniversalValue::Null,
    }
}

/// Extract a typed value from a BSON document field.
pub fn extract_field(doc: &bson::Document, field: &str, sync_type: &UniversalType) -> TypedValue {
    match doc.get(field) {
        Some(value) => BsonValueWithSchema::new(value.clone(), sync_type.clone()).to_typed_value(),
        None => TypedValue::null(sync_type.clone()),
    }
}

/// Convert a complete BSON document to a map of TypedValues using schema.
pub fn document_to_typed_values(
    doc: &bson::Document,
    schema: &[(String, UniversalType)],
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
        let bv = BsonValueWithSchema::new(Bson::Null, UniversalType::Text);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Boolean(true), UniversalType::Bool);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_int32_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Int32(42), UniversalType::Int);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Int(42)));
    }

    #[test]
    fn test_int64_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Int64(9876543210), UniversalType::BigInt);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::BigInt(9876543210)));
    }

    #[test]
    fn test_double_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Double(1.23456), UniversalType::Double);
        let tv = TypedValue::from(bv);
        if let UniversalValue::Double(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_from_string() {
        let bv = BsonValueWithSchema::new(
            Bson::String("123.456".to_string()),
            UniversalType::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(bv);
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
        let bv =
            BsonValueWithSchema::new(Bson::String("hello world".to_string()), UniversalType::Text);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let bv = BsonValueWithSchema::new(
            Bson::String("test".to_string()),
            UniversalType::VarChar { length: 100 },
        );
        let tv = TypedValue::from(bv);
        assert!(matches!(
            tv.sync_type,
            UniversalType::VarChar { length: 100 }
        ));
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "test"));
    }

    #[test]
    fn test_binary_conversion() {
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0x01, 0x02, 0x03],
        };
        let bv = BsonValueWithSchema::new(Bson::Binary(bin), UniversalType::Bytes);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_from_binary() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: uuid.as_bytes().to_vec(),
        };
        let bv = BsonValueWithSchema::new(Bson::Binary(bin), UniversalType::Uuid);
        let tv = TypedValue::from(bv);
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u, uuid);
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_uuid_from_string() {
        let bv = BsonValueWithSchema::new(
            Bson::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
            UniversalType::Uuid,
        );
        let tv = TypedValue::from(bv);
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let bson_dt = BsonDateTime::from_chrono(dt);
        let bv = BsonValueWithSchema::new(Bson::DateTime(bson_dt), UniversalType::DateTime);
        let tv = TypedValue::from(bv);
        if let UniversalValue::DateTime(result_dt) = tv.value {
            assert_eq!(result_dt.year(), 2024);
            assert_eq!(result_dt.month(), 6);
            assert_eq!(result_dt.day(), 15);
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_date_from_string() {
        let bv =
            BsonValueWithSchema::new(Bson::String("2024-06-15".to_string()), UniversalType::Date);
        let tv = TypedValue::from(bv);
        if let UniversalValue::DateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let bv =
            BsonValueWithSchema::new(Bson::String("14:30:45".to_string()), UniversalType::Time);
        let tv = TypedValue::from(bv);
        if let UniversalValue::DateTime(dt) = tv.value {
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
        let bv = BsonValueWithSchema::new(Bson::Document(doc), UniversalType::Json);
        let tv = TypedValue::from(bv);
        if let UniversalValue::Json(json_val) = tv.value {
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
            UniversalType::Array {
                element_type: Box::new(UniversalType::Int),
            },
        );
        let tv = TypedValue::from(bv);
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], UniversalValue::Int(1)));
            assert!(matches!(elements[1], UniversalValue::Int(2)));
            assert!(matches!(elements[2], UniversalValue::Int(3)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let arr = vec![Bson::String("a".to_string()), Bson::String("b".to_string())];
        let bv = BsonValueWithSchema::new(
            Bson::Array(arr),
            UniversalType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(bv);
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let bv = BsonValueWithSchema::new(
            Bson::String("active".to_string()),
            UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_conversion() {
        let doc = doc! {
            "type": "Point",
            "coordinates": [-73.97, 40.77]
        };
        let bv = BsonValueWithSchema::new(
            Bson::Document(doc),
            UniversalType::Geometry {
                geometry_type: GeometryType::Point,
            },
        );
        let tv = TypedValue::from(bv);
        if let UniversalValue::Json(json_val) = tv.value {
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
        let name = extract_field(&doc, "name", &UniversalType::Text);
        assert!(matches!(name.value, UniversalValue::Text(ref s) if s == "Alice"));

        let age = extract_field(&doc, "age", &UniversalType::Int);
        assert!(matches!(age.value, UniversalValue::Int(30)));

        let missing = extract_field(&doc, "missing", &UniversalType::Text);
        assert!(matches!(missing.value, UniversalValue::Null));
    }

    #[test]
    fn test_document_to_typed_values() {
        let doc = doc! {
            "name": "Bob",
            "active": true,
            "score": 95.5
        };
        let schema = vec![
            ("name".to_string(), UniversalType::Text),
            ("active".to_string(), UniversalType::Bool),
            ("score".to_string(), UniversalType::Double),
        ];

        let values = document_to_typed_values(&doc, &schema);
        assert!(matches!(
            values.get("name").unwrap().value,
            UniversalValue::Text(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            UniversalValue::Bool(true)
        ));
        if let UniversalValue::Double(f) = values.get("score").unwrap().value {
            assert!((f - 95.5).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_int64_to_int_conversion() {
        // MongoDB might return Int64 when we expect Int32
        let bv = BsonValueWithSchema::new(Bson::Int64(100), UniversalType::Int);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::Int(100)));
    }

    #[test]
    fn test_int32_to_bigint_conversion() {
        // MongoDB might return Int32 when we expect Int64
        let bv = BsonValueWithSchema::new(Bson::Int32(100), UniversalType::BigInt);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, UniversalValue::BigInt(100)));
    }

    #[test]
    fn test_objectid_in_document() {
        let oid = bson::oid::ObjectId::new();
        let doc = doc! {
            "_id": oid
        };
        let map = bson_doc_to_hashmap(&doc);
        if let Some(UniversalValue::Text(s)) = map.get("_id") {
            assert_eq!(s, &oid.to_hex());
        } else {
            panic!("Expected String for ObjectId");
        }
    }
}
