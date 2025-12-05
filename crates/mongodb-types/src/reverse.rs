//! Reverse conversion: BSON value → TypedValue.
//!
//! This module provides conversion from MongoDB BSON values to sync-core's `TypedValue`.

use bson::Bson;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use sync_core::{GeneratedValue, SyncDataType, TypedValue};

/// BSON value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct BsonValueWithSchema {
    /// The BSON value from MongoDB.
    pub value: Bson,
    /// The expected sync type for conversion.
    pub sync_type: SyncDataType,
}

impl BsonValueWithSchema {
    /// Create a new BsonValueWithSchema.
    pub fn new(value: Bson, sync_type: SyncDataType) -> Self {
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
                value: GeneratedValue::Null,
            },

            // Boolean
            (SyncDataType::Bool, Bson::Boolean(b)) => TypedValue::bool(*b),

            // Integer types
            (SyncDataType::TinyInt { width }, Bson::Int32(i)) => TypedValue {
                sync_type: SyncDataType::TinyInt { width: *width },
                value: GeneratedValue::Int32(*i),
            },
            (SyncDataType::SmallInt, Bson::Int32(i)) => TypedValue::smallint(*i),
            (SyncDataType::Int, Bson::Int32(i)) => TypedValue::int(*i),
            (SyncDataType::BigInt, Bson::Int64(i)) => TypedValue::bigint(*i),
            // Handle Int64 for Int (MongoDB might return Int64)
            (SyncDataType::Int, Bson::Int64(i)) => TypedValue::int(*i as i32),
            // Handle Int32 for BigInt
            (SyncDataType::BigInt, Bson::Int32(i)) => TypedValue::bigint(*i as i64),

            // Floating point
            (SyncDataType::Float, Bson::Double(f)) => TypedValue::float(*f),
            (SyncDataType::Double, Bson::Double(f)) => TypedValue::double(*f),

            // Decimal - stored as string in MongoDB for precision
            (SyncDataType::Decimal { precision, scale }, Bson::String(s)) => {
                TypedValue::decimal(s, *precision, *scale)
            }
            // Also handle Decimal128 if present
            (SyncDataType::Decimal { precision, scale }, Bson::Decimal128(d)) => {
                TypedValue::decimal(d.to_string(), *precision, *scale)
            }

            // String types
            (SyncDataType::Char { length }, Bson::String(s)) => TypedValue {
                sync_type: SyncDataType::Char { length: *length },
                value: GeneratedValue::String(s.clone()),
            },
            (SyncDataType::VarChar { length }, Bson::String(s)) => TypedValue {
                sync_type: SyncDataType::VarChar { length: *length },
                value: GeneratedValue::String(s.clone()),
            },
            (SyncDataType::Text, Bson::String(s)) => TypedValue::text(s),

            // Binary types
            (SyncDataType::Blob, Bson::Binary(bin)) => TypedValue {
                sync_type: SyncDataType::Blob,
                value: GeneratedValue::Bytes(bin.bytes.clone()),
            },
            (SyncDataType::Bytes, Bson::Binary(bin)) => TypedValue::bytes(bin.bytes.clone()),

            // UUID
            (SyncDataType::Uuid, Bson::Binary(bin)) => {
                if bin.subtype == bson::spec::BinarySubtype::Uuid
                    || bin.subtype == bson::spec::BinarySubtype::UuidOld
                {
                    if let Ok(uuid) = uuid::Uuid::from_slice(&bin.bytes) {
                        TypedValue::uuid(uuid)
                    } else {
                        TypedValue::null(SyncDataType::Uuid)
                    }
                } else {
                    TypedValue::null(SyncDataType::Uuid)
                }
            }
            // UUID might also be stored as string
            (SyncDataType::Uuid, Bson::String(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(SyncDataType::Uuid)
                }
            }

            // Date/time types
            (SyncDataType::DateTime, Bson::DateTime(dt)) => TypedValue::datetime(dt.to_chrono()),
            (SyncDataType::DateTimeNano, Bson::DateTime(dt)) => TypedValue {
                sync_type: SyncDataType::DateTimeNano,
                value: GeneratedValue::DateTime(dt.to_chrono()),
            },
            (SyncDataType::TimestampTz, Bson::DateTime(dt)) => TypedValue {
                sync_type: SyncDataType::TimestampTz,
                value: GeneratedValue::DateTime(dt.to_chrono()),
            },

            // Date stored as string
            (SyncDataType::Date, Bson::String(s)) => {
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
            (SyncDataType::Time, Bson::String(s)) => {
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
            (SyncDataType::Json, Bson::Document(doc)) => {
                let map = bson_doc_to_hashmap(doc);
                TypedValue {
                    sync_type: SyncDataType::Json,
                    value: GeneratedValue::Object(map),
                }
            }
            (SyncDataType::Jsonb, Bson::Document(doc)) => {
                let map = bson_doc_to_hashmap(doc);
                TypedValue {
                    sync_type: SyncDataType::Jsonb,
                    value: GeneratedValue::Object(map),
                }
            }

            // Array types
            (SyncDataType::Array { element_type }, Bson::Array(arr)) => {
                let values: Vec<GeneratedValue> = arr
                    .iter()
                    .map(|v| {
                        let bv = BsonValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(bv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array in MongoDB
            (SyncDataType::Set { values: set_values }, Bson::Array(arr)) => {
                let values: Vec<GeneratedValue> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Bson::String(s) = v {
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
                Bson::String(s),
            ) => TypedValue {
                sync_type: SyncDataType::Enum {
                    values: enum_values.clone(),
                },
                value: GeneratedValue::String(s.clone()),
            },

            // Geometry types - stored as GeoJSON document
            (SyncDataType::Geometry { geometry_type }, Bson::Document(doc)) => {
                let map = bson_doc_to_hashmap(doc);
                TypedValue {
                    sync_type: SyncDataType::Geometry {
                        geometry_type: geometry_type.clone(),
                    },
                    value: GeneratedValue::Object(map),
                }
            }

            // Fallback for unhandled combinations
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a BSON document to a HashMap of GeneratedValue.
fn bson_doc_to_hashmap(doc: &bson::Document) -> HashMap<String, GeneratedValue> {
    let mut map = HashMap::new();
    for (key, value) in doc {
        map.insert(key.clone(), bson_to_generated_value(value));
    }
    map
}

/// Convert a BSON value to GeneratedValue (without type context).
fn bson_to_generated_value(value: &Bson) -> GeneratedValue {
    match value {
        Bson::Null => GeneratedValue::Null,
        Bson::Boolean(b) => GeneratedValue::Bool(*b),
        Bson::Int32(i) => GeneratedValue::Int32(*i),
        Bson::Int64(i) => GeneratedValue::Int64(*i),
        Bson::Double(f) => GeneratedValue::Float64(*f),
        Bson::String(s) => GeneratedValue::String(s.clone()),
        Bson::Binary(bin) => {
            if bin.subtype == bson::spec::BinarySubtype::Uuid
                || bin.subtype == bson::spec::BinarySubtype::UuidOld
            {
                if let Ok(uuid) = uuid::Uuid::from_slice(&bin.bytes) {
                    GeneratedValue::Uuid(uuid)
                } else {
                    GeneratedValue::Bytes(bin.bytes.clone())
                }
            } else {
                GeneratedValue::Bytes(bin.bytes.clone())
            }
        }
        Bson::DateTime(dt) => GeneratedValue::DateTime(dt.to_chrono()),
        Bson::Decimal128(d) => GeneratedValue::Decimal {
            value: d.to_string(),
            precision: 38,
            scale: 10,
        },
        Bson::Array(arr) => {
            GeneratedValue::Array(arr.iter().map(bson_to_generated_value).collect())
        }
        Bson::Document(doc) => GeneratedValue::Object(bson_doc_to_hashmap(doc)),
        // ObjectId - convert to string
        Bson::ObjectId(oid) => GeneratedValue::String(oid.to_hex()),
        // Timestamp - convert to DateTime
        Bson::Timestamp(ts) => {
            let secs = ts.time as i64;
            if let Some(dt) = DateTime::from_timestamp(secs, 0) {
                GeneratedValue::DateTime(dt)
            } else {
                GeneratedValue::Null
            }
        }
        // Regex - convert to string pattern
        Bson::RegularExpression(regex) => {
            GeneratedValue::String(format!("/{}/{}", regex.pattern, regex.options))
        }
        // JavaScript - convert to string
        Bson::JavaScriptCode(code) => GeneratedValue::String(code.clone()),
        Bson::JavaScriptCodeWithScope(code_scope) => {
            GeneratedValue::String(code_scope.code.clone())
        }
        // Symbol - convert to string
        Bson::Symbol(s) => GeneratedValue::String(s.clone()),
        // Undefined - treat as null
        Bson::Undefined => GeneratedValue::Null,
        // MinKey/MaxKey - convert to string
        Bson::MinKey => GeneratedValue::String("$minKey".to_string()),
        Bson::MaxKey => GeneratedValue::String("$maxKey".to_string()),
        // DbPointer is deprecated
        Bson::DbPointer(_) => GeneratedValue::Null,
    }
}

/// Extract a typed value from a BSON document field.
pub fn extract_field(doc: &bson::Document, field: &str, sync_type: &SyncDataType) -> TypedValue {
    match doc.get(field) {
        Some(value) => BsonValueWithSchema::new(value.clone(), sync_type.clone()).to_typed_value(),
        None => TypedValue::null(sync_type.clone()),
    }
}

/// Convert a complete BSON document to a map of TypedValues using schema.
pub fn document_to_typed_values(
    doc: &bson::Document,
    schema: &[(String, SyncDataType)],
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
        let bv = BsonValueWithSchema::new(Bson::Null, SyncDataType::Text);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Boolean(true), SyncDataType::Bool);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Bool(true)));
    }

    #[test]
    fn test_int32_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Int32(42), SyncDataType::Int);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Int32(42)));
    }

    #[test]
    fn test_int64_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Int64(9876543210), SyncDataType::BigInt);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Int64(9876543210)));
    }

    #[test]
    fn test_double_conversion() {
        let bv = BsonValueWithSchema::new(Bson::Double(1.23456), SyncDataType::Double);
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Float64(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_from_string() {
        let bv = BsonValueWithSchema::new(
            Bson::String("123.456".to_string()),
            SyncDataType::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(bv);
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
        let bv =
            BsonValueWithSchema::new(Bson::String("hello world".to_string()), SyncDataType::Text);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::String(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let bv = BsonValueWithSchema::new(
            Bson::String("test".to_string()),
            SyncDataType::VarChar { length: 100 },
        );
        let tv = TypedValue::from(bv);
        assert!(matches!(
            tv.sync_type,
            SyncDataType::VarChar { length: 100 }
        ));
        assert!(matches!(tv.value, GeneratedValue::String(ref s) if s == "test"));
    }

    #[test]
    fn test_binary_conversion() {
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0x01, 0x02, 0x03],
        };
        let bv = BsonValueWithSchema::new(Bson::Binary(bin), SyncDataType::Bytes);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_from_binary() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let bin = bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: uuid.as_bytes().to_vec(),
        };
        let bv = BsonValueWithSchema::new(Bson::Binary(bin), SyncDataType::Uuid);
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Uuid(u) = tv.value {
            assert_eq!(u, uuid);
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_uuid_from_string() {
        let bv = BsonValueWithSchema::new(
            Bson::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
            SyncDataType::Uuid,
        );
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let bson_dt = BsonDateTime::from_chrono(dt);
        let bv = BsonValueWithSchema::new(Bson::DateTime(bson_dt), SyncDataType::DateTime);
        let tv = TypedValue::from(bv);
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
        let bv =
            BsonValueWithSchema::new(Bson::String("2024-06-15".to_string()), SyncDataType::Date);
        let tv = TypedValue::from(bv);
        if let GeneratedValue::DateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let bv = BsonValueWithSchema::new(Bson::String("14:30:45".to_string()), SyncDataType::Time);
        let tv = TypedValue::from(bv);
        if let GeneratedValue::DateTime(dt) = tv.value {
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
        let bv = BsonValueWithSchema::new(Bson::Document(doc), SyncDataType::Json);
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Object(map) = tv.value {
            assert!(matches!(map.get("name"), Some(GeneratedValue::String(s)) if s == "test"));
            assert!(matches!(map.get("count"), Some(GeneratedValue::Int32(42))));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let arr = vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)];
        let bv = BsonValueWithSchema::new(
            Bson::Array(arr),
            SyncDataType::Array {
                element_type: Box::new(SyncDataType::Int),
            },
        );
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Array(values) = tv.value {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], GeneratedValue::Int32(1)));
            assert!(matches!(values[1], GeneratedValue::Int32(2)));
            assert!(matches!(values[2], GeneratedValue::Int32(3)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let arr = vec![Bson::String("a".to_string()), Bson::String("b".to_string())];
        let bv = BsonValueWithSchema::new(
            Bson::Array(arr),
            SyncDataType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Array(values) = tv.value {
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let bv = BsonValueWithSchema::new(
            Bson::String("active".to_string()),
            SyncDataType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::String(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_conversion() {
        let doc = doc! {
            "type": "Point",
            "coordinates": [-73.97, 40.77]
        };
        let bv = BsonValueWithSchema::new(
            Bson::Document(doc),
            SyncDataType::Geometry {
                geometry_type: GeometryType::Point,
            },
        );
        let tv = TypedValue::from(bv);
        if let GeneratedValue::Object(map) = tv.value {
            assert!(matches!(map.get("type"), Some(GeneratedValue::String(s)) if s == "Point"));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_extract_field() {
        let doc = doc! {
            "name": "Alice",
            "age": 30
        };
        let name = extract_field(&doc, "name", &SyncDataType::Text);
        assert!(matches!(name.value, GeneratedValue::String(ref s) if s == "Alice"));

        let age = extract_field(&doc, "age", &SyncDataType::Int);
        assert!(matches!(age.value, GeneratedValue::Int32(30)));

        let missing = extract_field(&doc, "missing", &SyncDataType::Text);
        assert!(matches!(missing.value, GeneratedValue::Null));
    }

    #[test]
    fn test_document_to_typed_values() {
        let doc = doc! {
            "name": "Bob",
            "active": true,
            "score": 95.5
        };
        let schema = vec![
            ("name".to_string(), SyncDataType::Text),
            ("active".to_string(), SyncDataType::Bool),
            ("score".to_string(), SyncDataType::Double),
        ];

        let values = document_to_typed_values(&doc, &schema);
        assert!(matches!(
            values.get("name").unwrap().value,
            GeneratedValue::String(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            GeneratedValue::Bool(true)
        ));
        if let GeneratedValue::Float64(f) = values.get("score").unwrap().value {
            assert!((f - 95.5).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_int64_to_int_conversion() {
        // MongoDB might return Int64 when we expect Int32
        let bv = BsonValueWithSchema::new(Bson::Int64(100), SyncDataType::Int);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Int32(100)));
    }

    #[test]
    fn test_int32_to_bigint_conversion() {
        // MongoDB might return Int32 when we expect Int64
        let bv = BsonValueWithSchema::new(Bson::Int32(100), SyncDataType::BigInt);
        let tv = TypedValue::from(bv);
        assert!(matches!(tv.value, GeneratedValue::Int64(100)));
    }

    #[test]
    fn test_objectid_in_document() {
        let oid = bson::oid::ObjectId::new();
        let doc = doc! {
            "_id": oid
        };
        let map = bson_doc_to_hashmap(&doc);
        if let Some(GeneratedValue::String(s)) = map.get("_id") {
            assert_eq!(s, &oid.to_hex());
        } else {
            panic!("Expected String for ObjectId");
        }
    }
}
