//! Forward conversion: TypedValue → BSON value.
//!
//! This module provides conversion from sync-core's `TypedValue` to MongoDB BSON values.

use bson::{Bson, DateTime as BsonDateTime};
use sync_core::{GeneratedValue, GeometryType, SyncDataType, TypedValue};

/// Wrapper for BSON values that can be inserted into MongoDB.
#[derive(Debug, Clone)]
pub struct BsonValue(pub Bson);

impl BsonValue {
    /// Get the inner BSON value.
    pub fn into_inner(self) -> Bson {
        self.0
    }

    /// Get a reference to the inner BSON value.
    pub fn as_inner(&self) -> &Bson {
        &self.0
    }
}

impl From<TypedValue> for BsonValue {
    fn from(tv: TypedValue) -> Self {
        match (&tv.sync_type, &tv.value) {
            // Null
            (_, GeneratedValue::Null) => BsonValue(Bson::Null),

            // Boolean
            (SyncDataType::Bool, GeneratedValue::Bool(b)) => BsonValue(Bson::Boolean(*b)),

            // Integer types - MongoDB uses i32 and i64; handle both Int32 and Int64 from generators
            (SyncDataType::TinyInt { .. }, GeneratedValue::Int32(i)) => BsonValue(Bson::Int32(*i)),
            (SyncDataType::TinyInt { .. }, GeneratedValue::Int64(i)) => BsonValue(Bson::Int64(*i)),
            (SyncDataType::SmallInt, GeneratedValue::Int32(i)) => BsonValue(Bson::Int32(*i)),
            (SyncDataType::SmallInt, GeneratedValue::Int64(i)) => BsonValue(Bson::Int64(*i)),
            (SyncDataType::Int, GeneratedValue::Int32(i)) => BsonValue(Bson::Int32(*i)),
            (SyncDataType::Int, GeneratedValue::Int64(i)) => BsonValue(Bson::Int64(*i)),
            (SyncDataType::BigInt, GeneratedValue::Int64(i)) => BsonValue(Bson::Int64(*i)),
            (SyncDataType::BigInt, GeneratedValue::Int32(i)) => BsonValue(Bson::Int64(*i as i64)),

            // Floating point
            (SyncDataType::Float, GeneratedValue::Float64(f)) => BsonValue(Bson::Double(*f)),
            (SyncDataType::Double, GeneratedValue::Float64(f)) => BsonValue(Bson::Double(*f)),

            // Decimal - MongoDB has Decimal128, but we'll use string for precision
            (SyncDataType::Decimal { .. }, GeneratedValue::Decimal { value, .. }) => {
                // For high precision, store as string to avoid precision loss
                BsonValue(Bson::String(value.clone()))
            }

            // String types
            (SyncDataType::Char { .. }, GeneratedValue::String(s)) => {
                BsonValue(Bson::String(s.clone()))
            }
            (SyncDataType::VarChar { .. }, GeneratedValue::String(s)) => {
                BsonValue(Bson::String(s.clone()))
            }
            (SyncDataType::Text, GeneratedValue::String(s)) => BsonValue(Bson::String(s.clone())),

            // Binary types
            (SyncDataType::Blob, GeneratedValue::Bytes(b)) => {
                BsonValue(Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: b.clone(),
                }))
            }
            (SyncDataType::Bytes, GeneratedValue::Bytes(b)) => {
                BsonValue(Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: b.clone(),
                }))
            }

            // Date/time types
            (SyncDataType::Date, GeneratedValue::DateTime(dt)) => {
                // Store date as string in YYYY-MM-DD format
                BsonValue(Bson::String(dt.format("%Y-%m-%d").to_string()))
            }
            (SyncDataType::Time, GeneratedValue::DateTime(dt)) => {
                // Store time as string in HH:MM:SS format
                BsonValue(Bson::String(dt.format("%H:%M:%S").to_string()))
            }
            (SyncDataType::DateTime, GeneratedValue::DateTime(dt)) => {
                BsonValue(Bson::DateTime(BsonDateTime::from_chrono(*dt)))
            }
            (SyncDataType::DateTimeNano, GeneratedValue::DateTime(dt)) => {
                // MongoDB DateTime has millisecond precision, so we lose nanoseconds
                BsonValue(Bson::DateTime(BsonDateTime::from_chrono(*dt)))
            }
            (SyncDataType::TimestampTz, GeneratedValue::DateTime(dt)) => {
                BsonValue(Bson::DateTime(BsonDateTime::from_chrono(*dt)))
            }

            // UUID - MongoDB has native UUID binary subtype
            (SyncDataType::Uuid, GeneratedValue::Uuid(u)) => {
                BsonValue(Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Uuid,
                    bytes: u.as_bytes().to_vec(),
                }))
            }

            // JSON types - convert to BSON document
            (SyncDataType::Json, GeneratedValue::Object(map)) => {
                let doc = hashmap_to_bson_doc(map);
                BsonValue(Bson::Document(doc))
            }
            (SyncDataType::Jsonb, GeneratedValue::Object(map)) => {
                let doc = hashmap_to_bson_doc(map);
                BsonValue(Bson::Document(doc))
            }
            // JSON can also be a string
            (SyncDataType::Json, GeneratedValue::String(s)) => {
                // Try to parse as JSON, otherwise store as string
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => BsonValue(json_to_bson(&v)),
                    Err(_) => BsonValue(Bson::String(s.clone())),
                }
            }
            (SyncDataType::Jsonb, GeneratedValue::String(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => BsonValue(json_to_bson(&v)),
                    Err(_) => BsonValue(Bson::String(s.clone())),
                }
            }

            // Array types
            (SyncDataType::Array { element_type }, GeneratedValue::Array(arr)) => {
                let bson_arr: Vec<Bson> = arr
                    .iter()
                    .map(|v| {
                        let tv = TypedValue {
                            sync_type: (**element_type).clone(),
                            value: v.clone(),
                        };
                        BsonValue::from(tv).into_inner()
                    })
                    .collect();
                BsonValue(Bson::Array(bson_arr))
            }

            // Set - stored as array
            (SyncDataType::Set { .. }, GeneratedValue::Array(arr)) => {
                let bson_arr: Vec<Bson> = arr
                    .iter()
                    .map(|v| match v {
                        GeneratedValue::String(s) => Bson::String(s.clone()),
                        _ => Bson::Null,
                    })
                    .collect();
                BsonValue(Bson::Array(bson_arr))
            }

            // Enum - stored as string
            (SyncDataType::Enum { .. }, GeneratedValue::String(s)) => {
                BsonValue(Bson::String(s.clone()))
            }

            // Geometry types - store as GeoJSON
            (SyncDataType::Geometry { geometry_type }, GeneratedValue::Object(map)) => {
                let geojson_type = match geometry_type {
                    GeometryType::Point => "Point",
                    GeometryType::LineString => "LineString",
                    GeometryType::Polygon => "Polygon",
                    GeometryType::MultiPoint => "MultiPoint",
                    GeometryType::MultiLineString => "MultiLineString",
                    GeometryType::MultiPolygon => "MultiPolygon",
                    GeometryType::GeometryCollection => "GeometryCollection",
                };
                let mut doc = hashmap_to_bson_doc(map);
                doc.insert("type", geojson_type);
                BsonValue(Bson::Document(doc))
            }

            // Fallback - panic instead of silently returning null
            (sync_type, value) => panic!(
                "Unsupported type/value combination for BSON conversion: sync_type={sync_type:?}, value={value:?}"
            ),
        }
    }
}

/// Convert a HashMap of GeneratedValue to a BSON Document.
fn hashmap_to_bson_doc(map: &std::collections::HashMap<String, GeneratedValue>) -> bson::Document {
    let mut doc = bson::Document::new();
    for (key, value) in map {
        let bson_val = generated_value_to_bson(value);
        doc.insert(key.clone(), bson_val);
    }
    doc
}

/// Convert a GeneratedValue to a BSON value (without type context).
fn generated_value_to_bson(value: &GeneratedValue) -> Bson {
    match value {
        GeneratedValue::Null => Bson::Null,
        GeneratedValue::Bool(b) => Bson::Boolean(*b),
        GeneratedValue::Int32(i) => Bson::Int32(*i),
        GeneratedValue::Int64(i) => Bson::Int64(*i),
        GeneratedValue::Float64(f) => Bson::Double(*f),
        GeneratedValue::String(s) => Bson::String(s.clone()),
        GeneratedValue::Bytes(b) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b.clone(),
        }),
        GeneratedValue::Uuid(u) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: u.as_bytes().to_vec(),
        }),
        GeneratedValue::DateTime(dt) => Bson::DateTime(BsonDateTime::from_chrono(*dt)),
        GeneratedValue::Decimal { value, .. } => Bson::String(value.clone()),
        GeneratedValue::Array(arr) => {
            Bson::Array(arr.iter().map(generated_value_to_bson).collect())
        }
        GeneratedValue::Object(map) => Bson::Document(hashmap_to_bson_doc(map)),
    }
}

/// Convert a serde_json::Value to BSON.
fn json_to_bson(value: &serde_json::Value) -> Bson {
    match value {
        serde_json::Value::Null => Bson::Null,
        serde_json::Value::Bool(b) => Bson::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Int64(i)
                }
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        serde_json::Value::String(s) => Bson::String(s.clone()),
        serde_json::Value::Array(arr) => Bson::Array(arr.iter().map(json_to_bson).collect()),
        serde_json::Value::Object(map) => {
            let mut doc = bson::Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), json_to_bson(v));
            }
            Bson::Document(doc)
        }
    }
}

/// Convert a BSON Document to a Vec of BsonValue for insertion.
pub fn document_to_values(doc: bson::Document) -> Vec<(String, BsonValue)> {
    doc.into_iter().map(|(k, v)| (k, BsonValue(v))).collect()
}

/// Create a BSON document from TypedValues.
pub fn typed_values_to_document<I>(fields: I) -> bson::Document
where
    I: IntoIterator<Item = (String, TypedValue)>,
{
    let mut doc = bson::Document::new();
    for (name, tv) in fields {
        doc.insert(name, BsonValue::from(tv).into_inner());
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};
    use std::collections::HashMap;

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(SyncDataType::Text);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Boolean(true)));

        let tv = TypedValue::bool(false);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Boolean(false)));
    }

    #[test]
    fn test_tinyint_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::TinyInt { width: 4 },
            value: GeneratedValue::Int32(127),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int32(127)));
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::smallint(32000);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int32(32000)));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int(12345);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int32(12345)));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::bigint(9876543210);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float(1.234);
        let bson_val: BsonValue = tv.into();
        if let Bson::Double(f) = bson_val.0 {
            assert!((f - 1.234).abs() < 0.0001);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::double(1.23456);
        let bson_val: BsonValue = tv.into();
        if let Bson::Double(f) = bson_val.0 {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.45", 10, 2);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "123.45"));
    }

    #[test]
    fn test_char_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Char { length: 10 },
            value: GeneratedValue::String("test".to_string()),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "test"));
    }

    #[test]
    fn test_varchar_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::VarChar { length: 255 },
            value: GeneratedValue::String("hello world".to_string()),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "hello world"));
    }

    #[test]
    fn test_text_conversion() {
        let tv = TypedValue::text("long text content");
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "long text content"));
    }

    #[test]
    fn test_blob_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Blob,
            value: GeneratedValue::Bytes(vec![0x01, 0x02, 0x03]),
        };
        let bson_val: BsonValue = tv.into();
        if let Bson::Binary(bin) = bson_val.0 {
            assert_eq!(bin.bytes, vec![0x01, 0x02, 0x03]);
            assert_eq!(bin.subtype, bson::spec::BinarySubtype::Generic);
        } else {
            panic!("Expected Binary");
        }
    }

    #[test]
    fn test_bytes_conversion() {
        let tv = TypedValue::bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let bson_val: BsonValue = tv.into();
        if let Bson::Binary(bin) = bson_val.0 {
            assert_eq!(bin.bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("Expected Binary");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(u);
        let bson_val: BsonValue = tv.into();
        if let Bson::Binary(bin) = bson_val.0 {
            assert_eq!(bin.subtype, bson::spec::BinarySubtype::Uuid);
            assert_eq!(bin.bytes, u.as_bytes().to_vec());
        } else {
            panic!("Expected Binary with UUID subtype");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let tv = TypedValue::datetime(dt);
        let bson_val: BsonValue = tv.into();
        if let Bson::DateTime(bson_dt) = bson_val.0 {
            let converted = bson_dt.to_chrono();
            assert_eq!(converted.year(), 2024);
            assert_eq!(converted.month(), 6);
            assert_eq!(converted.day(), 15);
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_date_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap();
        let tv = TypedValue {
            sync_type: SyncDataType::Date,
            value: GeneratedValue::DateTime(dt),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "2024-06-15"));
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue {
            sync_type: SyncDataType::Time,
            value: GeneratedValue::DateTime(dt),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "14:30:45"));
    }

    #[test]
    fn test_json_object_conversion() {
        let mut map = HashMap::new();
        map.insert(
            "name".to_string(),
            GeneratedValue::String("test".to_string()),
        );
        map.insert("count".to_string(), GeneratedValue::Int32(42));

        let tv = TypedValue {
            sync_type: SyncDataType::Json,
            value: GeneratedValue::Object(map),
        };
        let bson_val: BsonValue = tv.into();
        if let Bson::Document(doc) = bson_val.0 {
            assert_eq!(doc.get_str("name").unwrap(), "test");
            assert_eq!(doc.get_i32("count").unwrap(), 42);
        } else {
            panic!("Expected Document");
        }
    }

    #[test]
    fn test_json_string_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Json,
            value: GeneratedValue::String(r#"{"key": "value"}"#.to_string()),
        };
        let bson_val: BsonValue = tv.into();
        if let Bson::Document(doc) = bson_val.0 {
            assert_eq!(doc.get_str("key").unwrap(), "value");
        } else {
            panic!("Expected Document");
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let tv = TypedValue::array(
            vec![
                GeneratedValue::Int32(1),
                GeneratedValue::Int32(2),
                GeneratedValue::Int32(3),
            ],
            SyncDataType::Int,
        );
        let bson_val: BsonValue = tv.into();
        if let Bson::Array(arr) = bson_val.0 {
            assert_eq!(arr.len(), 3);
            assert!(matches!(arr[0], Bson::Int32(1)));
            assert!(matches!(arr[1], Bson::Int32(2)));
            assert!(matches!(arr[2], Bson::Int32(3)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_array_text_conversion() {
        let tv = TypedValue::array(
            vec![
                GeneratedValue::String("a".to_string()),
                GeneratedValue::String("b".to_string()),
            ],
            SyncDataType::Text,
        );
        let bson_val: BsonValue = tv.into();
        if let Bson::Array(arr) = bson_val.0 {
            assert_eq!(arr.len(), 2);
            assert!(matches!(&arr[0], Bson::String(s) if s == "a"));
            assert!(matches!(&arr[1], Bson::String(s) if s == "b"));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            value: GeneratedValue::Array(vec![
                GeneratedValue::String("a".to_string()),
                GeneratedValue::String("b".to_string()),
            ]),
        };
        let bson_val: BsonValue = tv.into();
        if let Bson::Array(arr) = bson_val.0 {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let tv = TypedValue {
            sync_type: SyncDataType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
            value: GeneratedValue::String("active".to_string()),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_point_conversion() {
        let mut coords = HashMap::new();
        coords.insert(
            "coordinates".to_string(),
            GeneratedValue::Array(vec![
                GeneratedValue::Float64(-73.97),
                GeneratedValue::Float64(40.77),
            ]),
        );

        let tv = TypedValue {
            sync_type: SyncDataType::Geometry {
                geometry_type: GeometryType::Point,
            },
            value: GeneratedValue::Object(coords),
        };
        let bson_val: BsonValue = tv.into();
        if let Bson::Document(doc) = bson_val.0 {
            assert_eq!(doc.get_str("type").unwrap(), "Point");
            assert!(doc.get_array("coordinates").is_ok());
        } else {
            panic!("Expected Document");
        }
    }

    #[test]
    fn test_typed_values_to_document() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Alice")),
            ("age".to_string(), TypedValue::int(30)),
            ("active".to_string(), TypedValue::bool(true)),
        ];

        let doc = typed_values_to_document(fields);
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
        assert_eq!(doc.get_i32("age").unwrap(), 30);
        assert!(doc.get_bool("active").unwrap());
    }

    #[test]
    fn test_timestamptz_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 12, 25, 15, 30, 0).unwrap();
        let tv = TypedValue {
            sync_type: SyncDataType::TimestampTz,
            value: GeneratedValue::DateTime(dt),
        };
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::DateTime(_)));
    }
}
