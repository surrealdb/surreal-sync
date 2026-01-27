//! Forward conversion: TypedValue → BSON value.
//!
//! This module provides conversion from sync-core's `TypedValue` to MongoDB BSON values.

use bson::{Bson, DateTime as BsonDateTime};
use sync_core::values::GeometryData;
use sync_core::{GeometryType, TypedValue, UniversalType, UniversalValue};

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
            // Special case: JSON/JSONB can be stored as Text and needs parsing
            (UniversalType::Json, UniversalValue::Text(s)) => {
                // Try to parse as JSON, otherwise store as string
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => BsonValue(serde_json_to_bson(&v)),
                    Err(_) => BsonValue(Bson::String(s.clone())),
                }
            }
            (UniversalType::Jsonb, UniversalValue::Text(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => BsonValue(serde_json_to_bson(&v)),
                    Err(_) => BsonValue(Bson::String(s.clone())),
                }
            }

            // Special case: Arrays with element_type need typed conversion
            (UniversalType::Array { element_type }, UniversalValue::Array { elements, .. }) => {
                let bson_arr: Vec<Bson> = elements
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

            // All other cases delegate to From<UniversalValue>
            _ => BsonValue::from(tv.value),
        }
    }
}

impl From<UniversalValue> for BsonValue {
    fn from(value: UniversalValue) -> Self {
        match value {
            // Null
            UniversalValue::Null => BsonValue(Bson::Null),

            // Boolean
            UniversalValue::Bool(b) => BsonValue(Bson::Boolean(b)),

            // Integer types - MongoDB uses i32 and i64
            UniversalValue::Int8 { value, .. } => BsonValue(Bson::Int32(value as i32)),
            UniversalValue::Int16(i) => BsonValue(Bson::Int32(i as i32)),
            UniversalValue::Int32(i) => BsonValue(Bson::Int32(i)),
            UniversalValue::Int64(i) => BsonValue(Bson::Int64(i)),

            // Floating point
            UniversalValue::Float32(f) => BsonValue(Bson::Double(f as f64)),
            UniversalValue::Float64(f) => BsonValue(Bson::Double(f)),

            // Decimal - MongoDB has Decimal128, but we'll use string for precision
            UniversalValue::Decimal { value, .. } => BsonValue(Bson::String(value)),

            // String types
            UniversalValue::Char { value, .. } => BsonValue(Bson::String(value)),
            UniversalValue::VarChar { value, .. } => BsonValue(Bson::String(value)),
            UniversalValue::Text(s) => BsonValue(Bson::String(s)),

            // Binary types
            UniversalValue::Blob(b) => BsonValue(Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b,
            })),
            UniversalValue::Bytes(b) => BsonValue(Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b,
            })),

            // Date/time types
            UniversalValue::Date(dt) => {
                // Store date as string in YYYY-MM-DD format
                BsonValue(Bson::String(dt.format("%Y-%m-%d").to_string()))
            }
            UniversalValue::Time(dt) => {
                // Store time as string in HH:MM:SS format
                BsonValue(Bson::String(dt.format("%H:%M:%S").to_string()))
            }
            UniversalValue::LocalDateTime(dt) => {
                BsonValue(Bson::DateTime(BsonDateTime::from_chrono(dt)))
            }
            UniversalValue::LocalDateTimeNano(dt) => {
                // MongoDB DateTime has millisecond precision, so we lose nanoseconds
                BsonValue(Bson::DateTime(BsonDateTime::from_chrono(dt)))
            }
            UniversalValue::ZonedDateTime(dt) => {
                BsonValue(Bson::DateTime(BsonDateTime::from_chrono(dt)))
            }

            // UUID - MongoDB has native UUID binary subtype
            UniversalValue::Uuid(u) => BsonValue(Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: u.as_bytes().to_vec(),
            })),

            // ULID - store as string in MongoDB
            UniversalValue::Ulid(u) => BsonValue(Bson::String(u.to_string())),

            // JSON types - convert to BSON document
            UniversalValue::Json(json_val) => {
                let bson_val = json_value_to_bson(&json_val);
                BsonValue(bson_val)
            }
            UniversalValue::Jsonb(json_val) => {
                let bson_val = json_value_to_bson(&json_val);
                BsonValue(bson_val)
            }

            // Array types - recursively convert elements
            UniversalValue::Array { elements, .. } => {
                let bson_arr: Vec<Bson> = elements
                    .into_iter()
                    .map(|v| BsonValue::from(v).into_inner())
                    .collect();
                BsonValue(Bson::Array(bson_arr))
            }

            // Set - stored as array
            UniversalValue::Set { elements, .. } => {
                let bson_arr: Vec<Bson> = elements.into_iter().map(Bson::String).collect();
                BsonValue(Bson::Array(bson_arr))
            }

            // Enum - stored as string
            UniversalValue::Enum { value, .. } => BsonValue(Bson::String(value)),

            // Geometry types - store as GeoJSON with type field
            UniversalValue::Geometry {
                data,
                geometry_type,
            } => {
                let geojson_type = match geometry_type {
                    GeometryType::Point => "Point",
                    GeometryType::LineString => "LineString",
                    GeometryType::Polygon => "Polygon",
                    GeometryType::MultiPoint => "MultiPoint",
                    GeometryType::MultiLineString => "MultiLineString",
                    GeometryType::MultiPolygon => "MultiPolygon",
                    GeometryType::GeometryCollection => "GeometryCollection",
                };
                let GeometryData(json_val) = data;
                let mut bson_val = json_value_to_bson(&json_val);
                if let Bson::Document(ref mut doc) = bson_val {
                    doc.insert("type", geojson_type);
                }
                BsonValue(bson_val)
            }

            // Duration - store as ISO 8601 duration string
            UniversalValue::Duration(d) => {
                let secs = d.as_secs();
                let nanos = d.subsec_nanos();
                if nanos == 0 {
                    BsonValue(Bson::String(format!("PT{secs}S")))
                } else {
                    BsonValue(Bson::String(format!("PT{secs}.{nanos:09}S")))
                }
            }

            // Thing - record reference as "table:id" format
            UniversalValue::Thing { table, id } => {
                let id_str = match id.as_ref() {
                    UniversalValue::Text(s) => s.clone(),
                    UniversalValue::Int32(i) => i.to_string(),
                    UniversalValue::Int64(i) => i.to_string(),
                    UniversalValue::Uuid(u) => u.to_string(),
                    other => panic!(
                        "Unsupported Thing ID type: {other:?}. \
                         Supported types: Text, Int32, Int64, Uuid"
                    ),
                };
                BsonValue(Bson::String(format!("{table}:{id_str}")))
            }

            // Object - convert to BSON document
            UniversalValue::Object(map) => {
                let mut doc = bson::Document::new();
                for (k, v) in map {
                    doc.insert(k, BsonValue::from(v).into_inner());
                }
                BsonValue(Bson::Document(doc))
            }
        }
    }
}

/// Convert a serde_json::Value to a BSON value.
fn json_value_to_bson(json: &serde_json::Value) -> Bson {
    match json {
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
                Bson::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Bson::String(s.clone()),
        serde_json::Value::Array(arr) => Bson::Array(arr.iter().map(json_value_to_bson).collect()),
        serde_json::Value::Object(obj) => Bson::Document(json_object_to_bson_doc(obj)),
    }
}

/// Convert a serde_json::Map to a BSON Document.
fn json_object_to_bson_doc(map: &serde_json::Map<String, serde_json::Value>) -> bson::Document {
    let mut doc = bson::Document::new();
    for (key, value) in map {
        doc.insert(key.clone(), json_value_to_bson(value));
    }
    doc
}

/// Convert a UniversalValue to a BSON value (without type context).
#[allow(dead_code)]
fn generated_value_to_bson(value: &UniversalValue) -> Bson {
    match value {
        UniversalValue::Null => Bson::Null,
        UniversalValue::Bool(b) => Bson::Boolean(*b),
        UniversalValue::Int8 { value, .. } => Bson::Int32(*value as i32),
        UniversalValue::Int16(i) => Bson::Int32(*i as i32),
        UniversalValue::Int32(i) => Bson::Int32(*i),
        UniversalValue::Int64(i) => Bson::Int64(*i),
        UniversalValue::Float32(f) => Bson::Double(*f as f64),
        UniversalValue::Float64(f) => Bson::Double(*f),
        UniversalValue::Char { value, .. } => Bson::String(value.clone()),
        UniversalValue::VarChar { value, .. } => Bson::String(value.clone()),
        UniversalValue::Text(s) => Bson::String(s.clone()),
        UniversalValue::Blob(b) | UniversalValue::Bytes(b) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: b.clone(),
        }),
        UniversalValue::Uuid(u) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: u.as_bytes().to_vec(),
        }),
        UniversalValue::Ulid(u) => Bson::String(u.to_string()),
        UniversalValue::Date(dt)
        | UniversalValue::Time(dt)
        | UniversalValue::LocalDateTime(dt)
        | UniversalValue::LocalDateTimeNano(dt)
        | UniversalValue::ZonedDateTime(dt) => Bson::DateTime(BsonDateTime::from_chrono(*dt)),
        UniversalValue::Decimal { value, .. } => Bson::String(value.clone()),
        UniversalValue::Array { elements, .. } => {
            Bson::Array(elements.iter().map(generated_value_to_bson).collect())
        }
        UniversalValue::Json(json_val) | UniversalValue::Jsonb(json_val) => {
            json_value_to_bson(json_val)
        }
        UniversalValue::Set { elements, .. } => {
            Bson::Array(elements.iter().map(|s| Bson::String(s.clone())).collect())
        }
        UniversalValue::Enum { value, .. } => Bson::String(value.clone()),
        UniversalValue::Geometry { data, .. } => {
            use sync_core::values::GeometryData;
            let GeometryData(json_val) = data;
            serde_json_to_bson(json_val)
        }
        UniversalValue::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                Bson::String(format!("PT{secs}S"))
            } else {
                Bson::String(format!("PT{secs}.{nanos:09}S"))
            }
        }
        UniversalValue::Thing { table, id } => {
            let id_str = match id.as_ref() {
                UniversalValue::Text(s) => s.clone(),
                UniversalValue::Int32(i) => i.to_string(),
                UniversalValue::Int64(i) => i.to_string(),
                UniversalValue::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for MongoDB: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            Bson::String(format!("{table}:{id_str}"))
        }
        UniversalValue::Object(map) => {
            let mut doc = bson::Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), generated_value_to_bson(v));
            }
            Bson::Document(doc)
        }
    }
}

/// Convert a serde_json::Value to BSON.
fn serde_json_to_bson(value: &serde_json::Value) -> Bson {
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
        serde_json::Value::Array(arr) => Bson::Array(arr.iter().map(serde_json_to_bson).collect()),
        serde_json::Value::Object(map) => {
            let mut doc = bson::Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), serde_json_to_bson(v));
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

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(UniversalType::Text);
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
        let tv = TypedValue::int8(127, 4);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int32(127)));
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::int16(32000);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int32(32000)));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int32(12345);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int32(12345)));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9876543210);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.234);
        let bson_val: BsonValue = tv.into();
        if let Bson::Double(f) = bson_val.0 {
            assert!((f - 1.234).abs() < 0.001);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::float64(1.23456);
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
        let tv = TypedValue::char_type("test", 10);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "test"));
    }

    #[test]
    fn test_varchar_conversion() {
        let tv = TypedValue::varchar("hello world", 255);
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
        let tv = TypedValue::blob(vec![0x01, 0x02, 0x03]);
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
        let tv = TypedValue::date(dt);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "2024-06-15"));
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue::time(dt);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "14:30:45"));
    }

    #[test]
    fn test_json_object_conversion() {
        let mut map = serde_json::Map::new();
        map.insert(
            "name".to_string(),
            serde_json::Value::String("test".to_string()),
        );
        map.insert(
            "count".to_string(),
            serde_json::Value::Number(serde_json::Number::from(42)),
        );

        let tv = TypedValue::json(serde_json::Value::Object(map));
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
            sync_type: UniversalType::Json,
            value: UniversalValue::Text(r#"{"key": "value"}"#.to_string()),
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
                UniversalValue::Int32(1),
                UniversalValue::Int32(2),
                UniversalValue::Int32(3),
            ],
            UniversalType::Int32,
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
                UniversalValue::Text("a".to_string()),
                UniversalValue::Text("b".to_string()),
            ],
            UniversalType::Text,
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
        let tv = TypedValue::set(
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        let bson_val: BsonValue = tv.into();
        if let Bson::Array(arr) = bson_val.0 {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let tv =
            TypedValue::enum_type("active", vec!["active".to_string(), "inactive".to_string()]);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::String(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_point_conversion() {
        let mut coords = serde_json::Map::new();
        coords.insert(
            "coordinates".to_string(),
            serde_json::Value::Array(vec![
                serde_json::Value::Number(serde_json::Number::from_f64(-73.97).unwrap()),
                serde_json::Value::Number(serde_json::Number::from_f64(40.77).unwrap()),
            ]),
        );

        let tv =
            TypedValue::geometry_geojson(serde_json::Value::Object(coords), GeometryType::Point);
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
            ("age".to_string(), TypedValue::int32(30)),
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
        let tv = TypedValue::timestamptz(dt);
        let bson_val: BsonValue = tv.into();
        assert!(matches!(bson_val.0, Bson::DateTime(_)));
    }
}
