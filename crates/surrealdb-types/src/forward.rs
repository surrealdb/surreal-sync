//! Forward conversion: TypedValue → SurrealDB value.
//!
//! This module provides conversion from sync-core's `TypedValue` to SurrealDB values.

use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;
use surrealdb::sql::{Array, Datetime, Number, Object, Strand, Thing, Value};
use sync_core::{TypedValue, UniversalType, UniversalValue};

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
            // JSON can be a string that needs parsing
            (UniversalType::Json, UniversalValue::Text(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => SurrealValue(json_to_surreal(&v)),
                    Err(_) => SurrealValue(Value::Strand(Strand::from(s.clone()))),
                }
            }
            (UniversalType::Jsonb, UniversalValue::Text(s)) => {
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(v) => SurrealValue(json_to_surreal(&v)),
                    Err(_) => SurrealValue(Value::Strand(Strand::from(s.clone()))),
                }
            }
            // JSON can also be an array (e.g., from MySQL SET columns or JSON arrays)
            (UniversalType::Json, UniversalValue::Array { elements, .. }) => {
                let surreal_arr: Vec<Value> =
                    elements.iter().map(generated_value_to_surreal).collect();
                SurrealValue(Value::Array(surrealdb::sql::Array::from(surreal_arr)))
            }
            (UniversalType::Jsonb, UniversalValue::Array { elements, .. }) => {
                let surreal_arr: Vec<Value> =
                    elements.iter().map(generated_value_to_surreal).collect();
                SurrealValue(Value::Array(surrealdb::sql::Array::from(surreal_arr)))
            }

            // Array types need recursive conversion with element type info
            (UniversalType::Array { element_type }, UniversalValue::Array { elements, .. }) => {
                let surreal_arr: Vec<Value> = elements
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

            // For all other cases, delegate to From<UniversalValue>
            _ => SurrealValue::from(tv.value),
        }
    }
}

impl From<UniversalValue> for SurrealValue {
    fn from(value: UniversalValue) -> Self {
        match value {
            // Null
            UniversalValue::Null => SurrealValue(Value::None),

            // Boolean
            UniversalValue::Bool(b) => SurrealValue(Value::Bool(b)),

            // Integer types
            UniversalValue::TinyInt { value, .. } => {
                SurrealValue(Value::Number(Number::Int(value as i64)))
            }
            UniversalValue::SmallInt(i) => SurrealValue(Value::Number(Number::Int(i as i64))),
            UniversalValue::Int(i) => SurrealValue(Value::Number(Number::Int(i as i64))),
            UniversalValue::BigInt(i) => SurrealValue(Value::Number(Number::Int(i))),

            // Floating point
            UniversalValue::Float(f) => SurrealValue(Value::Number(Number::Float(f as f64))),
            UniversalValue::Double(f) => SurrealValue(Value::Number(Number::Float(f))),

            // Decimal
            UniversalValue::Decimal {
                value, precision, ..
            } => {
                if precision <= 38 {
                    match Decimal::from_str(&value) {
                        Ok(dec) => SurrealValue(Value::Number(Number::Decimal(dec))),
                        Err(_) => SurrealValue(Value::Strand(Strand::from(value))),
                    }
                } else {
                    // Store high precision as string
                    SurrealValue(Value::Strand(Strand::from(value)))
                }
            }

            // String types - auto-detect ISO 8601 duration strings
            UniversalValue::Char { value, .. } => {
                if let Some(duration) = try_parse_iso8601_duration(&value) {
                    SurrealValue(Value::Duration(surrealdb::sql::Duration::from(duration)))
                } else {
                    SurrealValue(Value::Strand(Strand::from(value)))
                }
            }
            UniversalValue::VarChar { value, .. } => {
                if let Some(duration) = try_parse_iso8601_duration(&value) {
                    SurrealValue(Value::Duration(surrealdb::sql::Duration::from(duration)))
                } else {
                    SurrealValue(Value::Strand(Strand::from(value)))
                }
            }
            UniversalValue::Text(s) => {
                // Auto-detect ISO 8601 duration strings (PTxxxS format) and convert to Duration
                if let Some(duration) = try_parse_iso8601_duration(&s) {
                    SurrealValue(Value::Duration(surrealdb::sql::Duration::from(duration)))
                } else {
                    SurrealValue(Value::Strand(Strand::from(s)))
                }
            }

            // Binary types
            UniversalValue::Blob(b) => SurrealValue(Value::Bytes(surrealdb::sql::Bytes::from(b))),
            UniversalValue::Bytes(b) => SurrealValue(Value::Bytes(surrealdb::sql::Bytes::from(b))),

            // Date/time types
            UniversalValue::Date(dt) => SurrealValue(Value::Strand(Strand::from(
                dt.format("%Y-%m-%d").to_string(),
            ))),
            UniversalValue::Time(dt) => SurrealValue(Value::Strand(Strand::from(
                dt.format("%H:%M:%S").to_string(),
            ))),
            UniversalValue::DateTime(dt) => SurrealValue(Value::Datetime(Datetime::from(dt))),
            UniversalValue::DateTimeNano(dt) => SurrealValue(Value::Datetime(Datetime::from(dt))),
            UniversalValue::TimestampTz(dt) => SurrealValue(Value::Datetime(Datetime::from(dt))),

            // UUID
            UniversalValue::Uuid(u) => SurrealValue(Value::Uuid(surrealdb::sql::Uuid::from(u))),

            // JSON types
            UniversalValue::Json(json_val) => SurrealValue(json_to_surreal(&json_val)),
            UniversalValue::Jsonb(json_val) => SurrealValue(json_to_surreal(&json_val)),

            // Array
            UniversalValue::Array { elements, .. } => {
                let surreal_arr: Vec<Value> = elements
                    .into_iter()
                    .map(|v| SurrealValue::from(v).into_inner())
                    .collect();
                SurrealValue(Value::Array(Array::from(surreal_arr)))
            }

            // Set - stored as array of strings
            UniversalValue::Set { elements, .. } => {
                let surreal_arr: Vec<Value> = elements
                    .into_iter()
                    .map(|s| Value::Strand(Strand::from(s)))
                    .collect();
                SurrealValue(Value::Array(Array::from(surreal_arr)))
            }

            // Enum - stored as string
            UniversalValue::Enum { value, .. } => SurrealValue(Value::Strand(Strand::from(value))),

            // Geometry - convert to JSON object
            UniversalValue::Geometry { data, .. } => {
                use sync_core::values::GeometryData;
                let GeometryData(json_val) = data;
                SurrealValue(json_to_surreal(&json_val))
            }

            // Duration - convert to SurrealDB Duration
            UniversalValue::Duration(d) => {
                SurrealValue(Value::Duration(surrealdb::sql::Duration::from(d)))
            }
        }
    }
}

/// Convert a UniversalValue to a SurrealDB Value (without type context).
/// This is a helper for internal use - prefer using `From<UniversalValue>` trait.
fn generated_value_to_surreal(value: &UniversalValue) -> Value {
    SurrealValue::from(value.clone()).into_inner()
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

/// Try to parse an ISO 8601 duration string (e.g., "PT181S" or "PT181.000000000S").
/// Returns Some(std::time::Duration) if the string matches the expected format.
fn try_parse_iso8601_duration(s: &str) -> Option<std::time::Duration> {
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

/// Create a SurrealDB Thing (record ID).
pub fn create_thing(table: &str, id: &UniversalValue) -> Option<Thing> {
    let id_part = match id {
        UniversalValue::Text(s) => surrealdb::sql::Id::String(s.clone()),
        UniversalValue::Int(i) => surrealdb::sql::Id::Number(*i as i64),
        UniversalValue::BigInt(i) => surrealdb::sql::Id::Number(*i),
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

/// Record type that uses native surrealdb::sql::Value instead of SurrealValue wrapper.
/// This is used by sync sources for direct SurrealDB insertion.
#[derive(Debug, Clone)]
pub struct RecordWithSurrealValues {
    pub id: Thing,
    pub data: std::collections::HashMap<String, Value>,
}

impl RecordWithSurrealValues {
    /// Create a new record with the given ID and data.
    pub fn new(id: Thing, data: std::collections::HashMap<String, Value>) -> Self {
        Self { id, data }
    }

    /// Get the upsert content as a SurrealDB Object.
    pub fn get_upsert_content(&self) -> Value {
        let mut m = BTreeMap::new();
        for (k, v) in &self.data {
            m.insert(k.clone(), v.clone());
        }
        Value::Object(Object::from(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};

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
        let tv = TypedValue::tinyint(127, 4);
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
        let tv = TypedValue::char_type("test", 10);
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
        let tv = TypedValue::date(dt);
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
        let tv = TypedValue::time(dt);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "14:30:45");
        } else {
            panic!("Expected Strand");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let json = serde_json::json!({
            "name": "test",
            "count": 42
        });

        let tv = TypedValue {
            sync_type: UniversalType::Json,
            value: UniversalValue::Json(Box::new(json)),
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
            value: UniversalValue::Text(r#"{"key": "value"}"#.to_string()),
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
                UniversalValue::Int(1),
                UniversalValue::Int(2),
                UniversalValue::Int(3),
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
                UniversalValue::Text("a".to_string()),
                UniversalValue::Text("b".to_string()),
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
        let tv = TypedValue::set(
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        let surreal_val: SurrealValue = tv.into();
        if let Value::Array(arr) = surreal_val.0 {
            assert_eq!(arr.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let tv =
            TypedValue::enum_type("active", vec!["active".to_string(), "inactive".to_string()]);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "active");
        } else {
            panic!("Expected Strand");
        }
    }

    #[test]
    fn test_geometry_point_conversion() {
        let json = serde_json::json!({
            "coordinates": [-73.97, 40.77]
        });

        let tv = TypedValue::geometry_geojson(json, sync_core::GeometryType::Point);
        let surreal_val: SurrealValue = tv.into();
        // With the simplified conversion, this will be a JSON object
        assert!(matches!(surreal_val.0, Value::Object(_)));
    }

    #[test]
    fn test_create_thing_string() {
        let id = UniversalValue::Text("user123".to_string());
        let thing = create_thing("users", &id);
        assert!(thing.is_some());
        let t = thing.unwrap();
        assert_eq!(t.tb, "users");
    }

    #[test]
    fn test_create_thing_int() {
        let id = UniversalValue::BigInt(42);
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

    #[test]
    fn test_text_with_iso8601_duration_converts_to_duration() {
        // Test that a Text value containing an ISO 8601 duration string
        // gets auto-detected and converted to a SurrealDB Duration
        let tv = TypedValue::text("PT181S");
        let surreal_val: SurrealValue = tv.into();
        if let Value::Duration(d) = surreal_val.0 {
            let std_duration: std::time::Duration = d.into();
            assert_eq!(std_duration.as_secs(), 181);
        } else {
            panic!("Expected Duration, got {:?}", surreal_val.0);
        }
    }

    #[test]
    fn test_varchar_with_iso8601_duration_converts_to_duration() {
        // Test that a VarChar value (like from MySQL) containing an ISO 8601 duration string
        // gets auto-detected and converted to a SurrealDB Duration
        let tv = TypedValue::varchar("PT181S", 64);
        let surreal_val: SurrealValue = tv.into();
        if let Value::Duration(d) = surreal_val.0 {
            let std_duration: std::time::Duration = d.into();
            assert_eq!(std_duration.as_secs(), 181);
        } else {
            panic!("Expected Duration, got {:?}", surreal_val.0);
        }
    }

    #[test]
    fn test_text_with_iso8601_duration_with_nanos() {
        // Test duration with nanoseconds: PT60.123456789S
        let tv = TypedValue::text("PT60.123456789S");
        let surreal_val: SurrealValue = tv.into();
        if let Value::Duration(d) = surreal_val.0 {
            let std_duration: std::time::Duration = d.into();
            assert_eq!(std_duration.as_secs(), 60);
            assert_eq!(std_duration.subsec_nanos(), 123456789);
        } else {
            panic!("Expected Duration, got {:?}", surreal_val.0);
        }
    }

    #[test]
    fn test_text_without_duration_pattern_stays_as_string() {
        // Regular text should remain as a string
        let tv = TypedValue::text("hello world");
        let surreal_val: SurrealValue = tv.into();
        if let Value::Strand(s) = surreal_val.0 {
            assert_eq!(s.as_str(), "hello world");
        } else {
            panic!("Expected Strand, got {:?}", surreal_val.0);
        }
    }
}
