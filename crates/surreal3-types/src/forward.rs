//! Forward conversion: TypedValue → SurrealDB value.
//!
//! This module provides conversion from sync-core's `TypedValue` to SurrealDB values.

use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;
use surrealdb::types::{Array, Datetime, Number, Object, RecordId, RecordIdKey, Value as DbValue};
use sync_core::{Type, TypedValue, Value};

/// Wrapper for SurrealDB values.
#[derive(Debug, Clone)]
pub struct SurrealValue(pub DbValue);

impl SurrealValue {
    /// Get the inner SurrealDB value.
    pub fn into_inner(self) -> DbValue {
        self.0
    }

    /// Get a reference to the inner SurrealDB value.
    pub fn as_inner(&self) -> &DbValue {
        &self.0
    }

    /// Convert a zero-temporal sentinel using the given sink policy.
    pub fn from_zero_temporal(
        intended_type: &Type,
        source: Option<&str>,
        policy: sync_core::ZeroTemporalPolicy,
    ) -> Self {
        match policy {
            sync_core::ZeroTemporalPolicy::None => SurrealValue(DbValue::None),
            sync_core::ZeroTemporalPolicy::Null => SurrealValue(DbValue::Null),
            sync_core::ZeroTemporalPolicy::String => {
                let s = source
                    .unwrap_or_else(|| Value::canonical_zero_literal(intended_type))
                    .to_string();
                SurrealValue(DbValue::String(s))
            }
        }
    }

    /// Convert a [`Value`] with an explicit zero-temporal policy.
    pub fn from_universal_with_policy(value: Value, policy: sync_core::ZeroTemporalPolicy) -> Self {
        match value {
            Value::ZeroTemporal {
                intended_type,
                source,
            } => Self::from_zero_temporal(&intended_type, source.as_deref(), policy),
            Value::Array { elements, .. } => {
                let surreal_arr: Vec<DbValue> = elements
                    .into_iter()
                    .map(|v| Self::from_universal_with_policy(v, policy).into_inner())
                    .collect();
                SurrealValue(DbValue::Array(Array::from(surreal_arr)))
            }
            Value::Object(map) => {
                let mut obj = BTreeMap::new();
                for (k, v) in map {
                    obj.insert(k, Self::from_universal_with_policy(v, policy).into_inner());
                }
                SurrealValue(DbValue::Object(Object::from(obj)))
            }
            other => SurrealValue::from(other),
        }
    }

    /// Convert a [`TypedValue`] with an explicit zero-temporal policy.
    pub fn from_typed_with_policy(tv: TypedValue, policy: sync_core::ZeroTemporalPolicy) -> Self {
        match (&tv.sync_type, &tv.value) {
            // JSON can be a string that needs parsing
            (Type::Json, Value::Text(s)) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => SurrealValue(json_to_surreal(&v)),
                Err(_) => SurrealValue(DbValue::String(s.clone())),
            },
            (Type::Jsonb, Value::Text(s)) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(v) => SurrealValue(json_to_surreal(&v)),
                Err(_) => SurrealValue(DbValue::String(s.clone())),
            },
            // JSON can also be an array (e.g., from MySQL SET columns or JSON arrays)
            (Type::Json, Value::Array { elements, .. }) => {
                let surreal_arr: Vec<DbValue> =
                    elements.iter().map(generated_value_to_surreal).collect();
                SurrealValue(DbValue::Array(Array::from(surreal_arr)))
            }
            (Type::Jsonb, Value::Array { elements, .. }) => {
                let surreal_arr: Vec<DbValue> =
                    elements.iter().map(generated_value_to_surreal).collect();
                SurrealValue(DbValue::Array(Array::from(surreal_arr)))
            }

            // Array types need recursive conversion with element type info
            (Type::Array { element_type }, Value::Array { elements, .. }) => {
                let surreal_arr: Vec<DbValue> = elements
                    .iter()
                    .map(|v| {
                        let tv = TypedValue {
                            sync_type: (**element_type).clone(),
                            value: v.clone(),
                        };
                        Self::from_typed_with_policy(tv, policy).into_inner()
                    })
                    .collect();
                SurrealValue(DbValue::Array(Array::from(surreal_arr)))
            }

            // For all other cases, delegate to policy-aware Value conversion
            _ => Self::from_universal_with_policy(tv.value, policy),
        }
    }
}

impl From<TypedValue> for SurrealValue {
    fn from(tv: TypedValue) -> Self {
        Self::from_typed_with_policy(tv, sync_core::ZeroTemporalPolicy::default())
    }
}

impl From<Value> for SurrealValue {
    fn from(value: Value) -> Self {
        match value {
            // Null
            Value::Null => SurrealValue(DbValue::None),

            // Boolean
            Value::Bool(b) => SurrealValue(DbValue::Bool(b)),

            // Integer types
            Value::Int8 { value, .. } => SurrealValue(DbValue::Number(Number::Int(value as i64))),
            Value::Int16(i) => SurrealValue(DbValue::Number(Number::Int(i as i64))),
            Value::Int32(i) => SurrealValue(DbValue::Number(Number::Int(i as i64))),
            Value::Int64(i) => SurrealValue(DbValue::Number(Number::Int(i))),

            // Floating point
            Value::Float32(f) => SurrealValue(DbValue::Number(Number::Float(f as f64))),
            Value::Float64(f) => SurrealValue(DbValue::Number(Number::Float(f))),

            // Decimal - try to parse as rust_decimal, fallback to float
            Value::Decimal { value, .. } => {
                match Decimal::from_str(&value) {
                    Ok(dec) => SurrealValue(DbValue::Number(Number::Decimal(dec))),
                    Err(_) => {
                        // Can't fit in rust_decimal - convert to f64
                        // TODO: Add option to store as String if user requests high precision preservation
                        let f: f64 = value.parse().unwrap_or(0.0);
                        SurrealValue(DbValue::Number(Number::Float(f)))
                    }
                }
            }

            // String types - auto-detect ISO 8601 duration strings
            Value::Char { value, .. } => {
                if let Some(duration) = try_parse_iso8601_duration(&value) {
                    SurrealValue(DbValue::Duration(surrealdb::types::Duration::from(
                        duration,
                    )))
                } else {
                    SurrealValue(DbValue::String(value))
                }
            }
            Value::VarChar { value, .. } => {
                if let Some(duration) = try_parse_iso8601_duration(&value) {
                    SurrealValue(DbValue::Duration(surrealdb::types::Duration::from(
                        duration,
                    )))
                } else {
                    SurrealValue(DbValue::String(value))
                }
            }
            Value::Text(s) => {
                // Auto-detect ISO 8601 duration strings (PTxxxS format) and convert to Duration
                if let Some(duration) = try_parse_iso8601_duration(&s) {
                    SurrealValue(DbValue::Duration(surrealdb::types::Duration::from(
                        duration,
                    )))
                } else {
                    SurrealValue(DbValue::String(s))
                }
            }

            // Binary types
            Value::Blob(b) => SurrealValue(DbValue::Bytes(surrealdb::types::Bytes::from(b))),
            Value::Bytes(b) => SurrealValue(DbValue::Bytes(surrealdb::types::Bytes::from(b))),

            // Date/time types
            Value::Date(dt) => SurrealValue(DbValue::String(dt.format("%Y-%m-%d").to_string())),
            // Note: Using "%H:%M:%S%.f" to preserve fractional seconds from PostgreSQL TIME type.
            // While SurrealDB doesn't have a native TIME type, storing as string with full precision
            // ensures no data loss during sync.
            Value::Time(dt) => SurrealValue(DbValue::String(dt.format("%H:%M:%S%.f").to_string())),
            Value::LocalDateTime(dt) => SurrealValue(DbValue::Datetime(Datetime::from(dt))),
            Value::LocalDateTimeNano(dt) => SurrealValue(DbValue::Datetime(Datetime::from(dt))),
            Value::ZonedDateTime(dt) => SurrealValue(DbValue::Datetime(Datetime::from(dt))),
            // TIMETZ - stored as string to preserve timezone format.
            // Note: We intentionally do NOT use Datetime here because time and datetime
            // are fundamentally different types. Datetime implies a specific point in time,
            // while TIMETZ represents a daily recurring time in a specific timezone.
            // Misusing Datetime to represent time would lose semantic meaning.
            Value::TimeTz(s) => SurrealValue(DbValue::String(s)),

            // UUID
            Value::Uuid(u) => SurrealValue(DbValue::Uuid(surrealdb::types::Uuid::from(u))),

            // ULID - convert to string since SurrealDB doesn't have native ULID ID type
            Value::Ulid(u) => SurrealValue(DbValue::String(u.to_string())),

            // JSON types
            Value::Json(json_val) => SurrealValue(json_to_surreal(&json_val)),
            Value::Jsonb(json_val) => SurrealValue(json_to_surreal(&json_val)),

            // Array
            Value::Array { elements, .. } => {
                let surreal_arr: Vec<DbValue> = elements
                    .into_iter()
                    .map(|v| SurrealValue::from(v).into_inner())
                    .collect();
                SurrealValue(DbValue::Array(Array::from(surreal_arr)))
            }

            // Set - stored as array of strings
            Value::Set { elements, .. } => {
                let surreal_arr: Vec<DbValue> = elements.into_iter().map(DbValue::String).collect();
                SurrealValue(DbValue::Array(Array::from(surreal_arr)))
            }

            // Enum - stored as string
            Value::Enum { value, .. } => SurrealValue(DbValue::String(value)),

            // Geometry - convert to JSON object
            Value::Geometry { data, .. } => {
                use sync_core::values::GeometryData;
                let GeometryData(json_val) = data;
                SurrealValue(json_to_surreal(&json_val))
            }

            // Duration - convert to SurrealDB Duration
            Value::Duration(d) => {
                SurrealValue(DbValue::Duration(surrealdb::types::Duration::from(d)))
            }

            // Thing - record reference
            Value::Thing { table, id } => {
                // Convert the ID to a SurrealDB ID type
                let surreal_id = match id.as_ref() {
                    Value::Text(s) => RecordIdKey::String(s.clone()),
                    Value::Int32(i) => RecordIdKey::Number(*i as i64),
                    Value::Int64(i) => RecordIdKey::Number(*i),
                    Value::Uuid(u) => RecordIdKey::Uuid(surrealdb::types::Uuid::from(*u)),
                    Value::Ulid(u) => RecordIdKey::String(u.to_string()),
                    // For unsupported types, convert to string representation
                    other => RecordIdKey::String(format!("{other:?}")),
                };
                let record_id = RecordId::new(table.as_str(), surreal_id);
                SurrealValue(DbValue::RecordId(record_id))
            }

            // Object - nested document
            Value::Object(map) => {
                let mut obj = BTreeMap::new();
                for (k, v) in map {
                    obj.insert(k.clone(), generated_value_to_surreal(&v));
                }
                SurrealValue(DbValue::Object(Object::from(obj)))
            }

            // Zero temporal — default sink policy is NONE (see ZeroTemporalPolicy)
            Value::ZeroTemporal {
                intended_type,
                source,
            } => SurrealValue::from_zero_temporal(
                &intended_type,
                source.as_deref(),
                sync_core::ZeroTemporalPolicy::default(),
            ),
        }
    }
}

/// Convert a Value to a SurrealDB DbValue (without type context).
/// This is a helper for internal use - prefer using `From<Value>` trait.
fn generated_value_to_surreal(value: &Value) -> DbValue {
    SurrealValue::from(value.clone()).into_inner()
}

/// Convert a serde_json::Value to SurrealDB DbValue.
fn json_to_surreal(value: &serde_json::Value) -> DbValue {
    match value {
        serde_json::Value::Null => DbValue::None,
        serde_json::Value::Bool(b) => DbValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DbValue::Number(Number::Int(i))
            } else if let Some(f) = n.as_f64() {
                DbValue::Number(Number::Float(f))
            } else {
                DbValue::None
            }
        }
        serde_json::Value::String(s) => DbValue::String(s.clone()),
        serde_json::Value::Array(arr) => DbValue::Array(Array::from(
            arr.iter().map(json_to_surreal).collect::<Vec<_>>(),
        )),
        serde_json::Value::Object(map) => {
            let mut obj = BTreeMap::new();
            for (k, v) in map {
                obj.insert(k.clone(), json_to_surreal(v));
            }
            DbValue::Object(Object::from(obj))
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

/// Create a SurrealDB RecordId (record ID).
///
/// Returns an error for unsupported ID types.
pub fn create_thing(table: &str, id: &Value) -> anyhow::Result<RecordId> {
    let id_part = match id {
        Value::Text(s) => RecordIdKey::String(s.clone()),
        Value::Int32(i) => RecordIdKey::Number(*i as i64),
        Value::Int64(i) => RecordIdKey::Number(*i),
        Value::Uuid(u) => RecordIdKey::Uuid(surrealdb::types::Uuid::from(*u)),
        Value::Ulid(u) => RecordIdKey::String(u.to_string()),
        other => anyhow::bail!(
            "Unsupported Value type for SurrealDB ID: {other:?}. \
             Supported types: Text, Int32, Int64, Uuid, Ulid"
        ),
    };
    Ok(RecordId::new(table, id_part))
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

/// Convert a HashMap of TypedValue to a HashMap of surrealdb::types::Value.
///
/// This is useful for sources that need to build record data as a HashMap
/// before creating SurrealDB records.
pub fn typed_values_to_surreal_map(
    typed_values: std::collections::HashMap<String, TypedValue>,
) -> std::collections::HashMap<String, DbValue> {
    typed_values
        .into_iter()
        .map(|(k, v)| (k, SurrealValue::from(v).into_inner()))
        .collect()
}

/// Record type that uses native surrealdb::types::Value instead of SurrealValue wrapper.
/// This is used by sync sources for direct SurrealDB insertion.
#[derive(Debug, Clone)]
pub struct RecordWithSurrealValues {
    pub id: RecordId,
    pub data: std::collections::HashMap<String, DbValue>,
}

impl RecordWithSurrealValues {
    /// Create a new record with the given ID and data.
    pub fn new(id: RecordId, data: std::collections::HashMap<String, DbValue>) -> Self {
        Self { id, data }
    }

    /// Get the upsert content as a SurrealDB Object.
    pub fn get_upsert_content(&self) -> DbValue {
        let mut m = BTreeMap::new();
        for (k, v) in &self.data {
            m.insert(k.clone(), v.clone());
        }
        DbValue::Object(Object::from(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(Type::Text);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, DbValue::None));
    }

    #[test]
    fn test_zero_temporal_policies() {
        let zero = Value::zero_temporal(Type::Date, Some("0000-00-00".into()));

        let none = SurrealValue::from_universal_with_policy(
            zero.clone(),
            sync_core::ZeroTemporalPolicy::None,
        );
        assert!(matches!(none.0, DbValue::None));

        let null = SurrealValue::from_universal_with_policy(
            zero.clone(),
            sync_core::ZeroTemporalPolicy::Null,
        );
        assert!(matches!(null.0, DbValue::Null));

        let string =
            SurrealValue::from_universal_with_policy(zero, sync_core::ZeroTemporalPolicy::String);
        match string.0 {
            DbValue::String(s) => assert_eq!(s, "0000-00-00"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn test_bool_conversion() {
        let tv = TypedValue::bool(true);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, DbValue::Bool(true)));

        let tv = TypedValue::bool(false);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, DbValue::Bool(false)));
    }

    #[test]
    fn test_tinyint_conversion() {
        let tv = TypedValue::int8(127, 4);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, DbValue::Number(Number::Int(127))));
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::int16(32000);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, DbValue::Number(Number::Int(32000))));
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int32(12345);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(surreal_val.0, DbValue::Number(Number::Int(12345))));
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9876543210);
        let surreal_val: SurrealValue = tv.into();
        assert!(matches!(
            surreal_val.0,
            DbValue::Number(Number::Int(9876543210))
        ));
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.234);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Number(Number::Float(f)) = surreal_val.0 {
            assert!((f - 1.234).abs() < 0.0001);
        } else {
            panic!("Expected Float");
        }
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::float64(1.23456);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Number(Number::Float(f)) = surreal_val.0 {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.45", 10, 2);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Number(Number::Decimal(d)) = surreal_val.0 {
            assert_eq!(d.to_string(), "123.45");
        } else {
            panic!("Expected Decimal");
        }
    }

    #[test]
    fn test_high_precision_decimal_conversion() {
        // DbValue exceeding rust_decimal MAX should be stored as float
        let tv = TypedValue::decimal("12345678901234567890123456789012345678901234567890", 50, 0);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Number(Number::Float(f)) = surreal_val.0 {
            // Large numbers get converted to f64 (with precision loss)
            assert!(f > 1e49);
        } else {
            panic!(
                "Expected Float for high precision decimal, got {:?}",
                surreal_val.0
            );
        }
    }

    #[test]
    fn test_char_conversion() {
        let tv = TypedValue::char_type("test", 10);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::String(s) = surreal_val.0 {
            assert_eq!(s, "test");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_text_conversion() {
        let tv = TypedValue::text("hello world");
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::String(s) = surreal_val.0 {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_bytes_conversion() {
        let tv = TypedValue::bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Bytes(b) = surreal_val.0 {
            assert_eq!(&*b.into_inner(), &[0xDE, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("Expected Bytes");
        }
    }

    #[test]
    fn test_uuid_conversion() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(u);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Uuid(uuid) = surreal_val.0 {
            let inner: uuid::Uuid = uuid.into();
            assert_eq!(inner.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let tv = TypedValue::datetime(dt);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Datetime(sdt) = surreal_val.0 {
            let inner: chrono::DateTime<chrono::Utc> = sdt.into();
            assert_eq!(inner.year(), 2024);
            assert_eq!(inner.month(), 6);
            assert_eq!(inner.day(), 15);
        } else {
            panic!("Expected Datetime");
        }
    }

    #[test]
    fn test_date_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap();
        let tv = TypedValue::date(dt);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::String(s) = surreal_val.0 {
            assert_eq!(s, "2024-06-15");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue::time(dt);
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::String(s) = surreal_val.0 {
            assert_eq!(s, "14:30:45");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let json = serde_json::json!({
            "name": "test",
            "count": 42
        });

        let tv = TypedValue {
            sync_type: Type::Json,
            value: Value::Json(Box::new(json)),
        };
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Object(obj) = surreal_val.0 {
            assert!(obj.get("name").is_some());
            assert!(obj.get("count").is_some());
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_json_string_conversion() {
        let tv = TypedValue {
            sync_type: Type::Json,
            value: Value::Text(r#"{"key": "value"}"#.to_string()),
        };
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Object(obj) = surreal_val.0 {
            assert!(obj.get("key").is_some());
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let tv = TypedValue::array(
            vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
            Type::Int32,
        );
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Array(arr) = surreal_val.0 {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_array_text_conversion() {
        let tv = TypedValue::array(
            vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
            Type::Text,
        );
        let surreal_val: SurrealValue = tv.into();
        if let DbValue::Array(arr) = surreal_val.0 {
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
        if let DbValue::Array(arr) = surreal_val.0 {
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
        if let DbValue::String(s) = surreal_val.0 {
            assert_eq!(s, "active");
        } else {
            panic!("Expected String");
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
        assert!(matches!(surreal_val.0, DbValue::Object(_)));
    }

    #[test]
    fn test_create_thing_string() {
        let id = Value::Text("user123".to_string());
        let record_id = create_thing("users", &id).unwrap();
        assert_eq!(record_id.table.as_str(), "users");
    }

    #[test]
    fn test_create_thing_int() {
        let id = Value::Int64(42);
        let record_id = create_thing("users", &id).unwrap();
        assert_eq!(record_id.table.as_str(), "users");
    }

    #[test]
    fn test_create_thing_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = Value::Uuid(u);
        let record_id = create_thing("users", &id).unwrap();
        assert_eq!(record_id.table.as_str(), "users");
    }

    #[test]
    fn test_create_thing_ulid() {
        let u = ulid::Ulid::new();
        let id = Value::Ulid(u);
        let record_id = create_thing("users", &id).unwrap();
        assert_eq!(record_id.table.as_str(), "users");
    }

    #[test]
    fn test_create_thing_unsupported_type() {
        let id = Value::Float64(1.23);
        let result = create_thing("users", &id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported"));
    }

    #[test]
    fn test_typed_values_to_object() {
        let fields = vec![
            ("name".to_string(), TypedValue::text("Alice")),
            ("age".to_string(), TypedValue::int32(30)),
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
        if let DbValue::Duration(d) = surreal_val.0 {
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
        if let DbValue::Duration(d) = surreal_val.0 {
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
        if let DbValue::Duration(d) = surreal_val.0 {
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
        if let DbValue::String(s) = surreal_val.0 {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected String, got {:?}", surreal_val.0);
        }
    }
}
