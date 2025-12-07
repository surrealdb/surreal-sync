//! Reverse conversion: JSON value → TypedValue.
//!
//! This module provides conversion from JSON values to sync-core's `TypedValue`.

use base64::Engine;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::collections::HashMap;
use sync_core::{TypedValue, UniversalType, UniversalValue};

/// Parse a datetime string in various formats.
///
/// Supports:
/// - RFC 3339: "2024-01-01T12:00:00Z"
/// - MySQL timestamp: "2024-01-01 12:00:00"
/// - MySQL timestamp with microseconds: "2024-01-01 12:00:00.123456"
fn parse_datetime_string(s: &str) -> Option<DateTime<Utc>> {
    // Try RFC 3339 first (ISO 8601 with timezone)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try MySQL timestamp format without microseconds
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(Utc.from_utc_datetime(&naive));
    }

    // Try MySQL timestamp format with microseconds
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(Utc.from_utc_datetime(&naive));
    }

    // Try PostgreSQL timestamp format with timezone offset
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z") {
        return Some(dt.with_timezone(&Utc));
    }

    None
}

/// JSON value paired with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct JsonValueWithSchema {
    /// The JSON value.
    pub value: serde_json::Value,
    /// The expected sync type for conversion.
    pub sync_type: UniversalType,
}

impl JsonValueWithSchema {
    /// Create a new JsonValueWithSchema.
    pub fn new(value: serde_json::Value, sync_type: UniversalType) -> Self {
        Self { value, sync_type }
    }

    /// Convert to TypedValue.
    pub fn to_typed_value(&self) -> TypedValue {
        TypedValue::from(self.clone())
    }
}

impl From<JsonValueWithSchema> for TypedValue {
    fn from(jv: JsonValueWithSchema) -> Self {
        match (&jv.sync_type, &jv.value) {
            // Null
            (sync_type, serde_json::Value::Null) => TypedValue::null(sync_type.clone()),

            // Boolean
            (UniversalType::Bool, serde_json::Value::Bool(b)) => TypedValue::bool(*b),
            // MySQL stores TINYINT(1) booleans as 0/1 in JSON_OBJECT
            (UniversalType::Bool, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::bool(i != 0)
                } else {
                    TypedValue::null(UniversalType::Bool)
                }
            }

            // Integer types
            (UniversalType::TinyInt { width }, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::tinyint(i as i32, *width)
                } else {
                    TypedValue::null(UniversalType::TinyInt { width: *width })
                }
            }
            (UniversalType::SmallInt, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::smallint(i as i32)
                } else {
                    TypedValue::null(UniversalType::SmallInt)
                }
            }
            (UniversalType::Int, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::int(i as i32)
                } else {
                    TypedValue::null(UniversalType::Int)
                }
            }
            (UniversalType::BigInt, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::bigint(i)
                } else {
                    TypedValue::null(UniversalType::BigInt)
                }
            }

            // Floating point
            (UniversalType::Float, serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    TypedValue::float(f)
                } else {
                    TypedValue::null(UniversalType::Float)
                }
            }
            (UniversalType::Double, serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    TypedValue::double(f)
                } else {
                    TypedValue::null(UniversalType::Double)
                }
            }

            // Decimal - stored as string in JSON
            (UniversalType::Decimal { precision, scale }, serde_json::Value::String(s)) => {
                TypedValue::decimal(s, *precision, *scale)
            }
            (UniversalType::Decimal { precision, scale }, serde_json::Value::Number(n)) => {
                TypedValue::decimal(n.to_string(), *precision, *scale)
            }

            // String types
            (UniversalType::Char { length }, serde_json::Value::String(s)) => {
                TypedValue::char_type(s, *length)
            }
            (UniversalType::VarChar { length }, serde_json::Value::String(s)) => {
                TypedValue::varchar(s, *length)
            }
            (UniversalType::Text, serde_json::Value::String(s)) => TypedValue::text(s),

            // Binary types - base64 encoded in JSON
            (UniversalType::Blob, serde_json::Value::String(s)) => {
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => TypedValue::blob(bytes),
                    Err(_) => TypedValue::null(UniversalType::Blob),
                }
            }
            (UniversalType::Bytes, serde_json::Value::String(s)) => {
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => TypedValue::bytes(bytes),
                    Err(_) => TypedValue::null(UniversalType::Bytes),
                }
            }

            // UUID
            (UniversalType::Uuid, serde_json::Value::String(s)) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                    TypedValue::uuid(uuid)
                } else {
                    TypedValue::null(UniversalType::Uuid)
                }
            }

            // Date/time types - multiple formats supported
            (UniversalType::DateTime, serde_json::Value::String(s)) => {
                if let Some(dt) = parse_datetime_string(s) {
                    TypedValue::datetime(dt)
                } else {
                    TypedValue::null(UniversalType::DateTime)
                }
            }
            (UniversalType::DateTimeNano, serde_json::Value::String(s)) => {
                if let Some(dt) = parse_datetime_string(s) {
                    TypedValue::datetime_nano(dt)
                } else {
                    TypedValue::null(UniversalType::DateTimeNano)
                }
            }
            (UniversalType::TimestampTz, serde_json::Value::String(s)) => {
                if let Some(dt) = parse_datetime_string(s) {
                    TypedValue::timestamptz(dt)
                } else {
                    TypedValue::null(UniversalType::TimestampTz)
                }
            }

            // Date stored as string
            (UniversalType::Date, serde_json::Value::String(s)) => {
                if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let datetime = dt.and_hms_opt(0, 0, 0).unwrap();
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue::date(utc_dt)
                } else {
                    TypedValue::null(UniversalType::Date)
                }
            }

            // Time stored as string
            (UniversalType::Time, serde_json::Value::String(s)) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    let datetime = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(datetime, Utc);
                    TypedValue::time(utc_dt)
                } else {
                    TypedValue::null(UniversalType::Time)
                }
            }

            // JSON types - can be objects or arrays
            (UniversalType::Json, serde_json::Value::Object(obj)) => {
                let map = json_object_to_hashmap(obj);
                TypedValue::json(UniversalValue::Object(map))
            }
            (UniversalType::Json, serde_json::Value::Array(arr)) => {
                let values: Vec<UniversalValue> = arr.iter().map(json_value_to_generated).collect();
                TypedValue::json(UniversalValue::Array(values))
            }
            (UniversalType::Jsonb, serde_json::Value::Object(obj)) => {
                let map = json_object_to_hashmap(obj);
                TypedValue::jsonb(UniversalValue::Object(map))
            }
            (UniversalType::Jsonb, serde_json::Value::Array(arr)) => {
                let values: Vec<UniversalValue> = arr.iter().map(json_value_to_generated).collect();
                TypedValue::jsonb(UniversalValue::Array(values))
            }

            // Array types
            (UniversalType::Array { element_type }, serde_json::Value::Array(arr)) => {
                let values: Vec<UniversalValue> = arr
                    .iter()
                    .map(|v| {
                        let jv = JsonValueWithSchema::new(v.clone(), (**element_type).clone());
                        TypedValue::from(jv).value
                    })
                    .collect();
                TypedValue::array(values, (**element_type).clone())
            }

            // Set - stored as array
            (UniversalType::Set { values: set_values }, serde_json::Value::Array(arr)) => {
                let values: Vec<UniversalValue> = arr
                    .iter()
                    .filter_map(|v| {
                        if let serde_json::Value::String(s) = v {
                            Some(UniversalValue::String(s.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                TypedValue::set(values, set_values.clone())
            }

            // Enum - stored as string
            (
                UniversalType::Enum {
                    values: enum_values,
                },
                serde_json::Value::String(s),
            ) => TypedValue::enum_type(s.clone(), enum_values.clone()),

            // Geometry types - GeoJSON format
            (UniversalType::Geometry { geometry_type }, serde_json::Value::Object(obj)) => {
                let map = json_object_to_hashmap(obj);
                TypedValue::geometry_object(map, geometry_type.clone())
            }

            // Fallback
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a JSON object to a HashMap of UniversalValue.
fn json_object_to_hashmap(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, UniversalValue> {
    let mut map = HashMap::new();
    for (key, value) in obj {
        map.insert(key.clone(), json_value_to_generated(value));
    }
    map
}

/// Convert a JSON value to UniversalValue (without type context).
fn json_value_to_generated(value: &serde_json::Value) -> UniversalValue {
    match value {
        serde_json::Value::Null => UniversalValue::Null,
        serde_json::Value::Bool(b) => UniversalValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                UniversalValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::Null
            }
        }
        serde_json::Value::String(s) => UniversalValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            UniversalValue::Array(arr.iter().map(json_value_to_generated).collect())
        }
        serde_json::Value::Object(obj) => UniversalValue::Object(json_object_to_hashmap(obj)),
    }
}

/// Configuration for JSON field conversions.
///
/// Some databases (MySQL, PostgreSQL) store boolean values as 0/1 in JSON fields.
/// This struct allows specifying which JSON paths should be converted to boolean values
/// or treated as SET columns (comma-separated arrays).
///
/// # Example
///
/// ```
/// use json_types::JsonConversionConfig;
///
/// let config = JsonConversionConfig::new()
///     .with_boolean_path("settings.enabled")
///     .with_boolean_path("flags.is_active")
///     .with_set_path("permissions");
/// ```
#[derive(Debug, Clone, Default)]
pub struct JsonConversionConfig {
    /// JSON paths that should convert 0/1 to boolean.
    /// Paths use dot notation, e.g., "settings.enabled" or "flags.is_active".
    pub boolean_paths: Vec<String>,
    /// JSON paths that should be treated as SET columns (comma-separated arrays).
    pub set_paths: Vec<String>,
}

impl JsonConversionConfig {
    /// Create a new empty configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a boolean path.
    pub fn with_boolean_path(mut self, path: &str) -> Self {
        self.boolean_paths.push(path.to_string());
        self
    }

    /// Add multiple boolean paths.
    pub fn with_boolean_paths(mut self, paths: &[&str]) -> Self {
        self.boolean_paths
            .extend(paths.iter().map(|s| s.to_string()));
        self
    }

    /// Add a SET path.
    pub fn with_set_path(mut self, path: &str) -> Self {
        self.set_paths.push(path.to_string());
        self
    }

    /// Add multiple SET paths.
    pub fn with_set_paths(mut self, paths: &[&str]) -> Self {
        self.set_paths.extend(paths.iter().map(|s| s.to_string()));
        self
    }
}

/// Convert a JSON value to TypedValue with path-based configuration.
///
/// This handles database-specific quirks like storing booleans as 0/1 in JSON fields.
///
/// # Arguments
/// * `value` - The JSON value to convert
/// * `current_path` - The current path in the JSON tree (for nested objects), typically ""
/// * `config` - Configuration specifying which paths should be treated specially
///
/// # Example
/// ```
/// use json_types::{JsonConversionConfig, json_to_typed_value_with_config};
/// use sync_core::UniversalValue;
///
/// let config = JsonConversionConfig::new()
///     .with_boolean_path("settings.enabled")
///     .with_boolean_path("flags.is_active");
///
/// let json = serde_json::json!({"settings": {"enabled": 1}});
/// let tv = json_to_typed_value_with_config(json, "", &config);
/// // tv.value will have {"settings": {"enabled": true}}
/// ```
pub fn json_to_typed_value_with_config(
    value: serde_json::Value,
    current_path: &str,
    config: &JsonConversionConfig,
) -> TypedValue {
    let gv = json_to_generated_value_with_config(value, current_path, config);
    TypedValue::json(gv)
}

/// Convert JSON to UniversalValue with path-based configuration.
///
/// This is the internal implementation that handles the recursive conversion.
pub fn json_to_generated_value_with_config(
    value: serde_json::Value,
    current_path: &str,
    config: &JsonConversionConfig,
) -> UniversalValue {
    match value {
        serde_json::Value::Null => UniversalValue::Null,
        serde_json::Value::Bool(b) => UniversalValue::Bool(b),
        serde_json::Value::Number(n) => {
            // Check if this path should be treated as boolean
            let is_boolean_path = config.boolean_paths.iter().any(|p| p == current_path);

            if let Some(i) = n.as_i64() {
                if is_boolean_path && (i == 0 || i == 1) {
                    // Convert 0/1 to boolean for specified paths
                    UniversalValue::Bool(i == 1)
                } else {
                    UniversalValue::Int64(i)
                }
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => {
            // Check if this path should be treated as a SET column
            let is_set_path = config.set_paths.iter().any(|p| p == current_path);

            if is_set_path {
                // Convert comma-separated SET values to array
                if s.is_empty() {
                    UniversalValue::Array(Vec::new())
                } else {
                    let values: Vec<UniversalValue> = s
                        .split(',')
                        .map(|v| UniversalValue::String(v.to_string()))
                        .collect();
                    UniversalValue::Array(values)
                }
            } else {
                UniversalValue::String(s)
            }
        }
        serde_json::Value::Array(arr) => {
            let values: Vec<UniversalValue> = arr
                .into_iter()
                .enumerate()
                .map(|(idx, item)| {
                    let item_path = format!("{current_path}[{idx}]");
                    json_to_generated_value_with_config(item, &item_path, config)
                })
                .collect();
            UniversalValue::Array(values)
        }
        serde_json::Value::Object(obj) => {
            let map: HashMap<String, UniversalValue> = obj
                .into_iter()
                .map(|(key, val)| {
                    // Build the nested path for this field
                    let nested_path = if current_path.is_empty() {
                        key.clone()
                    } else {
                        format!("{current_path}.{key}")
                    };
                    (
                        key,
                        json_to_generated_value_with_config(val, &nested_path, config),
                    )
                })
                .collect();
            UniversalValue::Object(map)
        }
    }
}

/// Extract a typed value from a JSON object field.
pub fn extract_field(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    sync_type: &UniversalType,
) -> TypedValue {
    match obj.get(field) {
        Some(value) => JsonValueWithSchema::new(value.clone(), sync_type.clone()).to_typed_value(),
        None => TypedValue::null(sync_type.clone()),
    }
}

/// Convert a complete JSON object to a map of TypedValues using schema.
pub fn json_object_to_typed_values(
    obj: &serde_json::Map<String, serde_json::Value>,
    schema: &[(String, UniversalType)],
) -> HashMap<String, TypedValue> {
    let mut result = HashMap::new();
    for (field_name, sync_type) in schema {
        let tv = extract_field(obj, field_name, sync_type);
        result.insert(field_name.clone(), tv);
    }
    result
}

/// Parse a JSONL line and convert to typed values.
pub fn parse_jsonl_line(
    line: &str,
    schema: &[(String, UniversalType)],
) -> Result<HashMap<String, TypedValue>, serde_json::Error> {
    let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line)?;
    Ok(json_object_to_typed_values(&obj, schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};
    use serde_json::json;
    use sync_core::GeometryType;

    #[test]
    fn test_null_conversion() {
        let jv = JsonValueWithSchema::new(serde_json::Value::Null, UniversalType::Text);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_bool_conversion() {
        let jv = JsonValueWithSchema::new(json!(true), UniversalType::Bool);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_bool_from_number_zero() {
        // MySQL stores TINYINT(1) booleans as 0/1 in JSON_OBJECT
        let jv = JsonValueWithSchema::new(json!(0), UniversalType::Bool);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Bool(false)));
    }

    #[test]
    fn test_bool_from_number_one() {
        // MySQL stores TINYINT(1) booleans as 0/1 in JSON_OBJECT
        let jv = JsonValueWithSchema::new(json!(1), UniversalType::Bool);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_bool_from_nonzero_number() {
        // Non-zero numbers should be true (like MySQL's boolean semantics)
        let jv = JsonValueWithSchema::new(json!(42), UniversalType::Bool);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_int_conversion() {
        let jv = JsonValueWithSchema::new(json!(42), UniversalType::Int);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let jv = JsonValueWithSchema::new(json!(9876543210i64), UniversalType::BigInt);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let jv = JsonValueWithSchema::new(json!(1.23456), UniversalType::Double);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Float64(f) = tv.value {
            assert!((f - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_decimal_from_string() {
        let jv = JsonValueWithSchema::new(
            json!("123.456"),
            UniversalType::Decimal {
                precision: 10,
                scale: 3,
            },
        );
        let tv = TypedValue::from(jv);
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
        let jv = JsonValueWithSchema::new(json!("hello world"), UniversalType::Text);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::String(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let jv = JsonValueWithSchema::new(json!("test"), UniversalType::VarChar { length: 100 });
        let tv = TypedValue::from(jv);
        assert!(matches!(
            tv.sync_type,
            UniversalType::VarChar { length: 100 }
        ));
        assert!(matches!(tv.value, UniversalValue::String(ref s) if s == "test"));
    }

    #[test]
    fn test_bytes_from_base64() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(vec![0x01, 0x02, 0x03]);
        let jv = JsonValueWithSchema::new(json!(encoded), UniversalType::Bytes);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Bytes(ref b) if *b == vec![0x01, 0x02, 0x03]));
    }

    #[test]
    fn test_uuid_conversion() {
        let jv = JsonValueWithSchema::new(
            json!("550e8400-e29b-41d4-a716-446655440000"),
            UniversalType::Uuid,
        );
        let tv = TypedValue::from(jv);
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        let jv = JsonValueWithSchema::new(json!(dt.to_rfc3339()), UniversalType::DateTime);
        let tv = TypedValue::from(jv);
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
        let jv = JsonValueWithSchema::new(json!("2024-06-15"), UniversalType::Date);
        let tv = TypedValue::from(jv);
        if let UniversalValue::DateTime(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_time_from_string() {
        let jv = JsonValueWithSchema::new(json!("14:30:45"), UniversalType::Time);
        let tv = TypedValue::from(jv);
        if let UniversalValue::DateTime(dt) = tv.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let jv =
            JsonValueWithSchema::new(json!({"name": "test", "count": 42}), UniversalType::Json);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Object(map) = tv.value {
            assert!(matches!(map.get("name"), Some(UniversalValue::String(s)) if s == "test"));
            assert!(matches!(map.get("count"), Some(UniversalValue::Int64(42))));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_json_array_conversion() {
        // JSON columns in MySQL can contain arrays - this should work
        let jv = JsonValueWithSchema::new(json!([1, 2, 3]), UniversalType::Json);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Array(values) = tv.value {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], UniversalValue::Int64(1)));
            assert!(matches!(values[1], UniversalValue::Int64(2)));
            assert!(matches!(values[2], UniversalValue::Int64(3)));
        } else {
            panic!("Expected Array, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_json_array_of_strings_conversion() {
        // JSON columns can contain arrays of strings (e.g., tags field)
        let jv = JsonValueWithSchema::new(json!(["tag1", "tag2", "tag3"]), UniversalType::Json);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Array(values) = tv.value {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], UniversalValue::String(ref s) if s == "tag1"));
            assert!(matches!(values[1], UniversalValue::String(ref s) if s == "tag2"));
            assert!(matches!(values[2], UniversalValue::String(ref s) if s == "tag3"));
        } else {
            panic!("Expected Array, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_jsonb_array_conversion() {
        // JSONB columns can also contain arrays
        let jv = JsonValueWithSchema::new(json!([1, 2, 3]), UniversalType::Jsonb);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Array(values) = tv.value {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], UniversalValue::Int64(1)));
        } else {
            panic!("Expected Array, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let jv = JsonValueWithSchema::new(
            json!([1, 2, 3]),
            UniversalType::Array {
                element_type: Box::new(UniversalType::Int),
            },
        );
        let tv = TypedValue::from(jv);
        if let UniversalValue::Array(values) = tv.value {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], UniversalValue::Int32(1)));
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_set_conversion() {
        let jv = JsonValueWithSchema::new(
            json!(["a", "b"]),
            UniversalType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        );
        let tv = TypedValue::from(jv);
        if let UniversalValue::Array(values) = tv.value {
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_enum_conversion() {
        let jv = JsonValueWithSchema::new(
            json!("active"),
            UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::String(ref s) if s == "active"));
    }

    #[test]
    fn test_geometry_conversion() {
        let jv = JsonValueWithSchema::new(
            json!({"type": "Point", "coordinates": [-73.97, 40.77]}),
            UniversalType::Geometry {
                geometry_type: GeometryType::Point,
            },
        );
        let tv = TypedValue::from(jv);
        if let UniversalValue::Object(map) = tv.value {
            assert!(matches!(map.get("type"), Some(UniversalValue::String(s)) if s == "Point"));
        } else {
            panic!("Expected Object");
        }
    }

    #[test]
    fn test_extract_field() {
        let obj = json!({"name": "Alice", "age": 30})
            .as_object()
            .unwrap()
            .clone();

        let name = extract_field(&obj, "name", &UniversalType::Text);
        assert!(matches!(name.value, UniversalValue::String(ref s) if s == "Alice"));

        let age = extract_field(&obj, "age", &UniversalType::Int);
        assert!(matches!(age.value, UniversalValue::Int32(30)));

        let missing = extract_field(&obj, "missing", &UniversalType::Text);
        assert!(matches!(missing.value, UniversalValue::Null));
    }

    #[test]
    fn test_parse_jsonl_line() {
        let line = r#"{"name": "Bob", "active": true, "score": 95.5}"#;
        let schema = vec![
            ("name".to_string(), UniversalType::Text),
            ("active".to_string(), UniversalType::Bool),
            ("score".to_string(), UniversalType::Double),
        ];

        let values = parse_jsonl_line(line, &schema).unwrap();
        assert!(matches!(
            values.get("name").unwrap().value,
            UniversalValue::String(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            UniversalValue::Bool(true)
        ));
    }
}
