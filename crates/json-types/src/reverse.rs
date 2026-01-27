//! Reverse conversion: JSON value → TypedValue.
//!
//! This module provides conversion from JSON values to sync-core's `TypedValue`.

use base64::Engine;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde_json;
use std::collections::HashMap;
use sync_core::{TypedValue, UniversalType, UniversalValue};

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
            (UniversalType::Int8 { width }, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::int8(i as i8, *width)
                } else {
                    TypedValue::null(UniversalType::Int8 { width: *width })
                }
            }
            (UniversalType::Int16, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::int16(i as i16)
                } else {
                    TypedValue::null(UniversalType::Int16)
                }
            }
            (UniversalType::Int32, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::int32(i as i32)
                } else {
                    TypedValue::null(UniversalType::Int32)
                }
            }
            (UniversalType::Int64, serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    TypedValue::int64(i)
                } else {
                    TypedValue::null(UniversalType::Int64)
                }
            }

            // Floating point
            (UniversalType::Float32, serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    TypedValue::float32(f as f32)
                } else {
                    TypedValue::null(UniversalType::Float32)
                }
            }
            (UniversalType::Float64, serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    TypedValue::float64(f)
                } else {
                    TypedValue::null(UniversalType::Float64)
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
            (UniversalType::LocalDateTime, serde_json::Value::String(s)) => {
                if let Some(dt) = parse_datetime_string(s) {
                    TypedValue::datetime(dt)
                } else {
                    TypedValue::null(UniversalType::LocalDateTime)
                }
            }
            (UniversalType::LocalDateTimeNano, serde_json::Value::String(s)) => {
                if let Some(dt) = parse_datetime_string(s) {
                    TypedValue::datetime_nano(dt)
                } else {
                    TypedValue::null(UniversalType::LocalDateTimeNano)
                }
            }
            (UniversalType::ZonedDateTime, serde_json::Value::String(s)) => {
                if let Some(dt) = parse_datetime_string(s) {
                    TypedValue::timestamptz(dt)
                } else {
                    TypedValue::null(UniversalType::ZonedDateTime)
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
                let value = json_object_to_universal(obj);
                TypedValue::json(value)
            }
            (UniversalType::Json, serde_json::Value::Array(arr)) => {
                let value = json_array_to_universal(arr);
                TypedValue::json(value)
            }
            (UniversalType::Jsonb, serde_json::Value::Object(obj)) => {
                let value = json_object_to_universal(obj);
                TypedValue::jsonb(value)
            }
            (UniversalType::Jsonb, serde_json::Value::Array(arr)) => {
                let value = json_array_to_universal(arr);
                TypedValue::jsonb(value)
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
                let elements: Vec<String> = arr
                    .iter()
                    .filter_map(|v| {
                        if let serde_json::Value::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                TypedValue::set(elements, set_values.clone())
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
                TypedValue::geometry_geojson(
                    serde_json::Value::Object(obj.clone()),
                    geometry_type.clone(),
                )
            }

            // Duration - parse ISO 8601 duration string (PTxS or PTx.xxxxxxxxxS format)
            (UniversalType::Duration, serde_json::Value::String(s)) => {
                if let Some(duration) = parse_iso8601_duration(s) {
                    TypedValue::duration(duration)
                } else {
                    TypedValue::null(UniversalType::Duration)
                }
            }

            // Fallback
            (sync_type, _) => TypedValue::null(sync_type.clone()),
        }
    }
}

/// Convert a JSON object to a UniversalValue.
#[allow(dead_code)]
fn json_object_to_universal(obj: &serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    serde_json::Value::Object(obj.clone())
}

/// Convert a JSON array to a UniversalValue.
#[allow(dead_code)]
fn json_array_to_universal(arr: &[serde_json::Value]) -> serde_json::Value {
    serde_json::Value::Array(arr.to_vec())
}

/// Convert a JSON object to a HashMap of UniversalValue (for GeoJSON geometry).
#[allow(dead_code)]
fn json_object_to_geojson_hashmap(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, UniversalValue> {
    let mut map = HashMap::new();
    for (key, value) in obj {
        map.insert(key.clone(), json_value_to_universal(value));
    }
    map
}

/// Convert a JSON value to UniversalValue (without schema information).
///
/// This performs a generic conversion without type hints, inferring types from the JSON values.
pub fn json_value_to_universal(value: &serde_json::Value) -> UniversalValue {
    match value {
        serde_json::Value::Null => UniversalValue::Null,
        serde_json::Value::Bool(b) => UniversalValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                UniversalValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => UniversalValue::Text(s.clone()),
        serde_json::Value::Array(arr) => UniversalValue::Array {
            elements: arr.iter().map(json_value_to_universal).collect(),
            element_type: Box::new(UniversalType::Text),
        },
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (key, val) in obj {
                map.insert(key.clone(), json_value_to_universal(val));
            }
            UniversalValue::Json(Box::new(serde_json::Value::Object(obj.clone())))
        }
    }
}

/// Convert a JSON value to UniversalValue (without type context).
#[allow(dead_code)]
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
        serde_json::Value::String(s) => UniversalValue::Text(s.clone()),
        serde_json::Value::Array(arr) => UniversalValue::Array {
            elements: arr.iter().map(json_value_to_generated).collect(),
            element_type: Box::new(sync_core::UniversalType::Text),
        },
        serde_json::Value::Object(_obj) => UniversalValue::Json(Box::new(value.clone())),
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
    // Convert UniversalValue back to serde_json::Value for TypedValue::json
    let json_value = if let UniversalValue::Json(json_val) = gv {
        *json_val
    } else {
        // For other types, convert to JSON
        universal_value_to_json(&gv)
    };
    TypedValue::json(json_value)
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
                UniversalValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => {
            // Check if this path should be treated as a SET column
            let is_set_path = config.set_paths.iter().any(|p| p == current_path);

            if is_set_path {
                // Convert comma-separated SET values to array
                if s.is_empty() {
                    UniversalValue::Array {
                        elements: Vec::new(),
                        element_type: Box::new(sync_core::UniversalType::Text),
                    }
                } else {
                    let values: Vec<UniversalValue> = s
                        .split(',')
                        .map(|v| UniversalValue::Text(v.to_string()))
                        .collect();
                    UniversalValue::Array {
                        elements: values,
                        element_type: Box::new(sync_core::UniversalType::Text),
                    }
                }
            } else {
                UniversalValue::Text(s)
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
            UniversalValue::Array {
                elements: values,
                element_type: Box::new(sync_core::UniversalType::Text),
            }
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
                    // Convert using the config
                    let converted = json_to_generated_value_with_config(val, &nested_path, config);
                    (key, converted)
                })
                .collect();
            // Convert the HashMap back to a JSON object
            let json_obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_value_to_json(v)))
                .collect();
            UniversalValue::Json(Box::new(serde_json::Value::Object(json_obj)))
        }
    }
}

/// Convert an UniversalValue to serde_json::Value (helper for config-based conversion).
fn universal_value_to_json(value: &UniversalValue) -> serde_json::Value {
    match value {
        UniversalValue::Null => serde_json::Value::Null,
        UniversalValue::Bool(b) => serde_json::Value::Bool(*b),
        UniversalValue::Int64(i) => serde_json::json!(*i),
        UniversalValue::Float64(f) => serde_json::json!(*f),
        UniversalValue::Text(s) => serde_json::Value::String(s.clone()),
        UniversalValue::Array { elements, .. } => {
            serde_json::Value::Array(elements.iter().map(universal_value_to_json).collect())
        }
        UniversalValue::Json(json_val) => (**json_val).clone(),
        _ => serde_json::Value::Null,
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

// ============================================================================
// Schema-aware conversion functions for source crates
// These replace surrealdb_types functions to enable decoupling from SurrealDB
// ============================================================================

/// Convert JSON value to UniversalValue using table schema for type lookup.
///
/// This is the universal equivalent of `surrealdb_types::json_to_surreal_with_table_schema`.
/// Returns UniversalValue instead of surrealdb::sql::Value.
pub fn json_to_universal_with_table_schema(
    value: serde_json::Value,
    field_name: &str,
    schema: &sync_core::TableDefinition,
) -> anyhow::Result<UniversalValue> {
    // Look up the column type for this field
    let column_type = schema.get_column_type(field_name);

    match column_type {
        Some(sync_type) => {
            let jv = JsonValueWithSchema::new(value, sync_type.clone());
            let tv = TypedValue::from(jv);
            Ok(tv.value)
        }
        None => {
            // No schema info for this field, use generic conversion
            Ok(json_value_to_universal(&value))
        }
    }
}

/// Convert a string ID value to UniversalValue based on schema-defined type.
///
/// This is the universal equivalent of `surrealdb_types::convert_id_with_database_schema`.
/// Returns UniversalValue instead of surrealdb::sql::Id.
pub fn convert_id_to_universal_with_database_schema(
    id_str: &str,
    table_name: &str,
    id_column: &str,
    schema: &sync_core::DatabaseSchema,
) -> anyhow::Result<UniversalValue> {
    // Look up the table schema
    let table_schema = schema
        .get_table(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{table_name}' not found in schema"))?;

    // Look up the ID column type
    let id_type = table_schema.get_column_type(id_column).ok_or_else(|| {
        anyhow::anyhow!("Column '{id_column}' not found in table '{table_name}' schema")
    })?;

    // Convert based on the schema-defined type
    convert_id_to_universal_value(id_str, table_name, id_type)
}

/// Convert a string ID value to UniversalValue using UniversalType.
pub fn convert_id_to_universal_value(
    id_str: &str,
    table_name: &str,
    id_type: &UniversalType,
) -> anyhow::Result<UniversalValue> {
    match id_type {
        UniversalType::Int8 { .. }
        | UniversalType::Int16
        | UniversalType::Int32
        | UniversalType::Int64 => {
            let id_int: i64 = id_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as integer for table '{table_name}': {e}"
                )
            })?;
            Ok(UniversalValue::Int64(id_int))
        }
        UniversalType::Uuid => {
            let uuid = uuid::Uuid::parse_str(id_str).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as UUID for table '{table_name}': {e}"
                )
            })?;
            Ok(UniversalValue::Uuid(uuid))
        }
        UniversalType::Text | UniversalType::VarChar { .. } | UniversalType::Char { .. } => {
            Ok(UniversalValue::Text(id_str.to_string()))
        }
        other => {
            anyhow::bail!(
                "Unsupported ID type {other:?} for table '{table_name}'. Supported types: Int8-64, Uuid, Text, VarChar, Char."
            );
        }
    }
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
        let jv = JsonValueWithSchema::new(json!(42), UniversalType::Int32);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let jv = JsonValueWithSchema::new(json!(9876543210i64), UniversalType::Int64);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Int64(9876543210)));
    }

    #[test]
    fn test_float_conversion() {
        let jv = JsonValueWithSchema::new(json!(1.23456), UniversalType::Float64);
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
        assert!(matches!(tv.value, UniversalValue::Text(ref s) if s == "hello world"));
    }

    #[test]
    fn test_varchar_conversion() {
        let jv = JsonValueWithSchema::new(json!("test"), UniversalType::VarChar { length: 100 });
        let tv = TypedValue::from(jv);
        assert!(matches!(
            tv.sync_type,
            UniversalType::VarChar { length: 100 }
        ));
        if let UniversalValue::VarChar { value, length } = tv.value {
            assert_eq!(value, "test");
            assert_eq!(length, 100);
        } else {
            panic!("Expected VarChar, got {:?}", tv.value);
        }
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
        let jv = JsonValueWithSchema::new(json!(dt.to_rfc3339()), UniversalType::LocalDateTime);
        let tv = TypedValue::from(jv);
        if let UniversalValue::LocalDateTime(result_dt) = tv.value {
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
        if let UniversalValue::Date(dt) = tv.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected Date, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_time_from_string() {
        let jv = JsonValueWithSchema::new(json!("14:30:45"), UniversalType::Time);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Time(dt) = tv.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected Time, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_json_object_conversion() {
        let jv =
            JsonValueWithSchema::new(json!({"name": "test", "count": 42}), UniversalType::Json);
        let tv = TypedValue::from(jv);
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
    fn test_json_array_conversion() {
        // JSON columns in MySQL can contain arrays - stored as Json with array content
        let jv = JsonValueWithSchema::new(json!([1, 2, 3]), UniversalType::Json);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Json(json_val) = tv.value {
            if let serde_json::Value::Array(arr) = json_val.as_ref() {
                assert_eq!(arr.len(), 3);
                assert!(
                    matches!(arr[0], serde_json::Value::Number(ref n) if n.as_i64() == Some(1))
                );
                assert!(
                    matches!(arr[1], serde_json::Value::Number(ref n) if n.as_i64() == Some(2))
                );
                assert!(
                    matches!(arr[2], serde_json::Value::Number(ref n) if n.as_i64() == Some(3))
                );
            } else {
                panic!("Expected Array inside Json");
            }
        } else {
            panic!("Expected Json, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_json_array_of_strings_conversion() {
        // JSON columns can contain arrays of strings (e.g., tags field) - stored as Json
        let jv = JsonValueWithSchema::new(json!(["tag1", "tag2", "tag3"]), UniversalType::Json);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Json(json_val) = tv.value {
            if let serde_json::Value::Array(arr) = json_val.as_ref() {
                assert_eq!(arr.len(), 3);
                assert!(matches!(arr[0], serde_json::Value::String(ref s) if s == "tag1"));
                assert!(matches!(arr[1], serde_json::Value::String(ref s) if s == "tag2"));
                assert!(matches!(arr[2], serde_json::Value::String(ref s) if s == "tag3"));
            } else {
                panic!("Expected Array inside Json");
            }
        } else {
            panic!("Expected Json, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_jsonb_array_conversion() {
        // JSONB columns can also contain arrays - stored as Jsonb
        let jv = JsonValueWithSchema::new(json!([1, 2, 3]), UniversalType::Jsonb);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Jsonb(json_val) = tv.value {
            if let serde_json::Value::Array(arr) = json_val.as_ref() {
                assert_eq!(arr.len(), 3);
                assert!(
                    matches!(arr[0], serde_json::Value::Number(ref n) if n.as_i64() == Some(1))
                );
            } else {
                panic!("Expected Array inside Jsonb");
            }
        } else {
            panic!("Expected Jsonb, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_array_int_conversion() {
        let jv = JsonValueWithSchema::new(
            json!([1, 2, 3]),
            UniversalType::Array {
                element_type: Box::new(UniversalType::Int32),
            },
        );
        let tv = TypedValue::from(jv);
        if let UniversalValue::Array { elements, .. } = tv.value {
            assert_eq!(elements.len(), 3);
            assert!(matches!(elements[0], UniversalValue::Int32(1)));
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
        if let UniversalValue::Set { elements, .. } = tv.value {
            assert_eq!(elements.len(), 2);
            assert!(elements.contains(&"a".to_string()));
            assert!(elements.contains(&"b".to_string()));
        } else {
            panic!("Expected Set, got {:?}", tv.value);
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
        if let UniversalValue::Enum { value, .. } = tv.value {
            assert_eq!(value, "active");
        } else {
            panic!("Expected Enum, got {:?}", tv.value);
        }
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
        if let UniversalValue::Geometry { data, .. } = tv.value {
            use sync_core::values::GeometryData;
            let GeometryData(ref geo_json) = data;
            if let serde_json::Value::Object(map) = geo_json {
                assert!(
                    matches!(map.get("type"), Some(serde_json::Value::String(s)) if s == "Point")
                );
            } else {
                panic!("Expected Object inside GeometryData");
            }
        } else {
            panic!("Expected Geometry, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_extract_field() {
        let obj = json!({"name": "Alice", "age": 30})
            .as_object()
            .unwrap()
            .clone();

        let name = extract_field(&obj, "name", &UniversalType::Text);
        assert!(matches!(name.value, UniversalValue::Text(ref s) if s == "Alice"));

        let age = extract_field(&obj, "age", &UniversalType::Int32);
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
            ("score".to_string(), UniversalType::Float64),
        ];

        let values = parse_jsonl_line(line, &schema).unwrap();
        assert!(matches!(
            values.get("name").unwrap().value,
            UniversalValue::Text(ref s) if s == "Bob"
        ));
        assert!(matches!(
            values.get("active").unwrap().value,
            UniversalValue::Bool(true)
        ));
    }

    #[test]
    fn test_duration_conversion() {
        // Test parsing ISO 8601 duration string "PT181S" (181 seconds)
        let jv = JsonValueWithSchema::new(json!("PT181S"), UniversalType::Duration);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Duration(d) = tv.value {
            assert_eq!(d.as_secs(), 181);
            assert_eq!(d.subsec_nanos(), 0);
        } else {
            panic!("Expected Duration, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_duration_with_nanos_conversion() {
        // Test parsing ISO 8601 duration string "PT60.123456789S" (60 seconds + 123456789 nanoseconds)
        let jv = JsonValueWithSchema::new(json!("PT60.123456789S"), UniversalType::Duration);
        let tv = TypedValue::from(jv);
        if let UniversalValue::Duration(d) = tv.value {
            assert_eq!(d.as_secs(), 60);
            assert_eq!(d.subsec_nanos(), 123456789);
        } else {
            panic!("Expected Duration, got {:?}", tv.value);
        }
    }

    #[test]
    fn test_duration_invalid_format() {
        // Test that invalid duration strings return null
        let jv = JsonValueWithSchema::new(json!("not a duration"), UniversalType::Duration);
        let tv = TypedValue::from(jv);
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_json_to_universal_with_table_schema_array() {
        // Test that json_to_universal_with_table_schema correctly converts JSON arrays
        // when the schema defines the field as Array<Text>
        use sync_core::{ColumnDefinition, TableDefinition};

        // Create a table schema with an array column
        let pk = ColumnDefinition::new("id", UniversalType::Text);
        let columns = vec![ColumnDefinition::new(
            "tags",
            UniversalType::Array {
                element_type: Box::new(UniversalType::Text),
            },
        )];
        let table_schema = TableDefinition::new("test_table", pk, columns);

        // Test converting a JSON array to UniversalValue::Array
        let json_array = json!(["tag1", "tag2", "tag3"]);
        let result =
            json_to_universal_with_table_schema(json_array, "tags", &table_schema).unwrap();

        match result {
            UniversalValue::Array { elements, .. } => {
                assert_eq!(elements.len(), 3);
                assert!(matches!(&elements[0], UniversalValue::Text(s) if s == "tag1"));
                assert!(matches!(&elements[1], UniversalValue::Text(s) if s == "tag2"));
                assert!(matches!(&elements[2], UniversalValue::Text(s) if s == "tag3"));
            }
            other => panic!("Expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_json_to_universal_with_table_schema_null_array() {
        // Test that null JSON values become UniversalValue::Null even for array columns
        use sync_core::{ColumnDefinition, TableDefinition};

        let pk = ColumnDefinition::new("id", UniversalType::Text);
        let columns = vec![ColumnDefinition::new(
            "tags",
            UniversalType::Array {
                element_type: Box::new(UniversalType::Text),
            },
        )];
        let table_schema = TableDefinition::new("test_table", pk, columns);

        let json_null = json!(null);
        let result = json_to_universal_with_table_schema(json_null, "tags", &table_schema).unwrap();

        assert!(
            matches!(result, UniversalValue::Null),
            "Expected Null, got {result:?}"
        );
    }

    #[test]
    fn test_json_to_universal_with_table_schema_unknown_field() {
        // Test that unknown fields fall back to generic conversion
        use sync_core::{ColumnDefinition, TableDefinition};

        let pk = ColumnDefinition::new("id", UniversalType::Text);
        let columns = vec![];
        let table_schema = TableDefinition::new("test_table", pk, columns);

        // Unknown field with JSON array should still convert using generic conversion
        let json_array = json!(["a", "b"]);
        let result =
            json_to_universal_with_table_schema(json_array, "unknown_field", &table_schema)
                .unwrap();

        match result {
            UniversalValue::Array { elements, .. } => {
                assert_eq!(elements.len(), 2);
            }
            other => panic!("Expected Array from generic conversion, got {other:?}"),
        }
    }
}
