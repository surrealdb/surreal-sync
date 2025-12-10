//! Schema-aware type conversion for surreal-sync
//!
//! This module provides generic data type representation and schema-aware conversion
//! that enables consistent type handling across different database sources and
//! sync modes (full vs incremental).
//!
//! ## Type System Bridge
//!
//! This module provides a bridge between the existing `LegacyType` enum and the new
//! `UniversalType` from sync-core. The `surreal_type_to_sync_type` function converts
//! between these representations, enabling the unified TypedValue conversion path.

use std::collections::HashMap;
use surrealdb::sql::{Number, Strand, Value};
use sync_core::UniversalType;

/// Generic data type representation for schema-aware conversion
///
/// This enum represents database column types in a generic way that can be used
/// across different database sources (PostgreSQL, MySQL, etc.) to enable
/// consistent type conversion between full and incremental sync modes.
#[derive(Debug, Clone, PartialEq)]
pub enum LegacyType {
    /// Boolean type
    Bool,
    /// Integer type (any size)
    Int,
    /// Floating point type
    Float,
    /// String/text type
    String,
    /// Binary data type
    Bytes,
    /// High-precision decimal with optional precision and scale
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    /// Date only (no time component)
    Date,
    /// Time only (no date component)
    Time,
    /// Timestamp without timezone
    DateTime,
    /// Timestamp with timezone
    TimestampTz,
    /// Time duration/interval
    Duration,
    /// JSON/JSONB data
    Json,
    /// Array of another type
    Array(Box<LegacyType>),
    /// UUID type
    Uuid,
    /// Geometric/spatial data
    Geometry,
    /// Source-specific type that doesn't map to generic types
    SourceSpecific(String),
}

/// Table schema information for schema-aware conversion
#[derive(Debug, Clone)]
pub struct LegacyTableDefinition {
    pub table_name: String,
    pub columns: HashMap<String, LegacyType>,
}

/// Database schema information containing all table schemas
#[derive(Debug, Clone)]
pub struct LegacySchema {
    pub tables: HashMap<String, LegacyTableDefinition>,
}

/// Convert LegacyType to UniversalType for use with TypedValue conversions.
///
/// This function bridges the existing `LegacyType` enum (used for schema representation)
/// with `UniversalType` from sync-core (used for TypedValue conversions). This enables
/// the unified conversion path:
/// `JSON → TypedValue (json-types) → surrealdb::sql::Value (surrealdb-types)`
///
/// # Note
/// This is a transitional function. When `LegacyType` is removed,
/// schema collection will directly produce `UniversalType` values.
pub fn legacy_type_to_universal_type(legacy_type: &LegacyType) -> UniversalType {
    match legacy_type {
        LegacyType::Bool => UniversalType::Bool,
        LegacyType::Int => UniversalType::Int64, // Use Int64 as safest default
        LegacyType::Float => UniversalType::Float64,
        LegacyType::String => UniversalType::Text,
        LegacyType::Bytes => UniversalType::Bytes,
        LegacyType::Decimal { precision, scale } => UniversalType::Decimal {
            // precision/scale are u32 but UniversalType expects u8, cap at 38
            precision: precision.map(|p| p.min(38) as u8).unwrap_or(38),
            scale: scale.map(|s| s.min(38) as u8).unwrap_or(10),
        },
        LegacyType::Date => UniversalType::Date,
        LegacyType::Time => UniversalType::Time,
        LegacyType::DateTime => UniversalType::LocalDateTime,
        LegacyType::TimestampTz => UniversalType::ZonedDateTime,
        LegacyType::Duration => UniversalType::Text, // Duration stored as text
        LegacyType::Json => UniversalType::Json,
        LegacyType::Array(inner) => UniversalType::Array {
            element_type: Box::new(legacy_type_to_universal_type(inner)),
        },
        LegacyType::Uuid => UniversalType::Uuid,
        LegacyType::Geometry => UniversalType::Geometry {
            // Use Point as a generic fallback - actual geometry type is in the data
            geometry_type: sync_core::GeometryType::Point,
        },
        LegacyType::SourceSpecific(_) => UniversalType::Text, // Fallback to text
    }
}

/// Convert JSON value to surrealdb::sql::Value using schema information for type precision
///
/// This function provides schema-aware conversion that preserves database-specific
/// type precision during incremental sync operations that go through JSON conversion.
pub fn json_to_surreal_with_schema(
    value: serde_json::Value,
    field_name: &str,
    schema: &LegacyTableDefinition,
) -> anyhow::Result<Value> {
    // Look up the generic type for this field
    let generic_type = schema.columns.get(field_name);

    tracing::debug!(
        "Converting JSON value for field '{}' with data type {:?}: {:?}",
        field_name,
        generic_type,
        value
    );

    match (value, generic_type) {
        // High-precision decimal conversion
        (serde_json::Value::Number(n), Some(LegacyType::Decimal { .. })) => {
            let decimal_str = n.to_string();
            match Number::try_from(decimal_str.as_str()) {
                Ok(surreal_num) => Ok(Value::Number(surreal_num)),
                Err(_) => {
                    // Fallback to float if decimal parsing fails
                    if let Some(f) = n.as_f64() {
                        Ok(Value::Number(Number::Float(f)))
                    } else {
                        Ok(Value::Strand(Strand::from(decimal_str)))
                    }
                }
            }
        }

        // Timestamp conversion (works for both PostgreSQL and MySQL)
        (serde_json::Value::String(s), Some(LegacyType::DateTime)) => {
            use chrono::NaiveDateTime;
            // Try common timestamp formats
            if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(dt)))
            // Try MySQL Timestamp-like format with fractional seconds (space separator)
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(dt)))
            // Try ISO 8601 format with T separator (PostgreSQL to_jsonb output)
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(dt)))
            // Try ISO 8601 format without fractional seconds
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(dt)))
            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(
                    dt.with_timezone(&chrono::Utc),
                )))
            } else {
                // Fallback to string if parsing fails
                Ok(Value::Strand(Strand::from(s)))
            }
        }

        // Timestamp with timezone
        (serde_json::Value::String(s), Some(LegacyType::TimestampTz)) => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(
                    dt.with_timezone(&chrono::Utc),
                )))
            } else {
                Ok(Value::Strand(Strand::from(s)))
            }
        }

        // UUID validation and preservation
        (serde_json::Value::String(s), Some(LegacyType::Uuid)) => {
            // For now, just preserve as string (could add UUID validation later)
            Ok(Value::Strand(Strand::from(s)))
        }

        // JSON type - parse if it's a string representation
        (serde_json::Value::String(s), Some(LegacyType::Json)) => {
            // Try to parse JSON string into object
            match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(parsed_json) => Ok(json_to_surreal_without_schema(parsed_json)),
                Err(_) => Ok(Value::Strand(Strand::from(s))), // Keep as string if not valid JSON
            }
        }

        // Date conversion
        (serde_json::Value::String(s), Some(LegacyType::Date)) => {
            if let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::sql::Datetime::from(dt)))
            } else {
                Ok(Value::Strand(Strand::from(s)))
            }
        }

        // Time conversion
        (serde_json::Value::String(s), Some(LegacyType::Time)) => {
            // Keep time as string since SurrealDB doesn't have pure time type
            Ok(Value::Strand(Strand::from(s)))
        }

        // Boolean conversion - handle MySQL TINYINT(1) which comes as 0/1 integers
        (serde_json::Value::Number(n), Some(LegacyType::Bool)) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Bool(i != 0))
            } else {
                Err(anyhow::anyhow!(
                    "Boolean field expected integer value, got non-integer number: {n}",
                ))
            }
        }

        // SET column conversion - handle MySQL SET columns encoded as comma-separated strings
        (serde_json::Value::String(s), Some(LegacyType::Array(inner)))
            if matches!(inner.as_ref(), LegacyType::String) =>
        {
            // Convert comma-separated SET values to array
            if s.is_empty() {
                Ok(Value::Array(surrealdb::sql::Array::from(
                    Vec::<Value>::new(),
                )))
            } else {
                let values: Vec<Value> = s
                    .split(',')
                    .map(|v| Value::Strand(Strand::from(v.to_string())))
                    .collect();
                Ok(Value::Array(surrealdb::sql::Array::from(values)))
            }
        }

        // Handle NULL SET columns
        (serde_json::Value::Null, Some(LegacyType::Array(inner)))
            if matches!(inner.as_ref(), LegacyType::String) =>
        {
            Ok(Value::Null)
        }

        // For all other cases, use the generic JSON conversion
        (value, _) => Ok(json_to_surreal_without_schema(value)),
    }
}

/// Convert JSON value to surrealdb::sql::Value without schema information
///
/// This function performs generic JSON to SurrealDB value conversion.
/// It auto-detects ISO 8601 duration strings and converts them to Duration values.
fn json_to_surreal_without_schema(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(Number::Int(i))
            } else if let Some(f) = n.as_f64() {
                Value::Number(Number::Float(f))
            } else {
                Value::Strand(Strand::from(n.to_string()))
            }
        }
        serde_json::Value::String(s) => {
            // Auto-detect ISO 8601 duration strings (PTxxxS format) and convert to Duration
            if let Some(duration) = try_parse_iso8601_duration(&s) {
                Value::Duration(surrealdb::sql::Duration::from(duration))
            } else {
                Value::Strand(Strand::from(s))
            }
        }
        serde_json::Value::Array(arr) => {
            let values: Vec<Value> = arr
                .into_iter()
                .map(json_to_surreal_without_schema)
                .collect();
            Value::Array(surrealdb::sql::Array::from(values))
        }
        serde_json::Value::Object(map) => {
            let mut obj = std::collections::BTreeMap::new();
            for (key, val) in map {
                obj.insert(key, json_to_surreal_without_schema(val));
            }
            Value::Object(surrealdb::sql::Object::from(obj))
        }
    }
}

/// Parse an ISO 8601 duration string (PTxS or PTx.xxxxxxxxxS format).
fn try_parse_iso8601_duration(s: &str) -> Option<std::time::Duration> {
    let trimmed = s.trim();
    if let Some(secs_str) = trimmed.strip_prefix("PT").and_then(|s| s.strip_suffix('S')) {
        if let Some(dot_pos) = secs_str.find('.') {
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

/// Convert a string ID value to the proper SurrealDB ID type using schema information
///
/// This function is used to convert string representations of IDs (from audit tables,
/// JSON extractions, etc.) back to their proper types (Integer, UUID) for creating
/// SurrealDB Thing IDs.
///
/// # Arguments
/// * `id_str` - The string representation of the ID
/// * `table_name` - The name of the table to look up the ID column type
/// * `id_column` - The name of the ID column (usually "id")
/// * `schema` - The database schema containing type information
///
/// # Returns
/// A `surrealdb::sql::Id` with the proper type, or an error if conversion fails
pub fn convert_id_with_schema(
    id_str: &str,
    table_name: &str,
    id_column: &str,
    schema: &LegacySchema,
) -> anyhow::Result<surrealdb::sql::Id> {
    // Look up the table schema
    let table_schema = schema
        .tables
        .get(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{table_name}' not found in schema"))?;

    // Look up the ID column type
    let id_type = table_schema.columns.get(id_column).ok_or_else(|| {
        anyhow::anyhow!("Column '{id_column}' not found in table '{table_name}' schema")
    })?;

    // Convert based on the schema-defined type
    match id_type {
        LegacyType::Int => {
            // Parse as integer
            let id_int: i64 = id_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as integer for table '{table_name}': {e}"
                )
            })?;
            Ok(surrealdb::sql::Id::Number(id_int))
        }
        LegacyType::Uuid => {
            // Parse as UUID
            let uuid = uuid::Uuid::parse_str(id_str).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as UUID for table '{table_name}': {e}"
                )
            })?;
            Ok(surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(uuid)))
        }
        LegacyType::String => {
            // Keep as string
            Ok(surrealdb::sql::Id::String(id_str.to_string()))
        }
        other => {
            // For other types, default to string but log a warning
            tracing::warn!(
                "Unexpected ID column type {:?} for table '{}', using string ID",
                other,
                table_name
            );
            Ok(surrealdb::sql::Id::String(id_str.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generic_data_type_creation() {
        let decimal_type = LegacyType::Decimal {
            precision: Some(10),
            scale: Some(2),
        };
        assert_eq!(
            decimal_type,
            LegacyType::Decimal {
                precision: Some(10),
                scale: Some(2),
            }
        );

        let timestamp_type = LegacyType::DateTime;
        assert_eq!(timestamp_type, LegacyType::DateTime);
    }

    #[test]
    fn test_table_schema_creation() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), LegacyType::Int);
        columns.insert(
            "price".to_string(),
            LegacyType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
        );
        columns.insert("created_at".to_string(), LegacyType::DateTime);

        let schema = LegacyTableDefinition {
            table_name: "products".to_string(),
            columns,
        };

        assert_eq!(schema.table_name, "products");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns.get("id"), Some(&LegacyType::Int));
    }

    #[test]
    fn test_schema_aware_decimal_conversion() {
        let mut columns = HashMap::new();
        columns.insert(
            "price".to_string(),
            LegacyType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
        );

        let schema = LegacyTableDefinition {
            table_name: "products".to_string(),
            columns,
        };

        // Test decimal conversion
        let json_value = serde_json::Value::Number(serde_json::Number::from_f64(123.45).unwrap());
        let result = json_to_surreal_with_schema(json_value, "price", &schema).unwrap();

        match result {
            Value::Number(_) => (), // Success
            _ => panic!("Expected Number, got {result:?}"),
        }
    }

    #[test]
    fn test_schema_aware_timestamp_conversion() {
        let mut columns = HashMap::new();
        columns.insert("created_at".to_string(), LegacyType::DateTime);

        let schema = LegacyTableDefinition {
            table_name: "events".to_string(),
            columns,
        };

        // Test timestamp conversion
        let json_value = serde_json::Value::String("2024-01-15 14:30:00".to_string());
        let result = json_to_surreal_with_schema(json_value, "created_at", &schema).unwrap();

        match result {
            Value::Datetime(_) => (), // Success
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_schema_aware_fallback() {
        let schema = LegacyTableDefinition {
            table_name: "test".to_string(),
            columns: HashMap::new(), // Empty schema
        };

        // Should fallback to regular JSON conversion
        let json_value = serde_json::Value::String("test".to_string());
        let result = json_to_surreal_with_schema(json_value, "unknown_field", &schema).unwrap();

        match result {
            Value::Strand(s) => assert_eq!(s.as_str(), "test"),
            _ => panic!("Expected Strand, got {result:?}"),
        }
    }

    #[test]
    fn test_convert_id_with_schema_integer() {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), LegacyType::Int);
        tables.insert(
            "users".to_string(),
            LegacyTableDefinition {
                table_name: "users".to_string(),
                columns,
            },
        );
        let schema = LegacySchema { tables };

        // Test integer ID conversion
        let id = convert_id_with_schema("12345", "users", "id", &schema).unwrap();
        match id {
            surrealdb::sql::Id::Number(n) => assert_eq!(n, 12345),
            _ => panic!("Expected Number, got {id:?}"),
        }
    }

    #[test]
    fn test_convert_id_with_schema_uuid() {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), LegacyType::Uuid);
        tables.insert(
            "products".to_string(),
            LegacyTableDefinition {
                table_name: "products".to_string(),
                columns,
            },
        );
        let schema = LegacySchema { tables };

        // Test UUID ID conversion
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = convert_id_with_schema(uuid_str, "products", "id", &schema).unwrap();
        match id {
            surrealdb::sql::Id::Uuid(u) => {
                // SurrealDB UUID wrapper formats as u'...' - extract inner UUID for comparison
                let inner_uuid: uuid::Uuid = u.into();
                assert_eq!(inner_uuid.to_string(), uuid_str);
            }
            _ => panic!("Expected Uuid, got {id:?}"),
        }
    }

    #[test]
    fn test_convert_id_with_schema_string() {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), LegacyType::String);
        tables.insert(
            "documents".to_string(),
            LegacyTableDefinition {
                table_name: "documents".to_string(),
                columns,
            },
        );
        let schema = LegacySchema { tables };

        // Test string ID conversion (should remain as string)
        let id = convert_id_with_schema("doc-123-abc", "documents", "id", &schema).unwrap();
        match id {
            surrealdb::sql::Id::String(s) => assert_eq!(s, "doc-123-abc"),
            _ => panic!("Expected String, got {id:?}"),
        }
    }

    #[test]
    fn test_convert_id_with_schema_table_not_found() {
        let schema = LegacySchema {
            tables: HashMap::new(),
        };

        // Test error when table not found
        let result = convert_id_with_schema("123", "nonexistent", "id", &schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in schema"));
    }
}
