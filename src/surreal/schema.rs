//! Schema-aware type conversion for surreal-sync
//!
//! This module provides generic data type representation and schema-aware conversion
//! that enables consistent type handling across different database sources and
//! sync modes (full vs incremental).
//!
//! ## Type System Bridge
//!
//! This module provides a bridge between the existing `SurrealType` enum and the new
//! `SyncDataType` from sync-core. The `surreal_type_to_sync_type` function converts
//! between these representations, enabling the unified TypedValue conversion path.
//!
//! In future, `SurrealType` and `SurrealValue` will be removed, and schema collection
//! will directly produce `SyncDataType` values.

use crate::SurrealValue;
use std::collections::HashMap;
use sync_core::SyncDataType;

/// Generic data type representation for schema-aware conversion
///
/// This enum represents database column types in a generic way that can be used
/// across different database sources (PostgreSQL, MySQL, etc.) to enable
/// consistent type conversion between full and incremental sync modes.
#[derive(Debug, Clone, PartialEq)]
pub enum SurrealType {
    /// Boolean type
    Boolean,
    /// Integer type (any size)
    Integer,
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
    Timestamp,
    /// Timestamp with timezone
    TimestampWithTimezone,
    /// Time duration/interval
    Duration,
    /// JSON/JSONB data
    Json,
    /// Array of another type
    Array(Box<SurrealType>),
    /// UUID type
    Uuid,
    /// Geometric/spatial data
    Geometry,
    /// Source-specific type that doesn't map to generic types
    SourceSpecific(String),
}

/// Table schema information for schema-aware conversion
#[derive(Debug, Clone)]
pub struct SurrealTableSchema {
    pub table_name: String,
    pub columns: HashMap<String, SurrealType>,
}

/// Database schema information containing all table schemas
#[derive(Debug, Clone)]
pub struct SurrealDatabaseSchema {
    pub tables: HashMap<String, SurrealTableSchema>,
}

/// Convert SurrealType to SyncDataType for use with TypedValue conversions.
///
/// This function bridges the existing `SurrealType` enum (used for schema representation)
/// with `SyncDataType` from sync-core (used for TypedValue conversions). This enables
/// the unified conversion path:
/// `JSON → TypedValue (json-types) → surrealdb::sql::Value (surrealdb-types)`
///
/// # Note
/// This is a transitional function. In Phase 24, when `SurrealType` is removed,
/// schema collection will directly produce `SyncDataType` values.
pub fn surreal_type_to_sync_type(surreal_type: &SurrealType) -> SyncDataType {
    match surreal_type {
        SurrealType::Boolean => SyncDataType::Bool,
        SurrealType::Integer => SyncDataType::BigInt, // Use BigInt as safest default
        SurrealType::Float => SyncDataType::Double,
        SurrealType::String => SyncDataType::Text,
        SurrealType::Bytes => SyncDataType::Bytes,
        SurrealType::Decimal { precision, scale } => SyncDataType::Decimal {
            // precision/scale are u32 but SyncDataType expects u8, cap at 38
            precision: precision.map(|p| p.min(38) as u8).unwrap_or(38),
            scale: scale.map(|s| s.min(38) as u8).unwrap_or(10),
        },
        SurrealType::Date => SyncDataType::Date,
        SurrealType::Time => SyncDataType::Time,
        SurrealType::Timestamp => SyncDataType::DateTime,
        SurrealType::TimestampWithTimezone => SyncDataType::TimestampTz,
        SurrealType::Duration => SyncDataType::Text, // Duration stored as text
        SurrealType::Json => SyncDataType::Json,
        SurrealType::Array(inner) => SyncDataType::Array {
            element_type: Box::new(surreal_type_to_sync_type(inner)),
        },
        SurrealType::Uuid => SyncDataType::Uuid,
        SurrealType::Geometry => SyncDataType::Geometry {
            // Use Point as a generic fallback - actual geometry type is in the data
            geometry_type: sync_core::GeometryType::Point,
        },
        SurrealType::SourceSpecific(_) => SyncDataType::Text, // Fallback to text
    }
}

/// Convert JSON value to SurrealValue using schema information for type precision
///
/// This function provides schema-aware conversion that preserves database-specific
/// type precision during incremental sync operations that go through JSON conversion.
pub fn json_to_surreal_with_schema(
    value: serde_json::Value,
    field_name: &str,
    schema: &SurrealTableSchema,
) -> anyhow::Result<SurrealValue> {
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
        (serde_json::Value::Number(n), Some(SurrealType::Decimal { .. })) => {
            let decimal_str = n.to_string();
            match surrealdb::sql::Number::try_from(decimal_str.as_str()) {
                Ok(surreal_num) => Ok(SurrealValue::Decimal(surreal_num)),
                Err(_) => {
                    // Fallback to float if decimal parsing fails
                    if let Some(f) = n.as_f64() {
                        Ok(SurrealValue::Float(f))
                    } else {
                        Ok(SurrealValue::String(decimal_str))
                    }
                }
            }
        }

        // Timestamp conversion (works for both PostgreSQL and MySQL)
        (serde_json::Value::String(s), Some(SurrealType::Timestamp)) => {
            use chrono::NaiveDateTime;
            // Try common timestamp formats
            if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(SurrealValue::DateTime(dt))
            // Try MySQL Timestamp-like format with fractional seconds (space separator)
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(SurrealValue::DateTime(dt))
            // Try ISO 8601 format with T separator (PostgreSQL to_jsonb output)
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(SurrealValue::DateTime(dt))
            // Try ISO 8601 format without fractional seconds
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(SurrealValue::DateTime(dt))
            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                Ok(SurrealValue::DateTime(dt.with_timezone(&chrono::Utc)))
            } else {
                // Fallback to string if parsing fails
                Ok(SurrealValue::String(s))
            }
        }

        // Timestamp with timezone
        (serde_json::Value::String(s), Some(SurrealType::TimestampWithTimezone)) => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                Ok(SurrealValue::DateTime(dt.with_timezone(&chrono::Utc)))
            } else {
                Ok(SurrealValue::String(s))
            }
        }

        // UUID validation and preservation
        (serde_json::Value::String(s), Some(SurrealType::Uuid)) => {
            // For now, just preserve as string (could add UUID validation later)
            Ok(SurrealValue::String(s))
        }

        // JSON type - parse if it's a string representation
        (serde_json::Value::String(s), Some(SurrealType::Json)) => {
            // Try to parse JSON string into object
            match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(parsed_json) => crate::json_to_surreal_without_schema(parsed_json),
                Err(_) => Ok(SurrealValue::String(s)), // Keep as string if not valid JSON
            }
        }

        // Date conversion
        (serde_json::Value::String(s), Some(SurrealType::Date)) => {
            if let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
                Ok(SurrealValue::DateTime(dt))
            } else {
                Ok(SurrealValue::String(s))
            }
        }

        // Time conversion
        (serde_json::Value::String(s), Some(SurrealType::Time)) => {
            // Keep time as string since SurrealDB doesn't have pure time type
            Ok(SurrealValue::String(s))
        }

        // Boolean conversion - handle MySQL TINYINT(1) which comes as 0/1 integers
        (serde_json::Value::Number(n), Some(SurrealType::Boolean)) => {
            if let Some(i) = n.as_i64() {
                Ok(SurrealValue::Bool(i != 0))
            } else {
                Err(anyhow::anyhow!(
                    "Boolean field expected integer value, got non-integer number: {n}",
                ))
            }
        }

        // SET column conversion - handle MySQL SET columns encoded as comma-separated strings
        (serde_json::Value::String(s), Some(SurrealType::Array(inner)))
            if matches!(inner.as_ref(), SurrealType::String) =>
        {
            // Convert comma-separated SET values to array
            if s.is_empty() {
                Ok(SurrealValue::Array(Vec::new()))
            } else {
                let values: Vec<SurrealValue> = s
                    .split(',')
                    .map(|v| SurrealValue::String(v.to_string()))
                    .collect();
                Ok(SurrealValue::Array(values))
            }
        }

        // Handle NULL SET columns
        (serde_json::Value::Null, Some(SurrealType::Array(inner)))
            if matches!(inner.as_ref(), SurrealType::String) =>
        {
            Ok(SurrealValue::Null)
        }

        // For all other cases, use the existing generic JSON conversion
        (value, _) => crate::json_to_surreal_without_schema(value),
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
    schema: &SurrealDatabaseSchema,
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
        SurrealType::Integer => {
            // Parse as integer
            let id_int: i64 = id_str.parse().map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as integer for table '{table_name}': {e}"
                )
            })?;
            Ok(surrealdb::sql::Id::Number(id_int))
        }
        SurrealType::Uuid => {
            // Parse as UUID
            let uuid = uuid::Uuid::parse_str(id_str).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as UUID for table '{table_name}': {e}"
                )
            })?;
            Ok(surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(uuid)))
        }
        SurrealType::String => {
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
        let decimal_type = SurrealType::Decimal {
            precision: Some(10),
            scale: Some(2),
        };
        assert_eq!(
            decimal_type,
            SurrealType::Decimal {
                precision: Some(10),
                scale: Some(2),
            }
        );

        let timestamp_type = SurrealType::Timestamp;
        assert_eq!(timestamp_type, SurrealType::Timestamp);
    }

    #[test]
    fn test_table_schema_creation() {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SurrealType::Integer);
        columns.insert(
            "price".to_string(),
            SurrealType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
        );
        columns.insert("created_at".to_string(), SurrealType::Timestamp);

        let schema = SurrealTableSchema {
            table_name: "products".to_string(),
            columns,
        };

        assert_eq!(schema.table_name, "products");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns.get("id"), Some(&SurrealType::Integer));
    }

    #[test]
    fn test_schema_aware_decimal_conversion() {
        let mut columns = HashMap::new();
        columns.insert(
            "price".to_string(),
            SurrealType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
        );

        let schema = SurrealTableSchema {
            table_name: "products".to_string(),
            columns,
        };

        // Test decimal conversion
        let json_value = serde_json::Value::Number(serde_json::Number::from_f64(123.45).unwrap());
        let result = json_to_surreal_with_schema(json_value, "price", &schema).unwrap();

        match result {
            SurrealValue::Decimal(_) => (), // Success
            _ => panic!("Expected Decimal, got {result:?}"),
        }
    }

    #[test]
    fn test_schema_aware_timestamp_conversion() {
        let mut columns = HashMap::new();
        columns.insert("created_at".to_string(), SurrealType::Timestamp);

        let schema = SurrealTableSchema {
            table_name: "events".to_string(),
            columns,
        };

        // Test timestamp conversion
        let json_value = serde_json::Value::String("2024-01-15 14:30:00".to_string());
        let result = json_to_surreal_with_schema(json_value, "created_at", &schema).unwrap();

        match result {
            SurrealValue::DateTime(_) => (), // Success
            _ => panic!("Expected DateTime, got {result:?}"),
        }
    }

    #[test]
    fn test_schema_aware_fallback() {
        let schema = SurrealTableSchema {
            table_name: "test".to_string(),
            columns: HashMap::new(), // Empty schema
        };

        // Should fallback to regular JSON conversion
        let json_value = serde_json::Value::String("test".to_string());
        let result = json_to_surreal_with_schema(json_value, "unknown_field", &schema).unwrap();

        match result {
            SurrealValue::String(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String, got {result:?}"),
        }
    }

    #[test]
    fn test_convert_id_with_schema_integer() {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), SurrealType::Integer);
        tables.insert(
            "users".to_string(),
            SurrealTableSchema {
                table_name: "users".to_string(),
                columns,
            },
        );
        let schema = SurrealDatabaseSchema { tables };

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
        columns.insert("id".to_string(), SurrealType::Uuid);
        tables.insert(
            "products".to_string(),
            SurrealTableSchema {
                table_name: "products".to_string(),
                columns,
            },
        );
        let schema = SurrealDatabaseSchema { tables };

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
        columns.insert("id".to_string(), SurrealType::String);
        tables.insert(
            "documents".to_string(),
            SurrealTableSchema {
                table_name: "documents".to_string(),
                columns,
            },
        );
        let schema = SurrealDatabaseSchema { tables };

        // Test string ID conversion (should remain as string)
        let id = convert_id_with_schema("doc-123-abc", "documents", "id", &schema).unwrap();
        match id {
            surrealdb::sql::Id::String(s) => assert_eq!(s, "doc-123-abc"),
            _ => panic!("Expected String, got {id:?}"),
        }
    }

    #[test]
    fn test_convert_id_with_schema_table_not_found() {
        let schema = SurrealDatabaseSchema {
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
