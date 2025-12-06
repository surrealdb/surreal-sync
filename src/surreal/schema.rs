//! Schema-aware type conversion for surreal-sync
//!
//! This module provides generic data type representation and schema-aware conversion
//! that enables consistent type handling across different database sources and
//! sync modes (full vs incremental).

use crate::SurrealValue;
use std::collections::HashMap;

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
}
