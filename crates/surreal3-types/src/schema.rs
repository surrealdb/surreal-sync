//! Schema-aware type conversion for surreal-sync
//!
//! This module provides schema-aware conversion that enables consistent type handling
//! across different database sources and sync modes (full vs incremental).
//!
//! ## Type System
//!
//! This module uses `UniversalType` from sync-core directly for schema representation,
//! along with `TableDefinition` and `DatabaseSchema` for schema structures.

use surrealdb::types::{Number, RecordIdKey, Value};
use sync_core::{DatabaseSchema, TableDefinition, UniversalType};

/// Convert JSON value to surrealdb::types::Value using schema information for type precision.
///
/// This function provides schema-aware conversion that preserves database-specific
/// type precision during incremental sync operations that go through JSON conversion.
///
/// Uses `TableDefinition` from sync-core for schema information.
pub fn json_to_surreal_with_table_schema(
    value: serde_json::Value,
    field_name: &str,
    schema: &TableDefinition,
) -> anyhow::Result<Value> {
    // Look up the column type for this field
    let column_type = schema.get_column_type(field_name);

    tracing::debug!(
        "Converting JSON value for field '{}' with type {:?}: {:?}",
        field_name,
        column_type,
        value
    );

    json_to_surreal_with_universal_type(value, column_type)
}

/// Convert JSON value to surrealdb::types::Value using UniversalType for type precision.
///
/// This is the core conversion function that handles all type-specific conversions.
pub fn json_to_surreal_with_universal_type(
    value: serde_json::Value,
    column_type: Option<&UniversalType>,
) -> anyhow::Result<Value> {
    match (value, column_type) {
        // High-precision decimal conversion
        (serde_json::Value::Number(n), Some(UniversalType::Decimal { .. })) => {
            let decimal_str = n.to_string();
            // Try to parse as Decimal first
            if let Ok(dec) = rust_decimal::Decimal::from_str_exact(&decimal_str) {
                Ok(Value::Number(Number::Decimal(dec)))
            } else if let Some(f) = n.as_f64() {
                // Fallback to float if decimal parsing fails
                Ok(Value::Number(Number::Float(f)))
            } else {
                Ok(Value::String(decimal_str))
            }
        }

        // Timestamp conversion (LocalDateTime - without timezone)
        (serde_json::Value::String(s), Some(UniversalType::LocalDateTime)) => {
            use chrono::NaiveDateTime;
            // Try common timestamp formats
            if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::types::Datetime::from(dt)))
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::types::Datetime::from(dt)))
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::types::Datetime::from(dt)))
            } else if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::types::Datetime::from(dt)))
            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                Ok(Value::Datetime(surrealdb::types::Datetime::from(
                    dt.with_timezone(&chrono::Utc),
                )))
            } else {
                Ok(Value::String(s))
            }
        }

        // Timestamp with timezone (ZonedDateTime)
        (serde_json::Value::String(s), Some(UniversalType::ZonedDateTime)) => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                Ok(Value::Datetime(surrealdb::types::Datetime::from(
                    dt.with_timezone(&chrono::Utc),
                )))
            } else {
                Ok(Value::String(s))
            }
        }

        // UUID validation and preservation
        (serde_json::Value::String(s), Some(UniversalType::Uuid)) => Ok(Value::String(s)),

        // JSON type - parse if it's a string representation
        (serde_json::Value::String(s), Some(UniversalType::Json | UniversalType::Jsonb)) => {
            match serde_json::from_str::<serde_json::Value>(&s) {
                Ok(parsed_json) => Ok(json_to_surreal_without_schema(parsed_json)),
                Err(_) => Ok(Value::String(s)),
            }
        }

        // Date conversion
        (serde_json::Value::String(s), Some(UniversalType::Date)) => {
            if let Ok(date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                let dt =
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
                Ok(Value::Datetime(surrealdb::types::Datetime::from(dt)))
            } else {
                Ok(Value::String(s))
            }
        }

        // Time conversion
        (serde_json::Value::String(s), Some(UniversalType::Time)) => Ok(Value::String(s)),

        // Boolean conversion - handle MySQL TINYINT(1) which comes as 0/1 integers
        (serde_json::Value::Number(n), Some(UniversalType::Bool)) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Bool(i != 0))
            } else {
                Err(anyhow::anyhow!(
                    "Boolean field expected integer value, got non-integer number: {n}",
                ))
            }
        }

        // SET column conversion - handle MySQL SET columns encoded as comma-separated strings
        (serde_json::Value::String(s), Some(UniversalType::Set { .. })) => {
            if s.is_empty() {
                Ok(Value::Array(surrealdb::types::Array::from(
                    Vec::<Value>::new(),
                )))
            } else {
                let values: Vec<Value> =
                    s.split(',').map(|v| Value::String(v.to_string())).collect();
                Ok(Value::Array(surrealdb::types::Array::from(values)))
            }
        }

        // Handle NULL SET columns
        (serde_json::Value::Null, Some(UniversalType::Set { .. })) => Ok(Value::Null),

        // Array type conversion
        (serde_json::Value::String(s), Some(UniversalType::Array { element_type }))
            if matches!(element_type.as_ref(), UniversalType::Text) =>
        {
            if s.is_empty() {
                Ok(Value::Array(surrealdb::types::Array::from(
                    Vec::<Value>::new(),
                )))
            } else {
                let values: Vec<Value> =
                    s.split(',').map(|v| Value::String(v.to_string())).collect();
                Ok(Value::Array(surrealdb::types::Array::from(values)))
            }
        }

        // Handle NULL Arrays
        (serde_json::Value::Null, Some(UniversalType::Array { .. })) => Ok(Value::Null),

        // For all other cases, use the generic JSON conversion
        (value, _) => Ok(json_to_surreal_without_schema(value)),
    }
}

/// Convert a string ID value to the proper SurrealDB ID type using schema information.
///
/// Uses `DatabaseSchema` from sync-core for schema lookup.
///
/// # Arguments
/// * `id_str` - The string representation of the ID
/// * `table_name` - The name of the table to look up the ID column type
/// * `id_column` - The name of the ID column
/// * `schema` - The database schema containing type information
pub fn convert_id_with_database_schema(
    id_str: &str,
    table_name: &str,
    id_column: &str,
    schema: &DatabaseSchema,
) -> anyhow::Result<RecordIdKey> {
    // Look up the table schema
    let table_schema = schema
        .get_table(table_name)
        .ok_or_else(|| anyhow::anyhow!("Table '{table_name}' not found in schema"))?;

    // Look up the ID column type
    let id_type = table_schema.get_column_type(id_column).ok_or_else(|| {
        anyhow::anyhow!("Column '{id_column}' not found in table '{table_name}' schema")
    })?;

    // Convert based on the schema-defined type
    convert_id_with_universal_type(id_str, table_name, id_type)
}

/// Convert a string ID value to the proper SurrealDB ID type using UniversalType.
fn convert_id_with_universal_type(
    id_str: &str,
    table_name: &str,
    id_type: &UniversalType,
) -> anyhow::Result<RecordIdKey> {
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
            Ok(RecordIdKey::Number(id_int))
        }
        UniversalType::Uuid => {
            let uuid = uuid::Uuid::parse_str(id_str).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse ID '{id_str}' as UUID for table '{table_name}': {e}"
                )
            })?;
            Ok(RecordIdKey::Uuid(surrealdb::types::Uuid::from(uuid)))
        }
        UniversalType::Text | UniversalType::VarChar { .. } | UniversalType::Char { .. } => {
            Ok(RecordIdKey::String(id_str.to_string()))
        }
        other => {
            tracing::warn!(
                "Unexpected ID column type {:?} for table '{}', using string ID",
                other,
                table_name
            );
            Ok(RecordIdKey::String(id_str.to_string()))
        }
    }
}

/// Convert JSON value to surrealdb::types::Value without schema information
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
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => {
            // Auto-detect ISO 8601 duration strings (PTxxxS format) and convert to Duration
            if let Some(duration) = try_parse_iso8601_duration(&s) {
                Value::Duration(surrealdb::types::Duration::from(duration))
            } else {
                Value::String(s)
            }
        }
        serde_json::Value::Array(arr) => {
            let values: Vec<Value> = arr
                .into_iter()
                .map(json_to_surreal_without_schema)
                .collect();
            Value::Array(surrealdb::types::Array::from(values))
        }
        serde_json::Value::Object(map) => {
            let mut obj = std::collections::BTreeMap::new();
            for (key, val) in map {
                obj.insert(key, json_to_surreal_without_schema(val));
            }
            Value::Object(surrealdb::types::Object::from(obj))
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

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::ColumnDefinition;

    // ========================================================================
    // New sync-core type tests
    // ========================================================================

    #[test]
    fn test_new_schema_aware_decimal_conversion() {
        let schema = TableDefinition::new(
            "products",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![ColumnDefinition::new(
                "price",
                UniversalType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            )],
        );

        // Test decimal conversion
        let json_value = serde_json::Value::Number(serde_json::Number::from_f64(123.45).unwrap());
        let result = json_to_surreal_with_table_schema(json_value, "price", &schema).unwrap();

        match result {
            Value::Number(_) => (), // Success
            _ => panic!("Expected Number, got {result:?}"),
        }
    }

    #[test]
    fn test_new_schema_aware_timestamp_conversion() {
        let schema = TableDefinition::new(
            "events",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![ColumnDefinition::new(
                "created_at",
                UniversalType::LocalDateTime,
            )],
        );

        // Test timestamp conversion
        let json_value = serde_json::Value::String("2024-01-15 14:30:00".to_string());
        let result = json_to_surreal_with_table_schema(json_value, "created_at", &schema).unwrap();

        match result {
            Value::Datetime(_) => (), // Success
            _ => panic!("Expected Datetime, got {result:?}"),
        }
    }

    #[test]
    fn test_new_schema_aware_fallback() {
        let schema = TableDefinition::new(
            "test",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![], // Empty columns
        );

        // Should fallback to regular JSON conversion for unknown fields
        let json_value = serde_json::Value::String("test".to_string());
        let result =
            json_to_surreal_with_table_schema(json_value, "unknown_field", &schema).unwrap();

        match result {
            Value::String(s) => assert_eq!(s, "test"),
            _ => panic!("Expected String, got {result:?}"),
        }
    }

    #[test]
    fn test_new_convert_id_integer() {
        let schema = DatabaseSchema::new(vec![TableDefinition::new(
            "users",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![],
        )]);

        // Test integer ID conversion
        let id = convert_id_with_database_schema("12345", "users", "id", &schema).unwrap();
        match id {
            RecordIdKey::Number(n) => assert_eq!(n, 12345),
            _ => panic!("Expected Number, got {id:?}"),
        }
    }

    #[test]
    fn test_new_convert_id_uuid() {
        let schema = DatabaseSchema::new(vec![TableDefinition::new(
            "products",
            ColumnDefinition::new("id", UniversalType::Uuid),
            vec![],
        )]);

        // Test UUID ID conversion
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = convert_id_with_database_schema(uuid_str, "products", "id", &schema).unwrap();
        match id {
            RecordIdKey::Uuid(u) => {
                let inner_uuid: uuid::Uuid = u.into();
                assert_eq!(inner_uuid.to_string(), uuid_str);
            }
            _ => panic!("Expected Uuid, got {id:?}"),
        }
    }

    #[test]
    fn test_new_convert_id_string() {
        let schema = DatabaseSchema::new(vec![TableDefinition::new(
            "documents",
            ColumnDefinition::new("id", UniversalType::Text),
            vec![],
        )]);

        // Test string ID conversion (should remain as string)
        let id =
            convert_id_with_database_schema("doc-123-abc", "documents", "id", &schema).unwrap();
        match id {
            RecordIdKey::String(s) => assert_eq!(s, "doc-123-abc"),
            _ => panic!("Expected String, got {id:?}"),
        }
    }

    #[test]
    fn test_new_convert_id_table_not_found() {
        let schema = DatabaseSchema::default();

        // Test error when table not found
        let result = convert_id_with_database_schema("123", "nonexistent", "id", &schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in schema"));
    }

    #[test]
    fn test_new_set_column_conversion() {
        let schema = TableDefinition::new(
            "users",
            ColumnDefinition::new("id", UniversalType::Int64),
            vec![ColumnDefinition::new(
                "roles",
                UniversalType::Set {
                    values: vec!["admin".to_string(), "user".to_string()],
                },
            )],
        );

        // Test SET column conversion
        let json_value = serde_json::Value::String("admin,user".to_string());
        let result = json_to_surreal_with_table_schema(json_value, "roles", &schema).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected Array, got {result:?}"),
        }

        // Test empty SET
        let json_value = serde_json::Value::String("".to_string());
        let result = json_to_surreal_with_table_schema(json_value, "roles", &schema).unwrap();

        match result {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 0);
            }
            _ => panic!("Expected empty Array, got {result:?}"),
        }

        // Test NULL SET
        let json_value = serde_json::Value::Null;
        let result = json_to_surreal_with_table_schema(json_value, "roles", &schema).unwrap();
        assert!(matches!(result, Value::Null));
    }
}
