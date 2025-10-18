//! PostgreSQL change data structures and conversion from wal2json format

use anyhow::{bail, Context, Result};
use std::collections::HashMap;

/// Represents a PostgreSQL value with proper type information
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    // Numeric types
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Numeric(String), // For DECIMAL/NUMERIC, stored as string to preserve precision

    // String types
    Text(String),
    Varchar(String),
    Char(String),

    // Boolean
    Boolean(bool),

    // Binary
    Bytea(Vec<u8>),

    // UUID
    Uuid(String),

    // JSON types
    Json(serde_json::Value),
    Jsonb(serde_json::Value),

    // Date/Time types
    Timestamp(String),
    TimestampTz(String),
    Date(String),
    Time(String),
    TimeTz(String),
    Interval(String),

    // Arrays
    Array(Vec<Value>),

    // Special
    Null,
}

/// Represents a database row with primary key and column data
#[derive(Debug, Clone)]
pub struct Row {
    /// Primary key value(s) - can be composite
    pub primary_key: Value,
    /// Map of column names to their values
    pub columns: HashMap<String, Value>,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
}

/// Represents a database action (Insert, Update, or Delete)
#[derive(Debug, Clone)]
pub enum Action {
    /// Insert operation with new row data
    Insert(Row),
    /// Update operation with new row data
    Update(Row),
    /// Delete operation with old row data
    Delete(Row),
    /// Begin transaction marker
    Begin {
        xid: String,
        timestamp: Option<String>,
    },
    /// Commit transaction marker with nextlsn
    Commit {
        xid: String,
        nextlsn: String,
        timestamp: Option<String>,
    },
}

/// Converts a wal2json parsed value into an Action enum
///
/// This function takes the serde_json::Value returned by parse_wal2json
/// and converts it into a strongly-typed Action enum with proper Row data.
///
/// # Arguments
/// * `wal2json_value` - The parsed wal2json data as serde_json::Value
///
/// # Returns
/// * `Result<Action>` - The converted Action enum or an error
pub fn wal2json_to_psql(wal2json_value: &serde_json::Value) -> Result<Action> {
    let obj = wal2json_value
        .as_object()
        .context("wal2json value must be an object")?;

    // Get the action type
    let action_str = obj
        .get("action")
        .and_then(|v| v.as_str())
        .context("Missing or invalid 'action' field")?;

    match action_str {
        "B" => {
            // Begin transaction
            let xid = obj.get("xid").and_then(|v| v.as_str()).map(String::from);
            let timestamp = obj
                .get("timestamp")
                .and_then(|v| v.as_str())
                .map(String::from);

            // wal2json doesn't include xid in the Begin action, it's tracked externally
            Ok(Action::Begin {
                xid: xid.unwrap_or_else(|| "unknown".to_string()),
                timestamp,
            })
        }
        "C" => {
            // Commit transaction
            let xid = obj.get("xid").and_then(|v| v.as_str()).map(String::from);
            let nextlsn = obj
                .get("nextlsn")
                .and_then(|v| v.as_str())
                .context("Commit action missing 'nextlsn'")?
                .to_string();
            let timestamp = obj
                .get("timestamp")
                .and_then(|v| v.as_str())
                .map(String::from);

            Ok(Action::Commit {
                xid: xid.unwrap_or_else(|| "unknown".to_string()),
                nextlsn,
                timestamp,
            })
        }
        "I" | "U" | "D" => {
            // Data modification actions
            let schema = obj
                .get("schema")
                .and_then(|v| v.as_str())
                .unwrap_or("public")
                .to_string();

            let table = obj
                .get("table")
                .and_then(|v| v.as_str())
                .context("Missing 'table' field")?
                .to_string();

            let mut columns = HashMap::new();
            let mut primary_key_value = Value::Null;

            // DELETE actions might have different structure
            // They may have "identity" instead of "columns" or just minimal columns
            let columns_array = if action_str == "D" {
                // For DELETE, check for "identity" field first (used in some wal2json configurations)
                obj.get("identity")
                    .and_then(|v| v.as_array())
                    .or_else(|| obj.get("columns").and_then(|v| v.as_array()))
            } else {
                obj.get("columns").and_then(|v| v.as_array())
            };

            // Only process columns if they exist
            if let Some(columns_array) = columns_array {
                // Get primary key info if available
                let pk_info = obj.get("pk").and_then(|v| v.as_array());
                let pk_names: Vec<String> = pk_info
                    .map(|pks| {
                        pks.iter()
                            .filter_map(|pk| {
                                pk.get("name").and_then(|n| n.as_str()).map(String::from)
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let mut pk_values = Vec::new();

                // Process each column
                for col in columns_array {
                    let col_obj = col.as_object().context("Column entry must be an object")?;

                    let col_name = col_obj
                        .get("name")
                        .and_then(|v| v.as_str())
                        .context("Column missing 'name' field")?;

                    let col_type = col_obj
                        .get("type")
                        .and_then(|v| v.as_str())
                        .context("Column missing 'type' field")?;

                    let col_value = col_obj.get("value");

                    // Convert the value based on PostgreSQL type
                    let converted_value = convert_postgres_value(col_value, col_type)?;

                    // Check if this is a primary key column
                    if pk_names.contains(&col_name.to_string()) {
                        pk_values.push(converted_value.clone());
                    }

                    columns.insert(col_name.to_string(), converted_value);
                }

                // Set primary key value
                primary_key_value = if pk_values.len() == 1 {
                    pk_values.into_iter().next().unwrap()
                } else if pk_values.len() > 1 {
                    // Composite primary key - store as array
                    Value::Array(pk_values)
                } else if let Some(id_value) = columns.get("id") {
                    // Fallback to 'id' column if no PK info
                    id_value.clone()
                } else {
                    Value::Null
                };
            } else if action_str == "D" {
                // For DELETE with no columns, try to extract from PK info
                if let Some(pk_array) = obj.get("pk").and_then(|v| v.as_array()) {
                    if let Some(pk) = pk_array.first() {
                        if let Some(pk_name) = pk.get("name").and_then(|n| n.as_str()) {
                            if let Some(pk_value) = pk.get("value") {
                                let pk_type =
                                    pk.get("type").and_then(|t| t.as_str()).unwrap_or("text");
                                primary_key_value =
                                    convert_postgres_value(Some(pk_value), pk_type)?;
                                columns.insert(pk_name.to_string(), primary_key_value.clone());
                            }
                        }
                    }
                }
            }

            let row = Row {
                primary_key: primary_key_value,
                columns,
                schema,
                table,
            };

            match action_str {
                "I" => Ok(Action::Insert(row)),
                "U" => Ok(Action::Update(row)),
                "D" => Ok(Action::Delete(row)),
                _ => bail!("Unknown action type: {action_str}"),
            }
        }
        _ => bail!("Unknown action type: {action_str}"),
    }
}

/// Converts a PostgreSQL value from wal2json format to our Value enum
fn convert_postgres_value(value: Option<&serde_json::Value>, pg_type: &str) -> Result<Value> {
    // Handle NULL values
    let value = match value {
        Some(v) if !v.is_null() => v,
        _ => return Ok(Value::Null),
    };

    // Convert based on PostgreSQL type
    match pg_type {
        // Numeric types
        "smallint" | "int2" => {
            let val = value.as_i64().context("Failed to parse smallint")?;
            Ok(Value::SmallInt(val as i16))
        }
        "integer" | "int" | "int4" => {
            let val = value.as_i64().context("Failed to parse integer")?;
            Ok(Value::Integer(val as i32))
        }
        "bigint" | "int8" => {
            let val = value.as_i64().context("Failed to parse bigint")?;
            Ok(Value::BigInt(val))
        }
        "real" | "float4" => {
            let val = value.as_f64().context("Failed to parse real")?;
            Ok(Value::Real(val as f32))
        }
        "double precision" | "float8" => {
            let val = value.as_f64().context("Failed to parse double")?;
            Ok(Value::Double(val))
        }
        "numeric" | "decimal" => {
            // Store as string to preserve precision
            let val = if let Some(s) = value.as_str() {
                s.to_string()
            } else if let Some(n) = value.as_f64() {
                n.to_string()
            } else {
                bail!("Failed to parse numeric value")
            };
            Ok(Value::Numeric(val))
        }

        // String types
        "text" => {
            let val = value.as_str().context("Failed to parse text")?.to_string();
            Ok(Value::Text(val))
        }
        s if s.starts_with("character varying") || s.starts_with("varchar") => {
            let val = value
                .as_str()
                .context("Failed to parse varchar")?
                .to_string();
            Ok(Value::Varchar(val))
        }
        s if s.starts_with("character") || s.starts_with("char") => {
            let val = value.as_str().context("Failed to parse char")?.to_string();
            Ok(Value::Char(val))
        }

        // Boolean
        "boolean" | "bool" => {
            let val = value.as_bool().context("Failed to parse boolean")?;
            Ok(Value::Boolean(val))
        }

        // Binary
        "bytea" => {
            let hex_str = value.as_str().context("Failed to parse bytea")?;
            // wal2json returns bytea as hex string without \x prefix
            let bytes = hex::decode(hex_str).context("Failed to decode bytea hex string")?;
            Ok(Value::Bytea(bytes))
        }

        // UUID
        "uuid" => {
            let val = value.as_str().context("Failed to parse uuid")?.to_string();
            Ok(Value::Uuid(val))
        }

        // JSON types
        "json" => {
            // wal2json returns JSON as a string, we need to parse it
            let json_str = value.as_str().context("Failed to get JSON string")?;
            let parsed = serde_json::from_str(json_str).context("Failed to parse JSON")?;
            Ok(Value::Json(parsed))
        }
        "jsonb" => {
            // wal2json returns JSONB as a string, we need to parse it
            let json_str = value.as_str().context("Failed to get JSONB string")?;
            let parsed = serde_json::from_str(json_str).context("Failed to parse JSONB")?;
            Ok(Value::Jsonb(parsed))
        }

        // Date/Time types
        "timestamp" | "timestamp without time zone" => {
            let val = value
                .as_str()
                .context("Failed to parse timestamp")?
                .to_string();
            Ok(Value::Timestamp(val))
        }
        "timestamptz" | "timestamp with time zone" => {
            let val = value
                .as_str()
                .context("Failed to parse timestamptz")?
                .to_string();
            Ok(Value::TimestampTz(val))
        }
        "date" => {
            let val = value.as_str().context("Failed to parse date")?.to_string();
            Ok(Value::Date(val))
        }
        "time" | "time without time zone" => {
            let val = value.as_str().context("Failed to parse time")?.to_string();
            Ok(Value::Time(val))
        }
        "timetz" | "time with time zone" => {
            let val = value
                .as_str()
                .context("Failed to parse timetz")?
                .to_string();
            Ok(Value::TimeTz(val))
        }
        "interval" => {
            let val = value
                .as_str()
                .context("Failed to parse interval")?
                .to_string();
            Ok(Value::Interval(val))
        }

        // Array types
        s if s.ends_with("[]") => {
            // PostgreSQL array format from wal2json is like "{1,2,3}" or "{apple,banana,cherry}"
            let array_str = value.as_str().context("Failed to get array string")?;

            // Parse PostgreSQL array format
            let parsed_array = parse_postgres_array(array_str, s)?;
            Ok(Value::Array(parsed_array))
        }

        // Default fallback to text
        _ => {
            let val = value.to_string();
            Ok(Value::Text(val))
        }
    }
}

/// Parses PostgreSQL array format like "{1,2,3}" or "{apple,banana,cherry}"
fn parse_postgres_array(array_str: &str, array_type: &str) -> Result<Vec<Value>> {
    // Remove the curly braces
    let trimmed = array_str.trim_start_matches('{').trim_end_matches('}');

    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    // Determine the element type
    let element_type = array_type.trim_end_matches("[]");

    // Split by comma (simple implementation - doesn't handle nested arrays or quoted strings with commas)
    let elements: Vec<&str> = trimmed.split(',').collect();

    let mut result = Vec::new();
    for elem in elements {
        let elem = elem.trim();

        // Convert each element based on the array element type
        let value = match element_type {
            "integer" | "int" | "int4" => {
                let val = elem
                    .parse::<i32>()
                    .context("Failed to parse integer array element")?;
                Value::Integer(val)
            }
            "bigint" | "int8" => {
                let val = elem
                    .parse::<i64>()
                    .context("Failed to parse bigint array element")?;
                Value::BigInt(val)
            }
            "text" | "varchar" => Value::Text(elem.to_string()),
            "boolean" | "bool" => {
                let val = elem
                    .parse::<bool>()
                    .context("Failed to parse boolean array element")?;
                Value::Boolean(val)
            }
            _ => Value::Text(elem.to_string()),
        };

        result.push(value);
    }

    Ok(result)
}

// Add hex crate dependency for bytea handling
// Note: You'll need to add `hex = "0.4"` to Cargo.toml dependencies

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_convert_insert_action() {
        let wal2json = json!({
            "action": "I",
            "schema": "public",
            "table": "users",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "name", "type": "text", "value": "Alice"},
                {"name": "active", "type": "boolean", "value": true}
            ],
            "pk": [
                {"name": "id", "type": "integer"}
            ]
        });

        let action = wal2json_to_psql(&wal2json).unwrap();

        match action {
            Action::Insert(row) => {
                assert_eq!(row.table, "users");
                assert_eq!(row.schema, "public");
                assert_eq!(row.primary_key, Value::Integer(1));
                assert_eq!(
                    row.columns.get("name"),
                    Some(&Value::Text("Alice".to_string()))
                );
                assert_eq!(row.columns.get("active"), Some(&Value::Boolean(true)));
            }
            _ => panic!("Expected Insert action"),
        }
    }

    #[test]
    fn test_convert_array_types() {
        let wal2json = json!({
            "action": "I",
            "schema": "public",
            "table": "test",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "numbers", "type": "integer[]", "value": "{1,2,3,4,5}"},
                {"name": "fruits", "type": "text[]", "value": "{apple,banana,cherry}"}
            ]
        });

        let action = wal2json_to_psql(&wal2json).unwrap();

        match action {
            Action::Insert(row) => {
                // Check integer array
                if let Some(Value::Array(numbers)) = row.columns.get("numbers") {
                    assert_eq!(numbers.len(), 5);
                    assert_eq!(numbers[0], Value::Integer(1));
                    assert_eq!(numbers[4], Value::Integer(5));
                } else {
                    panic!("Expected integer array");
                }

                // Check text array
                if let Some(Value::Array(fruits)) = row.columns.get("fruits") {
                    assert_eq!(fruits.len(), 3);
                    assert_eq!(fruits[0], Value::Text("apple".to_string()));
                    assert_eq!(fruits[2], Value::Text("cherry".to_string()));
                } else {
                    panic!("Expected text array");
                }
            }
            _ => panic!("Expected Insert action"),
        }
    }
}
