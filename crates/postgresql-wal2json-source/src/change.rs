//! PostgreSQL change data structures and conversion from wal2json format

use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use sync_core::{UniversalType, UniversalValue};

/// Represents a database row with primary key and column data
#[derive(Debug, Clone)]
pub struct Row {
    /// Primary key value(s) - can be composite
    pub primary_key: UniversalValue,
    /// Map of column names to their values
    pub columns: HashMap<String, UniversalValue>,
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

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Insert(_) => write!(f, "Insert"),
            Action::Update(_) => write!(f, "Update"),
            Action::Delete(_) => write!(f, "Delete"),
            Action::Begin { .. } => write!(f, "Begin"),
            Action::Commit { .. } => write!(f, "Commit"),
        }
    }
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
            let mut primary_key_value = UniversalValue::Null;

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
                    let converted_value = convert_postgres_wal2json_value(col_value, col_type)?;

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
                    UniversalValue::Array {
                        elements: pk_values,
                        element_type: Box::new(UniversalType::Text),
                    }
                } else if let Some(id_value) = columns.get("id") {
                    // Fallback to 'id' column if no PK info
                    id_value.clone()
                } else {
                    UniversalValue::Null
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
                                    convert_postgres_wal2json_value(Some(pk_value), pk_type)?;
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

/// Converts a PostgreSQL value from wal2json format to UniversalValue
fn convert_postgres_wal2json_value(
    value: Option<&serde_json::Value>,
    pg_type: &str,
) -> Result<UniversalValue> {
    // Handle NULL values
    let value = match value {
        Some(v) if !v.is_null() => v,
        _ => return Ok(UniversalValue::Null),
    };

    // Convert based on PostgreSQL type
    match pg_type {
        // Numeric types
        "smallint" | "int2" => {
            let val = value.as_i64().context("Failed to parse smallint")?;
            Ok(UniversalValue::Int16(val as i16))
        }
        "integer" | "int" | "int4" => {
            let val = value.as_i64().context("Failed to parse integer")?;
            Ok(UniversalValue::Int32(val as i32))
        }
        "bigint" | "int8" => {
            let val = value.as_i64().context("Failed to parse bigint")?;
            Ok(UniversalValue::Int64(val))
        }
        "real" | "float4" => {
            let val = value.as_f64().context("Failed to parse real")?;
            Ok(UniversalValue::Float32(val as f32))
        }
        "double precision" | "float8" => {
            let val = value.as_f64().context("Failed to parse double")?;
            Ok(UniversalValue::Float64(val))
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
            Ok(UniversalValue::Decimal {
                value: val,
                precision: 38,
                scale: 10,
            })
        }

        // String types
        "text" => {
            let val = value.as_str().context("Failed to parse text")?.to_string();
            Ok(UniversalValue::Text(val))
        }
        s if s.starts_with("character varying") || s.starts_with("varchar") => {
            let val = value
                .as_str()
                .context("Failed to parse varchar")?
                .to_string();
            Ok(UniversalValue::VarChar {
                value: val,
                length: 255,
            })
        }
        s if s.starts_with("character") || s.starts_with("char") => {
            let val = value.as_str().context("Failed to parse char")?.to_string();
            let len = val.len() as u16;
            Ok(UniversalValue::Char {
                value: val,
                length: len.max(1),
            })
        }

        // Boolean
        "boolean" | "bool" => {
            let val = value.as_bool().context("Failed to parse boolean")?;
            Ok(UniversalValue::Bool(val))
        }

        // Binary
        "bytea" => {
            let hex_str = value.as_str().context("Failed to parse bytea")?;
            // wal2json returns bytea as hex string without \x prefix
            let bytes = hex::decode(hex_str).context("Failed to decode bytea hex string")?;
            Ok(UniversalValue::Bytes(bytes))
        }

        // UUID
        "uuid" => {
            let val = value.as_str().context("Failed to parse uuid")?;
            let parsed_uuid = uuid::Uuid::parse_str(val)
                .map_err(|e| anyhow::anyhow!("Failed to parse UUID '{val}': {e}"))?;
            Ok(UniversalValue::Uuid(parsed_uuid))
        }

        // JSON types
        "json" => {
            // wal2json returns JSON as a string, we need to parse it
            let json_str = value.as_str().context("Failed to get JSON string")?;
            let parsed: serde_json::Value =
                serde_json::from_str(json_str).context("Failed to parse JSON")?;
            Ok(UniversalValue::Json(Box::new(parsed)))
        }
        "jsonb" => {
            // wal2json returns JSONB as a string, we need to parse it
            let json_str = value.as_str().context("Failed to get JSONB string")?;
            let parsed: serde_json::Value =
                serde_json::from_str(json_str).context("Failed to parse JSONB")?;
            Ok(UniversalValue::Jsonb(Box::new(parsed)))
        }

        // Date/Time types
        "timestamp" | "timestamp without time zone" => {
            let val = value.as_str().context("Failed to parse timestamp")?;
            let dt = parse_timestamp(val)
                .map_err(|e| anyhow::anyhow!("Failed to parse timestamp '{val}': {e}"))?;
            Ok(UniversalValue::LocalDateTime(dt))
        }
        "timestamptz" | "timestamp with time zone" => {
            let val = value.as_str().context("Failed to parse timestamptz")?;
            let dt = parse_timestamptz(val)
                .map_err(|e| anyhow::anyhow!("Failed to parse timestamptz '{val}': {e}"))?;
            Ok(UniversalValue::ZonedDateTime(dt))
        }
        "date" => {
            let val = value.as_str().context("Failed to parse date")?;
            let dt = parse_date(val)
                .map_err(|e| anyhow::anyhow!("Failed to parse date '{val}': {e}"))?;
            Ok(UniversalValue::Date(dt))
        }
        "time" | "time without time zone" => {
            let val = value.as_str().context("Failed to parse time")?;
            let dt = parse_time(val)
                .map_err(|e| anyhow::anyhow!("Failed to parse time '{val}': {e}"))?;
            Ok(UniversalValue::Time(dt))
        }
        "timetz" | "time with time zone" => {
            let val = value.as_str().context("Failed to parse timetz")?;
            // Note: We store TIMETZ as string to preserve the original format.
            // Time and datetime are fundamentally different types - datetime implies
            // a specific point in time, while TIMETZ represents a daily recurring time
            // in a specific timezone. Using ZonedDateTime would misrepresent the semantics.
            Ok(UniversalValue::TimeTz(val.to_string()))
        }
        "interval" => {
            let val = value.as_str().context("Failed to parse interval")?;
            let dur = parse_interval(val)
                .map_err(|e| anyhow::anyhow!("Failed to parse interval '{val}': {e}"))?;
            Ok(UniversalValue::Duration(dur))
        }

        // Array types
        s if s.ends_with("[]") => {
            // PostgreSQL array format from wal2json is like "{1,2,3}" or "{apple,banana,cherry}"
            let array_str = value.as_str().context("Failed to get array string")?;

            // Parse PostgreSQL array format
            let (parsed_array, element_type) = parse_postgres_array(array_str, s)?;
            Ok(UniversalValue::Array {
                elements: parsed_array,
                element_type: Box::new(element_type),
            })
        }

        // Default fallback to text
        _ => {
            let val = value.to_string();
            Ok(UniversalValue::Text(val))
        }
    }
}

/// Parses PostgreSQL array format like "{1,2,3}" or "{apple,banana,cherry}"
fn parse_postgres_array(
    array_str: &str,
    array_type: &str,
) -> Result<(Vec<UniversalValue>, UniversalType)> {
    // Remove the curly braces
    let trimmed = array_str.trim_start_matches('{').trim_end_matches('}');

    if trimmed.is_empty() {
        // Determine element type from array type
        let element_type = array_type_to_universal_type(array_type)?;
        return Ok((Vec::new(), element_type));
    }

    // Determine the element type
    let element_type_str = array_type.trim_end_matches("[]");
    let element_type = array_type_to_universal_type(array_type)?;

    // Split by comma (simple implementation - doesn't handle nested arrays or quoted strings with commas)
    let elements: Vec<&str> = trimmed.split(',').collect();

    let mut result = Vec::new();
    for elem in elements {
        let elem = elem.trim();

        // Convert each element based on the array element type
        let value = match element_type_str {
            "integer" | "int" | "int4" => {
                let val = elem
                    .parse::<i32>()
                    .context("Failed to parse integer array element")?;
                UniversalValue::Int32(val)
            }
            "bigint" | "int8" => {
                let val = elem
                    .parse::<i64>()
                    .context("Failed to parse bigint array element")?;
                UniversalValue::Int64(val)
            }
            "smallint" | "int2" => {
                let val = elem
                    .parse::<i16>()
                    .context("Failed to parse smallint array element")?;
                UniversalValue::Int16(val)
            }
            "real" | "float4" => {
                let val = elem
                    .parse::<f32>()
                    .context("Failed to parse real array element")?;
                UniversalValue::Float32(val)
            }
            "double precision" | "float8" => {
                let val = elem
                    .parse::<f64>()
                    .context("Failed to parse double array element")?;
                UniversalValue::Float64(val)
            }
            "text" | "varchar" => UniversalValue::Text(elem.to_string()),
            "boolean" | "bool" => {
                let val = elem
                    .parse::<bool>()
                    .context("Failed to parse boolean array element")?;
                UniversalValue::Bool(val)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported array element type: '{element_type_str}'"
                ))
            }
        };

        result.push(value);
    }

    Ok((result, element_type))
}

/// Helper to determine UniversalType from PostgreSQL array type string
fn array_type_to_universal_type(array_type: &str) -> Result<UniversalType> {
    let element_type_str = array_type.trim_end_matches("[]");
    match element_type_str {
        "integer" | "int" | "int4" => Ok(UniversalType::Int32),
        "bigint" | "int8" => Ok(UniversalType::Int64),
        "smallint" | "int2" => Ok(UniversalType::Int16),
        "real" | "float4" => Ok(UniversalType::Float32),
        "double precision" | "float8" => Ok(UniversalType::Float64),
        "boolean" | "bool" => Ok(UniversalType::Bool),
        "text" | "varchar" => Ok(UniversalType::Text),
        _ => Err(anyhow::anyhow!(
            "Unsupported array element type: '{element_type_str}'"
        )),
    }
}

// ============================================================================
// Date/Time Parsing Functions
// These parse PostgreSQL date/time strings directly to chrono types,
// eliminating the need for intermediate wrapper types.
// ============================================================================

/// Parses PostgreSQL TIMESTAMP (without timezone) to DateTime<Utc>
/// Assumes the timestamp is in UTC if no timezone is specified.
///
/// Supports:
/// - ISO 8601: "2024-01-15T10:30:00Z"
/// - PostgreSQL wal2json: "2024-01-15 10:30:00", "1997-12-17 15:37:16.123456"
fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, String> {
    // Try ISO 8601 format with 'Z' first
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Ok(dt);
    }

    // Try PostgreSQL wal2json format
    let formats = [
        "%Y-%m-%d %H:%M:%S",    // 2024-01-15 10:30:00
        "%Y-%m-%d %H:%M:%S%.f", // With fractional seconds
        "%Y-%m-%dT%H:%M:%S",    // ISO 8601 without timezone
        "%Y-%m-%dT%H:%M:%S%.f", // ISO 8601 with fractional seconds, no timezone
    ];

    for format in formats {
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(s, format) {
            return Ok(DateTime::from_naive_utc_and_offset(naive_dt, Utc));
        }
    }

    Err(format!("Unable to parse timestamp: {s}"))
}

/// Parses PostgreSQL TIMESTAMPTZ (with timezone) to DateTime<Utc>
///
/// Supports:
/// - ISO 8601: "2024-01-15T10:30:00+00:00", "1997-12-17T15:37:16Z"
/// - PostgreSQL wal2json: "2024-01-15 10:30:00+00", "1997-12-17 15:37:16+00"
/// - Traditional SQL: "12/17/1997 07:37:16.00 PST"
/// - Traditional PostgreSQL: "Wed Dec 17 07:37:16 1997 PST"
fn parse_timestamptz(s: &str) -> Result<DateTime<Utc>, String> {
    // Try ISO 8601 format first
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Ok(dt);
    }

    // Try PostgreSQL wal2json format
    if let Ok(dt) = parse_postgresql_wal2json_timestamptz(s) {
        return Ok(dt);
    }

    // Try traditional SQL format: "12/17/1997 07:37:16.00 PST"
    if let Ok(dt) = parse_sql_timestamptz(s) {
        return Ok(dt);
    }

    // Try traditional PostgreSQL format: "Wed Dec 17 07:37:16 1997 PST"
    if let Ok(dt) = parse_postgresql_timestamptz(s) {
        return Ok(dt);
    }

    Err(format!("Unable to parse timestamptz: {s}"))
}

/// Parses PostgreSQL wal2json format: "2024-01-15 10:30:00+00"
fn parse_postgresql_wal2json_timestamptz(s: &str) -> Result<DateTime<Utc>, String> {
    // Normalize short timezone offset like +00 or -08 to +00:00 or -08:00
    let normalized = if s.len() > 3 {
        let tz_start = s.len() - 3;
        if s[tz_start..tz_start + 1] == *"+" || s[tz_start..tz_start + 1] == *"-" {
            format!("{s}:00")
        } else {
            s.to_string()
        }
    } else {
        s.to_string()
    };

    let formats = [
        "%Y-%m-%d %H:%M:%S%:z",    // 2024-01-15 10:30:00+00:00
        "%Y-%m-%d %H:%M:%S%z",     // 2024-01-15 10:30:00+0000
        "%Y-%m-%d %H:%M:%S%.f%:z", // With fractional seconds
        "%Y-%m-%d %H:%M:%S%.f%z",  // With fractional seconds
    ];

    for format in formats {
        if let Ok(dt) = DateTime::parse_from_str(&normalized, format) {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    Err(format!("Failed to parse PostgreSQL wal2json format: {s}"))
}

/// Parses traditional SQL timestamp format: "12/17/1997 07:37:16.00 PST"
fn parse_sql_timestamptz(s: &str) -> Result<DateTime<Utc>, String> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 3 {
        return Err("Invalid SQL timestamp format".to_string());
    }

    let date_part = parts[0];
    let time_part = parts[1];
    let tz_part = parts[2];

    let date_components: Vec<&str> = date_part.split('/').collect();
    if date_components.len() != 3 {
        return Err("Invalid date component".to_string());
    }

    let month: u32 = date_components[0].parse().map_err(|_| "Invalid month")?;
    let day: u32 = date_components[1].parse().map_err(|_| "Invalid day")?;
    let year: i32 = date_components[2].parse().map_err(|_| "Invalid year")?;

    let time_components: Vec<&str> = time_part.split(':').collect();
    if time_components.len() != 3 {
        return Err("Invalid time component".to_string());
    }

    let hour: u32 = time_components[0].parse().map_err(|_| "Invalid hour")?;
    let minute: u32 = time_components[1].parse().map_err(|_| "Invalid minute")?;
    let second: f64 = time_components[2].parse().map_err(|_| "Invalid second")?;

    let naive_date = NaiveDate::from_ymd_opt(year, month, day).ok_or("Invalid date")?;
    let naive_time = NaiveTime::from_hms_milli_opt(
        hour,
        minute,
        second as u32,
        (second.fract() * 1000.0) as u32,
    )
    .ok_or("Invalid time")?;
    let naive_datetime = NaiveDateTime::new(naive_date, naive_time);

    apply_timezone_offset(naive_datetime, tz_part)
}

/// Parses traditional PostgreSQL timestamp format: "Wed Dec 17 07:37:16 1997 PST"
fn parse_postgresql_timestamptz(s: &str) -> Result<DateTime<Utc>, String> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 6 {
        return Err("Invalid PostgreSQL timestamp format".to_string());
    }

    // parts[0] = day of week (ignored)
    let month_str = parts[1];
    let day: u32 = parts[2].parse().map_err(|_| "Invalid day")?;
    let time_str = parts[3];
    let year: i32 = parts[4].parse().map_err(|_| "Invalid year")?;
    let tz_part = parts[5];

    let month = parse_month_name(month_str)?;

    let time_components: Vec<&str> = time_str.split(':').collect();
    if time_components.len() != 3 {
        return Err("Invalid time component".to_string());
    }

    let hour: u32 = time_components[0].parse().map_err(|_| "Invalid hour")?;
    let minute: u32 = time_components[1].parse().map_err(|_| "Invalid minute")?;
    let second: u32 = time_components[2].parse().map_err(|_| "Invalid second")?;

    let naive_date = NaiveDate::from_ymd_opt(year, month, day).ok_or("Invalid date")?;
    let naive_time = NaiveTime::from_hms_opt(hour, minute, second).ok_or("Invalid time")?;
    let naive_datetime = NaiveDateTime::new(naive_date, naive_time);

    apply_timezone_offset(naive_datetime, tz_part)
}

/// Parses month name to month number
fn parse_month_name(s: &str) -> Result<u32, String> {
    match s {
        "Jan" => Ok(1),
        "Feb" => Ok(2),
        "Mar" => Ok(3),
        "Apr" => Ok(4),
        "May" => Ok(5),
        "Jun" => Ok(6),
        "Jul" => Ok(7),
        "Aug" => Ok(8),
        "Sep" => Ok(9),
        "Oct" => Ok(10),
        "Nov" => Ok(11),
        "Dec" => Ok(12),
        _ => Err(format!("Invalid month: {s}")),
    }
}

/// Applies timezone offset to naive datetime
fn apply_timezone_offset(naive_dt: NaiveDateTime, tz: &str) -> Result<DateTime<Utc>, String> {
    let offset_hours = match tz {
        "PST" => -8,
        "PDT" => -7,
        "MST" => -7,
        "MDT" => -6,
        "CST" => -6,
        "CDT" => -5,
        "EST" => -5,
        "EDT" => -4,
        "UTC" | "GMT" => 0,
        _ => return Err(format!("Unsupported timezone: {tz}")),
    };

    let utc_dt = naive_dt - chrono::Duration::hours(offset_hours);
    Ok(Utc.from_utc_datetime(&utc_dt))
}

/// Parses PostgreSQL DATE to DateTime<Utc> (at midnight UTC)
fn parse_date(s: &str) -> Result<DateTime<Utc>, String> {
    let naive_date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| format!("Failed to parse date: {e}"))?;
    let naive_datetime = naive_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| "Invalid date components".to_string())?;
    Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc))
}

/// Parses PostgreSQL TIME (without timezone) to DateTime<Utc> (using epoch date 1970-01-01)
fn parse_time(s: &str) -> Result<DateTime<Utc>, String> {
    let naive_time = NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .map_err(|e| format!("Failed to parse time: {e}"))?;
    let epoch_date =
        NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| "Invalid epoch date".to_string())?;
    let naive_datetime = epoch_date.and_time(naive_time);
    Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc))
}

/// Parses PostgreSQL INTERVAL to std::time::Duration
///
/// PostgreSQL outputs intervals in `postgres` style (the default IntervalStyle),
/// which uses a mixed format combining verbose units with HH:MM:SS:
/// - "02:30:00" (sub-day intervals)
/// - "1 day 02:30:00" (mixed format)
/// - "1 mon 2 days 03:04:05" (with months)
/// - "1 year 2 mons 3 days 04:05:06" (with years)
///
/// Note: Years are converted to 360 days (12 months × 30 days), months are 30 days.
///
/// # Important: Semantic Loss in Conversion
///
/// This conversion is **lossy** and may not preserve the semantics of PostgreSQL intervals
/// when used with other systems like SurrealDB:
///
/// - In PostgreSQL: `now() + interval '1 year'` correctly adds one calendar year
///   (accounting for leap years, etc.)
/// - After conversion: A 1-year interval becomes a fixed 360-day duration
/// - In target systems: Adding this duration may not land on the same date/month
///   one year later
///
/// Similarly, there's no way to express "exactly one month later" in systems like
/// SurrealDB that use fixed durations, since months have varying lengths (28-31 days).
///
/// **Use with caution** when converting PostgreSQL intervals to other duration systems.
/// This conversion is best suited for:
/// - Approximate time calculations
/// - Systems where calendar-aware arithmetic is not required
/// - Logging and display purposes
fn parse_interval(s: &str) -> Result<Duration, String> {
    let input = s.trim();

    // Special case: if the entire string is in HH:MM:SS format, parse it directly
    if is_pure_hms_format(input) {
        return parse_hms_component(input)
            .map(Duration::from_secs_f64)
            .ok_or_else(|| format!("Invalid HH:MM:SS format: {input}"));
    }

    // Otherwise, parse as a potentially mixed format
    let mut total_seconds: f64 = 0.0;

    let tokens: Vec<&str> = input.split_whitespace().collect();

    let mut i = 0;
    while i < tokens.len() {
        let token = tokens[i];

        // Check if this token looks like HH:MM:SS
        if is_pure_hms_format(token) {
            if let Some(seconds) = parse_hms_component(token) {
                total_seconds += seconds;
                i += 1;
                continue;
            }
        }

        // Try to parse as a number
        if let Ok(value) = token.parse::<f64>() {
            if i + 1 < tokens.len() {
                let unit = tokens[i + 1];

                // Check if the unit is actually an HH:MM:SS format
                if is_pure_hms_format(unit) {
                    // "1 02:30:00" means 1 day + 2:30:00
                    total_seconds += value * 86400.0;
                    if let Some(hms_seconds) = parse_hms_component(unit) {
                        total_seconds += hms_seconds;
                    }
                    i += 2;
                } else {
                    total_seconds += interval_unit_to_seconds(value, unit)?;
                    i += 2;
                }
            } else {
                return Err(format!("Value without unit: {token}"));
            }
        } else {
            // Not a number, might be a combined format like "6years"
            if let Some((value, unit)) = parse_combined_interval_part(token) {
                total_seconds += interval_unit_to_seconds(value, unit)?;
                i += 1;
            } else {
                return Err(format!("Invalid interval token: {token}"));
            }
        }
    }

    Ok(Duration::from_secs_f64(total_seconds))
}

/// Checks if a string is in HH:MM:SS format
fn is_pure_hms_format(s: &str) -> bool {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return false;
    }
    parts[0].parse::<f64>().is_ok()
        && parts[1].parse::<f64>().is_ok()
        && parts[2].parse::<f64>().is_ok()
}

/// Parses HH:MM:SS format into total seconds
fn parse_hms_component(s: &str) -> Option<f64> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return None;
    }

    let hours = parts[0].parse::<f64>().ok()?;
    let minutes = parts[1].parse::<f64>().ok()?;
    let seconds = parts[2].parse::<f64>().ok()?;

    Some(hours * 3600.0 + minutes * 60.0 + seconds)
}

/// Parses a combined part like "6years" into (value, unit)
fn parse_combined_interval_part(part: &str) -> Option<(f64, &str)> {
    let unit_start = part.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-')?;

    let value_str = &part[..unit_start];
    let unit_str = &part[unit_start..];

    if value_str.is_empty() || unit_str.is_empty() {
        return None;
    }

    let value: f64 = value_str.parse().ok()?;
    Some((value, unit_str))
}

/// Converts a value and unit to seconds
fn interval_unit_to_seconds(value: f64, unit: &str) -> Result<f64, String> {
    let multiplier = match unit {
        "second" | "seconds" | "sec" | "secs" | "s" => 1.0,
        "minute" | "minutes" | "min" | "mins" | "m" => 60.0,
        "hour" | "hours" | "hr" | "hrs" | "h" => 3600.0,
        "day" | "days" | "d" => 86400.0,
        "week" | "weeks" | "w" => 604800.0,
        "month" | "months" | "mon" | "mons" => 2592000.0, // 30 days
        "year" | "years" | "yr" | "yrs" | "y" => 31104000.0, // 360 days
        _ => return Err(format!("Unsupported interval unit: {unit}")),
    };

    Ok(value * multiplier)
}

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
                assert_eq!(row.primary_key, UniversalValue::Int32(1));
                assert_eq!(
                    row.columns.get("name"),
                    Some(&UniversalValue::Text("Alice".to_string()))
                );
                assert_eq!(row.columns.get("active"), Some(&UniversalValue::Bool(true)));
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
                if let Some(UniversalValue::Array { elements, .. }) = row.columns.get("numbers") {
                    assert_eq!(elements.len(), 5);
                    assert_eq!(elements[0], UniversalValue::Int32(1));
                    assert_eq!(elements[4], UniversalValue::Int32(5));
                } else {
                    panic!("Expected integer array");
                }

                // Check text array
                if let Some(UniversalValue::Array { elements, .. }) = row.columns.get("fruits") {
                    assert_eq!(elements.len(), 3);
                    assert_eq!(elements[0], UniversalValue::Text("apple".to_string()));
                    assert_eq!(elements[2], UniversalValue::Text("cherry".to_string()));
                } else {
                    panic!("Expected text array");
                }
            }
            _ => panic!("Expected Insert action"),
        }
    }

    #[test]
    fn test_convert_all_numeric_types() {
        let wal2json = json!({
            "action": "I",
            "schema": "public",
            "table": "numeric_test",
            "columns": [
                {"name": "small", "type": "smallint", "value": 123},
                {"name": "normal", "type": "integer", "value": 456789},
                {"name": "big", "type": "bigint", "value": 9223372036854775807_i64},
                {"name": "real_num", "type": "real", "value": 3.25},
                {"name": "double_num", "type": "double precision", "value": 2.567891234},
                {"name": "decimal_num", "type": "numeric", "value": "123.456"}
            ]
        });

        let action = wal2json_to_psql(&wal2json).unwrap();

        match action {
            Action::Insert(row) => {
                assert_eq!(row.columns.get("small"), Some(&UniversalValue::Int16(123)));
                assert_eq!(
                    row.columns.get("normal"),
                    Some(&UniversalValue::Int32(456789))
                );
                assert_eq!(
                    row.columns.get("big"),
                    Some(&UniversalValue::Int64(9223372036854775807))
                );
                if let Some(UniversalValue::Float32(v)) = row.columns.get("real_num") {
                    assert!((v - 3.25).abs() < 0.01);
                } else {
                    panic!("Expected Float32");
                }
                if let Some(UniversalValue::Float64(v)) = row.columns.get("double_num") {
                    assert!((v - 2.567891234).abs() < 0.0001);
                } else {
                    panic!("Expected Float64");
                }
                assert_eq!(
                    row.columns.get("decimal_num"),
                    Some(&UniversalValue::Decimal {
                        value: "123.456".to_string(),
                        precision: 38,
                        scale: 10
                    })
                );
            }
            _ => panic!("Expected Insert action"),
        }
    }

    #[test]
    fn test_convert_json_types() {
        let wal2json = json!({
            "action": "I",
            "schema": "public",
            "table": "json_test",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "json_col", "type": "json", "value": "{\"key\":\"value\"}"},
                {"name": "jsonb_col", "type": "jsonb", "value": "{\"nested\":{\"data\":123}}"}
            ]
        });

        let action = wal2json_to_psql(&wal2json).unwrap();

        match action {
            Action::Insert(row) => {
                if let Some(UniversalValue::Json(json_val)) = row.columns.get("json_col") {
                    assert_eq!(json_val.get("key").and_then(|v| v.as_str()), Some("value"));
                } else {
                    panic!("Expected Json value");
                }
                if let Some(UniversalValue::Jsonb(jsonb_val)) = row.columns.get("jsonb_col") {
                    assert_eq!(
                        jsonb_val
                            .get("nested")
                            .and_then(|n| n.get("data"))
                            .and_then(|v| v.as_i64()),
                        Some(123)
                    );
                } else {
                    panic!("Expected Jsonb value");
                }
            }
            _ => panic!("Expected Insert action"),
        }
    }

    #[test]
    fn test_convert_uuid_and_bytea() {
        let wal2json = json!({
            "action": "I",
            "schema": "public",
            "table": "binary_test",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "uuid_col", "type": "uuid", "value": "550e8400-e29b-41d4-a716-446655440000"},
                {"name": "bytea_col", "type": "bytea", "value": "48656c6c6f"}
            ]
        });

        let action = wal2json_to_psql(&wal2json).unwrap();

        match action {
            Action::Insert(row) => {
                if let Some(UniversalValue::Uuid(uuid_val)) = row.columns.get("uuid_col") {
                    assert_eq!(uuid_val.to_string(), "550e8400-e29b-41d4-a716-446655440000");
                } else {
                    panic!("Expected Uuid value");
                }
                if let Some(UniversalValue::Bytes(bytes_val)) = row.columns.get("bytea_col") {
                    assert_eq!(bytes_val, &vec![72, 101, 108, 108, 111]); // "Hello" in ASCII
                } else {
                    panic!("Expected Bytes value");
                }
            }
            _ => panic!("Expected Insert action"),
        }
    }
}
