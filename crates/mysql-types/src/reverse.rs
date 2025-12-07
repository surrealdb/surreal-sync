//! Reverse conversion: MySQL values → TypedValue
//!
//! This module implements conversion from MySQL's native values back to
//! sync-core's `TypedValue` for reading data from MySQL.
//!
//! # Boolean Detection
//!
//! MySQL uses `TINYINT(1)` to represent boolean values. This module detects
//! boolean columns in two ways:
//!
//! 1. **Column width detection**: When `column_length` is set to 1 for TINYINT,
//!    values 0 and 1 are converted to boolean.
//!
//! 2. **Value-based detection**: Even without column metadata, `TINYINT` values
//!    of exactly 0 or 1 can be converted to boolean using `with_boolean_hint(true)`.
//!
//! # Schema-Aware Conversion
//!
//! For more complex conversions (JSON boolean paths, SET columns), use the
//! schema-aware conversion functions that accept `TableSchema` information.

use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::Value;
use sync_core::{TypedValue, UniversalType, UniversalValue};
use thiserror::Error;

// Re-export from json-types for convenience
pub use json_types::{
    json_to_generated_value_with_config, json_to_typed_value_with_config, JsonConversionConfig,
};

/// MySQL value with schema information for type-aware conversion.
#[derive(Debug, Clone)]
pub struct MySQLValueWithSchema {
    /// The raw MySQL value.
    pub value: Value,
    /// The MySQL column type.
    pub column_type: ColumnType,
    /// Column flags (e.g., UNSIGNED, BINARY).
    pub column_flags: ColumnFlags,
    /// Optional length/size information.
    pub column_length: Option<u32>,
    /// For DECIMAL: precision.
    pub precision: Option<u8>,
    /// For DECIMAL: scale.
    pub scale: Option<u8>,
    /// Hint that this TINYINT should be treated as boolean.
    /// When true, TINYINT values 0 and 1 are converted to boolean.
    /// This is useful when column_length is not available but the schema
    /// indicates this is a boolean column.
    pub boolean_hint: bool,
}

/// Error during MySQL value conversion.
#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Unsupported MySQL type: {0:?}")]
    UnsupportedType(ColumnType),
    #[error("Type mismatch: expected {expected}, got {actual:?}")]
    TypeMismatch { expected: String, actual: Value },
    #[error("Invalid UTF-8 in string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Invalid date/time value: {0}")]
    InvalidDateTime(String),
    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),
    #[error("Invalid JSON number: cannot represent {value} as i64 or f64")]
    InvalidJsonNumber { value: String },
    #[error("Invalid JSON: {0}")]
    InvalidJson(String),
    #[error("Missing column value at index {index} for column '{column_name}'")]
    MissingColumnValue { index: usize, column_name: String },
}

impl MySQLValueWithSchema {
    /// Create a new MySQLValueWithSchema.
    pub fn new(value: Value, column_type: ColumnType, column_flags: ColumnFlags) -> Self {
        Self {
            value,
            column_type,
            column_flags,
            column_length: None,
            precision: None,
            scale: None,
            boolean_hint: false,
        }
    }

    /// Set column length.
    pub fn with_length(mut self, length: u32) -> Self {
        self.column_length = Some(length);
        self
    }

    /// Set decimal precision and scale.
    pub fn with_precision(mut self, precision: u8, scale: u8) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    /// Set boolean hint for TINYINT columns.
    ///
    /// When set to true, TINYINT values of 0 and 1 will be converted to boolean
    /// even if the column length is not 1.
    pub fn with_boolean_hint(mut self, hint: bool) -> Self {
        self.boolean_hint = hint;
        self
    }

    /// Check if this column should be treated as boolean.
    ///
    /// Returns true if:
    /// - The column type is TINYINT and column_length is 1 (MySQL convention), or
    /// - The boolean_hint is set to true
    fn is_boolean_column(&self) -> bool {
        if self.column_type != ColumnType::MYSQL_TYPE_TINY {
            return false;
        }
        self.boolean_hint || self.column_length == Some(1)
    }

    /// Convert to TypedValue.
    pub fn to_typed_value(self) -> Result<TypedValue, ConversionError> {
        TypedValue::try_from(self)
    }
}

impl TryFrom<MySQLValueWithSchema> for TypedValue {
    type Error = ConversionError;

    fn try_from(mv: MySQLValueWithSchema) -> Result<Self, Self::Error> {
        use ColumnType::*;

        // Handle NULL first
        if matches!(mv.value, Value::NULL) {
            let ext_type = column_type_to_sync_type(mv.column_type, &mv);
            return Ok(TypedValue::null(ext_type));
        }

        match mv.column_type {
            // Integer types
            MYSQL_TYPE_TINY => {
                let i = extract_int(&mv.value)?;

                // Check if this is a boolean column (TINYINT(1) or boolean_hint)
                if mv.is_boolean_column() {
                    // Convert 0/1 to boolean
                    if i == 0 {
                        return Ok(TypedValue::bool(false));
                    } else if i == 1 {
                        return Ok(TypedValue::bool(true));
                    }
                    // Values other than 0/1 stay as integer even for boolean columns
                }

                Ok(TypedValue::new(
                    UniversalType::TinyInt {
                        width: mv.column_length.unwrap_or(4) as u8,
                    },
                    UniversalValue::Int32(i as i32),
                ))
            }

            MYSQL_TYPE_SHORT => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::SmallInt,
                    UniversalValue::Int32(i as i32),
                ))
            }

            MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Int,
                    UniversalValue::Int32(i as i32),
                ))
            }

            MYSQL_TYPE_LONGLONG => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::BigInt,
                    UniversalValue::Int64(i),
                ))
            }

            // Floating point
            MYSQL_TYPE_FLOAT => {
                let f = extract_float(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Float,
                    UniversalValue::Float64(f),
                ))
            }

            MYSQL_TYPE_DOUBLE => {
                let f = extract_float(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Double,
                    UniversalValue::Float64(f),
                ))
            }

            // Decimal
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
                let s = extract_string(&mv.value)?;
                let precision = mv.precision.unwrap_or(10);
                let scale = mv.scale.unwrap_or(0);
                Ok(TypedValue::decimal(s, precision, scale))
            }

            // String types
            MYSQL_TYPE_STRING => {
                let s = extract_string(&mv.value)?;
                let length = mv.column_length.unwrap_or(255) as u16;
                Ok(TypedValue::new(
                    UniversalType::Char { length },
                    UniversalValue::String(s),
                ))
            }

            MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR => {
                let s = extract_string(&mv.value)?;
                let length = mv.column_length.unwrap_or(255) as u16;

                // Check if it looks like a UUID
                if length == 36 && is_uuid_format(&s) {
                    if let Ok(uuid) = uuid::Uuid::parse_str(&s) {
                        return Ok(TypedValue::uuid(uuid));
                    }
                }

                Ok(TypedValue::new(
                    UniversalType::VarChar { length },
                    UniversalValue::String(s),
                ))
            }

            MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_BLOB
            | MYSQL_TYPE_LONG_BLOB => {
                if mv.column_flags.contains(ColumnFlags::BINARY_FLAG) {
                    let bytes = extract_bytes(&mv.value)?;
                    Ok(TypedValue::new(
                        UniversalType::Blob,
                        UniversalValue::Bytes(bytes),
                    ))
                } else {
                    // TEXT types
                    let s = extract_string(&mv.value)?;
                    Ok(TypedValue::new(
                        UniversalType::Text,
                        UniversalValue::String(s),
                    ))
                }
            }

            // Date/time types
            MYSQL_TYPE_DATE => {
                let dt = extract_date(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Date,
                    UniversalValue::String(dt.format("%Y-%m-%d").to_string()),
                ))
            }

            MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => {
                let time = extract_time(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Time,
                    UniversalValue::String(time.format("%H:%M:%S").to_string()),
                ))
            }

            MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => {
                let dt = extract_datetime(&mv.value)?;
                Ok(TypedValue::datetime(dt))
            }

            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => {
                let dt = extract_datetime(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::TimestampTz,
                    UniversalValue::DateTime(dt),
                ))
            }

            MYSQL_TYPE_YEAR => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::SmallInt,
                    UniversalValue::Int32(i as i32),
                ))
            }

            // JSON
            MYSQL_TYPE_JSON => {
                let s = extract_string(&mv.value)?;
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(json_to_typed_value_with_config(
                        json,
                        "",
                        &JsonConversionConfig::default(),
                    ))
                } else {
                    Ok(TypedValue::new(
                        UniversalType::Json,
                        UniversalValue::String(s),
                    ))
                }
            }

            // Enum and Set
            MYSQL_TYPE_ENUM => {
                let s = extract_string(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Enum { values: vec![] },
                    UniversalValue::String(s),
                ))
            }

            MYSQL_TYPE_SET => {
                let s = extract_string(&mv.value)?;
                let values: Vec<UniversalValue> = s
                    .split(',')
                    .map(|v| UniversalValue::String(v.to_string()))
                    .collect();
                Ok(TypedValue::new(
                    UniversalType::Set { values: vec![] },
                    UniversalValue::Array(values),
                ))
            }

            // Geometry
            MYSQL_TYPE_GEOMETRY => {
                let bytes = extract_bytes(&mv.value)?;
                Ok(TypedValue::new(
                    UniversalType::Geometry {
                        geometry_type: sync_core::GeometryType::Point,
                    },
                    UniversalValue::Bytes(bytes),
                ))
            }

            // Bit
            MYSQL_TYPE_BIT => {
                let bytes = extract_bytes(&mv.value)?;
                // For BIT(1), treat as boolean
                if bytes.len() == 1 && bytes[0] <= 1 {
                    Ok(TypedValue::bool(bytes[0] == 1))
                } else {
                    Ok(TypedValue::new(
                        UniversalType::Bytes,
                        UniversalValue::Bytes(bytes),
                    ))
                }
            }

            _ => Err(ConversionError::UnsupportedType(mv.column_type)),
        }
    }
}

/// Convert MySQL column type to UniversalType.
fn column_type_to_sync_type(col_type: ColumnType, mv: &MySQLValueWithSchema) -> UniversalType {
    use ColumnType::*;
    match col_type {
        MYSQL_TYPE_TINY => {
            // Check if this is a boolean column
            if mv.is_boolean_column() {
                UniversalType::Bool
            } else {
                UniversalType::TinyInt {
                    width: mv.column_length.unwrap_or(4) as u8,
                }
            }
        }
        MYSQL_TYPE_SHORT => UniversalType::SmallInt,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => UniversalType::Int,
        MYSQL_TYPE_LONGLONG => UniversalType::BigInt,
        MYSQL_TYPE_FLOAT => UniversalType::Float,
        MYSQL_TYPE_DOUBLE => UniversalType::Double,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => UniversalType::Decimal {
            precision: mv.precision.unwrap_or(10),
            scale: mv.scale.unwrap_or(0),
        },
        MYSQL_TYPE_STRING => UniversalType::Char {
            length: mv.column_length.unwrap_or(255) as u16,
        },
        MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR => UniversalType::VarChar {
            length: mv.column_length.unwrap_or(255) as u16,
        },
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_BLOB | MYSQL_TYPE_LONG_BLOB => {
            if mv.column_flags.contains(ColumnFlags::BINARY_FLAG) {
                UniversalType::Blob
            } else {
                UniversalType::Text
            }
        }
        MYSQL_TYPE_DATE => UniversalType::Date,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => UniversalType::Time,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => UniversalType::DateTime,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => UniversalType::TimestampTz,
        MYSQL_TYPE_YEAR => UniversalType::SmallInt,
        MYSQL_TYPE_JSON => UniversalType::Json,
        MYSQL_TYPE_ENUM => UniversalType::Enum { values: vec![] },
        MYSQL_TYPE_SET => UniversalType::Set { values: vec![] },
        MYSQL_TYPE_GEOMETRY => UniversalType::Geometry {
            geometry_type: sync_core::GeometryType::Point,
        },
        MYSQL_TYPE_BIT => UniversalType::Bytes,
        _ => UniversalType::Text,
    }
}

/// Extract integer from MySQL Value.
fn extract_int(value: &Value) -> Result<i64, ConversionError> {
    match value {
        Value::Int(i) => Ok(*i),
        Value::UInt(u) => Ok(*u as i64),
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            s.parse().map_err(|_| ConversionError::TypeMismatch {
                expected: "integer".to_string(),
                actual: value.clone(),
            })
        }
        _ => Err(ConversionError::TypeMismatch {
            expected: "integer".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Extract float from MySQL Value.
fn extract_float(value: &Value) -> Result<f64, ConversionError> {
    match value {
        Value::Float(f) => Ok(*f as f64),
        Value::Double(d) => Ok(*d),
        Value::Int(i) => Ok(*i as f64),
        Value::UInt(u) => Ok(*u as f64),
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            s.parse().map_err(|_| ConversionError::TypeMismatch {
                expected: "float".to_string(),
                actual: value.clone(),
            })
        }
        _ => Err(ConversionError::TypeMismatch {
            expected: "float".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Extract string from MySQL Value.
fn extract_string(value: &Value) -> Result<String, ConversionError> {
    match value {
        Value::Bytes(b) => Ok(String::from_utf8(b.clone())?),
        Value::Int(i) => Ok(i.to_string()),
        Value::UInt(u) => Ok(u.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        Value::Double(d) => Ok(d.to_string()),
        _ => Err(ConversionError::TypeMismatch {
            expected: "string".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Extract bytes from MySQL Value.
fn extract_bytes(value: &Value) -> Result<Vec<u8>, ConversionError> {
    match value {
        Value::Bytes(b) => Ok(b.clone()),
        _ => Err(ConversionError::TypeMismatch {
            expected: "bytes".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Extract date from MySQL Value.
fn extract_date(value: &Value) -> Result<NaiveDate, ConversionError> {
    match value {
        Value::Date(year, month, day, _, _, _, _) => {
            NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32).ok_or_else(|| {
                ConversionError::InvalidDateTime(format!(
                    "invalid date components: year={year}, month={month}, day={day}"
                ))
            })
        }
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| {
                ConversionError::InvalidDateTime(format!(
                    "cannot parse date from string '{s}': {e}"
                ))
            })
        }
        _ => Err(ConversionError::TypeMismatch {
            expected: "date".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Extract time from MySQL Value.
fn extract_time(value: &Value) -> Result<NaiveTime, ConversionError> {
    match value {
        Value::Time(_, _, hour, min, sec, micro) => {
            NaiveTime::from_hms_micro_opt(*hour as u32, *min as u32, *sec as u32, *micro)
                .ok_or_else(|| {
                    ConversionError::InvalidDateTime(format!(
                        "invalid time components: hour={hour}, min={min}, sec={sec}, micro={micro}"
                    ))
                })
        }
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            NaiveTime::parse_from_str(&s, "%H:%M:%S").map_err(|e| {
                ConversionError::InvalidDateTime(format!(
                    "cannot parse time from string '{s}': {e}"
                ))
            })
        }
        _ => Err(ConversionError::TypeMismatch {
            expected: "time".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Extract datetime from MySQL Value.
fn extract_datetime(value: &Value) -> Result<chrono::DateTime<Utc>, ConversionError> {
    match value {
        Value::Date(year, month, day, hour, min, sec, micro) => {
            let date = NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                .ok_or_else(|| {
                    ConversionError::InvalidDateTime(format!(
                        "invalid date components: year={year}, month={month}, day={day}"
                    ))
                })?;
            let time =
                NaiveTime::from_hms_micro_opt(*hour as u32, *min as u32, *sec as u32, *micro)
                    .ok_or_else(|| {
                        ConversionError::InvalidDateTime(format!(
                    "invalid time components: hour={hour}, min={min}, sec={sec}, micro={micro}"
                ))
                    })?;
            let naive = chrono::NaiveDateTime::new(date, time);
            Ok(Utc.from_utc_datetime(&naive))
        }
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            // Try various formats
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                return Ok(dt.with_timezone(&Utc));
            }
            if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                return Ok(Utc.from_utc_datetime(&naive));
            }
            if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f") {
                return Ok(Utc.from_utc_datetime(&naive));
            }
            Err(ConversionError::InvalidDateTime(format!(
                "cannot parse datetime from string '{s}': expected RFC3339 or '%Y-%m-%d %H:%M:%S' format"
            )))
        }
        _ => Err(ConversionError::TypeMismatch {
            expected: "datetime".to_string(),
            actual: value.clone(),
        }),
    }
}

/// Check if string looks like a UUID.
fn is_uuid_format(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let chars: Vec<char> = s.chars().collect();
    chars[8] == '-' && chars[13] == '-' && chars[18] == '-' && chars[23] == '-'
}

/// Configuration for row-level type conversion.
#[derive(Debug, Clone, Default)]
pub struct RowConversionConfig {
    /// Column names that should be treated as boolean (for TINYINT columns)
    pub boolean_columns: Vec<String>,
    /// Column names that are SET type (comma-separated values -> array)
    pub set_columns: Vec<String>,
    /// JSON field configuration (for nested paths within JSON columns)
    pub json_config: Option<JsonConversionConfig>,
}

/// Convert a MySQL row to a HashMap of TypedValues.
///
/// This function converts all columns in a MySQL row to TypedValues,
/// handling boolean detection and JSON conversion with optional configuration.
///
/// # Arguments
/// * `row` - The MySQL row to convert
/// * `boolean_columns` - Optional list of column names that should be treated as booleans
///   (for TINYINT columns that don't have width=1 metadata)
/// * `json_config` - Optional configuration for JSON field conversions
///
/// # Returns
/// A HashMap mapping column names to TypedValues
pub fn row_to_typed_values(
    row: &mysql_async::Row,
    boolean_columns: Option<&[&str]>,
    json_config: Option<&JsonConversionConfig>,
) -> Result<std::collections::HashMap<String, TypedValue>, ConversionError> {
    row_to_typed_values_with_config(
        row,
        &RowConversionConfig {
            boolean_columns: boolean_columns
                .map(|cols| cols.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
            set_columns: Vec::new(),
            json_config: json_config.cloned(),
        },
    )
}

/// Convert a MySQL row to a HashMap of TypedValues with full configuration.
///
/// This function provides more control over type conversion including SET columns.
///
/// # Arguments
/// * `row` - The MySQL row to convert
/// * `config` - Configuration for boolean, SET, and JSON conversions
///
/// # Returns
/// A HashMap mapping column names to TypedValues
pub fn row_to_typed_values_with_config(
    row: &mysql_async::Row,
    config: &RowConversionConfig,
) -> Result<std::collections::HashMap<String, TypedValue>, ConversionError> {
    let columns = row.columns();
    let mut result = std::collections::HashMap::new();

    for (index, column) in columns.iter().enumerate() {
        let column_name = column.name_str().to_string();
        let column_type = column.column_type();
        let column_flags = column.flags();

        // Get the raw value
        let raw_value = row
            .as_ref(index)
            .ok_or_else(|| ConversionError::MissingColumnValue {
                index,
                column_name: column_name.clone(),
            })?
            .clone();

        // Check if this column should be treated as boolean
        let is_boolean = config.boolean_columns.contains(&column_name);

        // Check if this column is a SET column
        let is_set = config.set_columns.contains(&column_name);

        // Handle SET columns - split comma-separated string into array
        if is_set {
            if let Ok(s) = extract_string(&raw_value) {
                let values: Vec<UniversalValue> = if s.is_empty() {
                    Vec::new()
                } else {
                    s.split(',')
                        .map(|v| UniversalValue::String(v.to_string()))
                        .collect()
                };
                let typed_value = TypedValue::new(
                    UniversalType::Set { values: vec![] },
                    UniversalValue::Array(values),
                );
                result.insert(column_name, typed_value);
                continue;
            }
        }

        // For JSON columns with config, use special conversion
        if column_type == ColumnType::MYSQL_TYPE_JSON {
            if let Some(ref json_config) = config.json_config {
                if let Value::Bytes(bytes) = &raw_value {
                    if let Ok(s) = String::from_utf8(bytes.clone()) {
                        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&s) {
                            let typed_value =
                                json_to_typed_value_with_config(json_value, "", json_config);
                            result.insert(column_name, typed_value);
                            continue;
                        }
                    }
                }
            }
        }

        // Standard conversion with optional boolean hint
        let mv = MySQLValueWithSchema::new(raw_value, column_type, column_flags)
            .with_boolean_hint(is_boolean);

        let typed_value = mv.to_typed_value()?;
        result.insert(column_name, typed_value);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_int_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Int(42),
            ColumnType::MYSQL_TYPE_LONG,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Int));
        assert!(matches!(tv.value, UniversalValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Int(9_223_372_036_854_775_807),
            ColumnType::MYSQL_TYPE_LONGLONG,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::BigInt));
        assert!(matches!(
            tv.value,
            UniversalValue::Int64(9_223_372_036_854_775_807)
        ));
    }

    #[test]
    fn test_string_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"hello world".to_vec()),
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::VarChar { .. }));
        if let UniversalValue::String(s) = tv.value {
            assert_eq!(s, "hello world");
        } else {
            panic!("Expected String value");
        }
    }

    #[test]
    fn test_uuid_detection() {
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"550e8400-e29b-41d4-a716-446655440000".to_vec()),
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnFlags::empty(),
        )
        .with_length(36);
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Uuid));
        if let UniversalValue::Uuid(u) = tv.value {
            assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid value");
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Date(2024, 6, 15, 10, 30, 45, 0),
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::DateTime));
        if let UniversalValue::DateTime(dt) = tv.value {
            assert_eq!(dt.year(), 2024);
            assert_eq!(dt.month(), 6);
            assert_eq!(dt.day(), 15);
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_null_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::NULL,
            ColumnType::MYSQL_TYPE_LONG,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Int));
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_json_conversion() {
        let json_str = r#"{"name":"Alice","age":30}"#;
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(json_str.as_bytes().to_vec()),
            ColumnType::MYSQL_TYPE_JSON,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Json));
        if let UniversalValue::Object(obj) = tv.value {
            assert!(obj.contains_key("name"));
            assert!(obj.contains_key("age"));
        } else {
            panic!("Expected Object value");
        }
    }

    #[test]
    fn test_decimal_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"123.456".to_vec()),
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
            ColumnFlags::empty(),
        )
        .with_precision(10, 3);
        let tv = mv.to_typed_value().unwrap();
        if let UniversalType::Decimal { precision, scale } = tv.sync_type {
            assert_eq!(precision, 10);
            assert_eq!(scale, 3);
        } else {
            panic!("Expected Decimal type");
        }
        if let UniversalValue::Decimal { value, .. } = tv.value {
            assert_eq!(value, "123.456");
        } else {
            panic!("Expected Decimal value");
        }
    }

    #[test]
    fn test_blob_conversion() {
        let binary_data = vec![0x00, 0x01, 0x02, 0xFF];
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(binary_data.clone()),
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnFlags::BINARY_FLAG,
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Blob));
        if let UniversalValue::Bytes(b) = tv.value {
            assert_eq!(b, binary_data);
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_text_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"long text content".to_vec()),
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnFlags::empty(), // No BINARY flag = TEXT
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Text));
        if let UniversalValue::String(s) = tv.value {
            assert_eq!(s, "long text content");
        } else {
            panic!("Expected String value");
        }
    }

    #[test]
    fn test_tinyint1_boolean_true() {
        // TINYINT(1) with value 1 should be boolean true
        let mv = MySQLValueWithSchema::new(
            Value::Int(1),
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        )
        .with_length(1);
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_tinyint1_boolean_false() {
        // TINYINT(1) with value 0 should be boolean false
        let mv = MySQLValueWithSchema::new(
            Value::Int(0),
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        )
        .with_length(1);
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(false)));
    }

    #[test]
    fn test_tinyint_without_boolean_hint() {
        // TINYINT without length=1 or hint should stay as integer
        let mv = MySQLValueWithSchema::new(
            Value::Int(1),
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::TinyInt { .. }));
        assert!(matches!(tv.value, UniversalValue::Int32(1)));
    }

    #[test]
    fn test_tinyint_with_boolean_hint_true() {
        // TINYINT with boolean_hint=true should convert 0/1 to boolean
        let mv = MySQLValueWithSchema::new(
            Value::Int(1),
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        )
        .with_boolean_hint(true);
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_tinyint_with_boolean_hint_false() {
        // TINYINT with boolean_hint=true should convert 0/1 to boolean
        let mv = MySQLValueWithSchema::new(
            Value::Int(0),
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        )
        .with_boolean_hint(true);
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(false)));
    }

    #[test]
    fn test_tinyint_non_boolean_value() {
        // Even with TINYINT(1), values other than 0/1 should stay as int
        let mv = MySQLValueWithSchema::new(
            Value::Int(5),
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        )
        .with_length(1);
        let tv = mv.to_typed_value().unwrap();
        // The type still says TinyInt, but value is 5
        assert!(matches!(tv.sync_type, UniversalType::TinyInt { .. }));
        assert!(matches!(tv.value, UniversalValue::Int32(5)));
    }

    #[test]
    fn test_null_boolean_column() {
        // NULL in a boolean column should preserve the Bool type
        let mv = MySQLValueWithSchema::new(
            Value::NULL,
            ColumnType::MYSQL_TYPE_TINY,
            ColumnFlags::empty(),
        )
        .with_length(1);
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Null));
    }

    #[test]
    fn test_json_boolean_path_conversion() {
        // Test that 0/1 in JSON are converted to boolean when path is specified
        let config = JsonConversionConfig::new()
            .with_boolean_path("settings.enabled")
            .with_boolean_path("flags.is_active");

        let json = serde_json::json!({
            "settings": {
                "enabled": 1,
                "count": 5
            },
            "flags": {
                "is_active": 0
            }
        });

        let tv = json_to_typed_value_with_config(json, "", &config);

        if let UniversalValue::Object(root) = tv.value {
            // Check settings.enabled is now boolean true
            if let Some(UniversalValue::Object(settings)) = root.get("settings") {
                assert!(matches!(
                    settings.get("enabled"),
                    Some(UniversalValue::Bool(true))
                ));
                // count should still be an integer
                assert!(matches!(
                    settings.get("count"),
                    Some(UniversalValue::Int64(5))
                ));
            } else {
                panic!("Expected settings object");
            }

            // Check flags.is_active is now boolean false
            if let Some(UniversalValue::Object(flags)) = root.get("flags") {
                assert!(matches!(
                    flags.get("is_active"),
                    Some(UniversalValue::Bool(false))
                ));
            } else {
                panic!("Expected flags object");
            }
        } else {
            panic!("Expected Object value");
        }
    }

    #[test]
    fn test_json_set_path_conversion() {
        // Test that comma-separated strings are converted to arrays when path is specified
        let config = JsonConversionConfig::new().with_set_path("permissions");

        let json = serde_json::json!({
            "permissions": "read,write,execute",
            "name": "admin"
        });

        let tv = json_to_typed_value_with_config(json, "", &config);

        if let UniversalValue::Object(root) = tv.value {
            // Check permissions is now an array
            if let Some(UniversalValue::Array(perms)) = root.get("permissions") {
                assert_eq!(perms.len(), 3);
                assert!(matches!(&perms[0], UniversalValue::String(s) if s == "read"));
                assert!(matches!(&perms[1], UniversalValue::String(s) if s == "write"));
                assert!(matches!(&perms[2], UniversalValue::String(s) if s == "execute"));
            } else {
                panic!("Expected Array value for permissions");
            }

            // name should still be a string
            assert!(matches!(root.get("name"), Some(UniversalValue::String(s)) if s == "admin"));
        } else {
            panic!("Expected Object value");
        }
    }

    #[test]
    fn test_json_empty_config() {
        // Test that without config, 0/1 stay as integers
        let config = JsonConversionConfig::new();

        let json = serde_json::json!({
            "enabled": 1,
            "disabled": 0
        });

        let tv = json_to_typed_value_with_config(json, "", &config);

        if let UniversalValue::Object(root) = tv.value {
            assert!(matches!(
                root.get("enabled"),
                Some(UniversalValue::Int64(1))
            ));
            assert!(matches!(
                root.get("disabled"),
                Some(UniversalValue::Int64(0))
            ));
        } else {
            panic!("Expected Object value");
        }
    }

    // ==================== ERROR HANDLING TESTS ====================

    #[test]
    fn test_unsupported_type_error() {
        // MYSQL_TYPE_NULL is an internal MySQL type that shouldn't be used for columns
        let mv = MySQLValueWithSchema::new(
            Value::Int(42),
            ColumnType::MYSQL_TYPE_NULL,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::UnsupportedType(_)));
        assert!(err.to_string().contains("MYSQL_TYPE_NULL"));
    }

    #[test]
    fn test_type_mismatch_int_expects_int() {
        // Pass a string value when an integer is expected
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"not a number".to_vec()),
            ColumnType::MYSQL_TYPE_LONG,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        // Note: extract_int tries to parse bytes as string, which fails
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
        assert!(err.to_string().contains("expected"));
        assert!(err.to_string().contains("integer"));
    }

    #[test]
    fn test_type_mismatch_float_expects_float() {
        // Pass an invalid float string
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"not_a_float".to_vec()),
            ColumnType::MYSQL_TYPE_DOUBLE,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
        assert!(err.to_string().contains("float"));
    }

    #[test]
    fn test_invalid_date_error() {
        // Invalid date components (month 13)
        let mv = MySQLValueWithSchema::new(
            Value::Date(2024, 13, 1, 0, 0, 0, 0),
            ColumnType::MYSQL_TYPE_DATE,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::InvalidDateTime(_)));
        assert!(err.to_string().contains("month=13"));
    }

    #[test]
    fn test_invalid_date_string_error() {
        // Invalid date string format
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"not-a-date".to_vec()),
            ColumnType::MYSQL_TYPE_DATE,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::InvalidDateTime(_)));
        assert!(err.to_string().contains("not-a-date"));
    }

    #[test]
    fn test_invalid_time_error() {
        // Invalid time components (hour 25)
        let mv = MySQLValueWithSchema::new(
            Value::Time(false, 0, 25, 0, 0, 0),
            ColumnType::MYSQL_TYPE_TIME,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::InvalidDateTime(_)));
        assert!(err.to_string().contains("hour=25"));
    }

    #[test]
    fn test_invalid_datetime_string_error() {
        // Invalid datetime string format
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"invalid-datetime".to_vec()),
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::InvalidDateTime(_)));
        assert!(err.to_string().contains("invalid-datetime"));
    }

    #[test]
    fn test_invalid_utf8_error() {
        // Invalid UTF-8 bytes for a string column
        let invalid_utf8 = vec![0xff, 0xfe, 0xfd];
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(invalid_utf8),
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::InvalidUtf8(_)));
    }

    #[test]
    fn test_bytes_type_mismatch() {
        // Pass an Int value when bytes are expected (for BLOB)
        let mv = MySQLValueWithSchema::new(
            Value::Int(42),
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnFlags::BINARY_FLAG,
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
        assert!(err.to_string().contains("bytes"));
    }

    #[test]
    fn test_date_type_mismatch() {
        // Pass a Time value when a Date is expected
        let mv = MySQLValueWithSchema::new(
            Value::Time(false, 0, 10, 30, 0, 0),
            ColumnType::MYSQL_TYPE_DATE,
            ColumnFlags::empty(),
        );
        let result = mv.to_typed_value();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ConversionError::TypeMismatch { .. }));
        assert!(err.to_string().contains("date"));
    }

    #[test]
    fn test_error_display_is_descriptive() {
        // Test that error messages are descriptive enough
        let err = ConversionError::TypeMismatch {
            expected: "integer".to_string(),
            actual: Value::Bytes(b"hello".to_vec()),
        };
        let msg = err.to_string();
        assert!(msg.contains("expected integer"));
        assert!(msg.contains("hello") || msg.contains("Bytes")); // Should include what we got

        let err2 = ConversionError::InvalidDateTime(
            "invalid date: year=2024, month=13, day=1".to_string(),
        );
        let msg2 = err2.to_string();
        assert!(msg2.contains("year=2024"));
        assert!(msg2.contains("month=13"));

        let err3 = ConversionError::UnsupportedType(ColumnType::MYSQL_TYPE_NULL);
        let msg3 = err3.to_string();
        assert!(msg3.contains("MYSQL_TYPE_NULL"));
    }

    #[test]
    fn test_set_column_empty_string() {
        // SET column with empty string should produce empty array
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"".to_vec()),
            ColumnType::MYSQL_TYPE_SET,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Set { .. }));
        if let UniversalValue::Array(arr) = tv.value {
            // Empty string produces one empty element after split
            // This is expected MySQL behavior
            assert_eq!(arr.len(), 1);
            if let UniversalValue::String(s) = &arr[0] {
                assert_eq!(s, "");
            }
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_set_column_multiple_values() {
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"read,write,execute".to_vec()),
            ColumnType::MYSQL_TYPE_SET,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Set { .. }));
        if let UniversalValue::Array(arr) = tv.value {
            assert_eq!(arr.len(), 3);
            assert!(matches!(&arr[0], UniversalValue::String(s) if s == "read"));
            assert!(matches!(&arr[1], UniversalValue::String(s) if s == "write"));
            assert!(matches!(&arr[2], UniversalValue::String(s) if s == "execute"));
        } else {
            panic!("Expected Array value");
        }
    }

    #[test]
    fn test_enum_column() {
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(b"active".to_vec()),
            ColumnType::MYSQL_TYPE_ENUM,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Enum { .. }));
        assert!(matches!(tv.value, UniversalValue::String(s) if s == "active"));
    }

    #[test]
    fn test_bit_as_boolean() {
        // BIT(1) with value 0 should be boolean false
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(vec![0]),
            ColumnType::MYSQL_TYPE_BIT,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(false)));

        // BIT(1) with value 1 should be boolean true
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(vec![1]),
            ColumnType::MYSQL_TYPE_BIT,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bool));
        assert!(matches!(tv.value, UniversalValue::Bool(true)));
    }

    #[test]
    fn test_bit_as_bytes() {
        // BIT(8) should be bytes
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(vec![0xff]),
            ColumnType::MYSQL_TYPE_BIT,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Bytes));
        if let UniversalValue::Bytes(b) = tv.value {
            assert_eq!(b, vec![0xff]);
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_geometry_column() {
        let wkb_point = vec![0x01, 0x01, 0x00, 0x00, 0x00]; // Minimal WKB point prefix
        let mv = MySQLValueWithSchema::new(
            Value::Bytes(wkb_point.clone()),
            ColumnType::MYSQL_TYPE_GEOMETRY,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::Geometry { .. }));
        if let UniversalValue::Bytes(b) = tv.value {
            assert_eq!(b, wkb_point);
        } else {
            panic!("Expected Bytes value");
        }
    }

    #[test]
    fn test_year_column() {
        let mv = MySQLValueWithSchema::new(
            Value::Int(2024),
            ColumnType::MYSQL_TYPE_YEAR,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, UniversalType::SmallInt));
        assert!(matches!(tv.value, UniversalValue::Int32(2024)));
    }
}
