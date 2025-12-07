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
use sync_core::{GeneratedValue, SyncDataType, TypedValue};
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
    #[error("Invalid date/time value")]
    InvalidDateTime,
    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),
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
                    SyncDataType::TinyInt {
                        width: mv.column_length.unwrap_or(4) as u8,
                    },
                    GeneratedValue::Int32(i as i32),
                ))
            }

            MYSQL_TYPE_SHORT => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::SmallInt,
                    GeneratedValue::Int32(i as i32),
                ))
            }

            MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Int,
                    GeneratedValue::Int32(i as i32),
                ))
            }

            MYSQL_TYPE_LONGLONG => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::BigInt,
                    GeneratedValue::Int64(i),
                ))
            }

            // Floating point
            MYSQL_TYPE_FLOAT => {
                let f = extract_float(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Float,
                    GeneratedValue::Float64(f),
                ))
            }

            MYSQL_TYPE_DOUBLE => {
                let f = extract_float(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Double,
                    GeneratedValue::Float64(f),
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
                    SyncDataType::Char { length },
                    GeneratedValue::String(s),
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
                    SyncDataType::VarChar { length },
                    GeneratedValue::String(s),
                ))
            }

            MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_BLOB
            | MYSQL_TYPE_LONG_BLOB => {
                if mv.column_flags.contains(ColumnFlags::BINARY_FLAG) {
                    let bytes = extract_bytes(&mv.value)?;
                    Ok(TypedValue::new(
                        SyncDataType::Blob,
                        GeneratedValue::Bytes(bytes),
                    ))
                } else {
                    // TEXT types
                    let s = extract_string(&mv.value)?;
                    Ok(TypedValue::new(
                        SyncDataType::Text,
                        GeneratedValue::String(s),
                    ))
                }
            }

            // Date/time types
            MYSQL_TYPE_DATE => {
                let dt = extract_date(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Date,
                    GeneratedValue::String(dt.format("%Y-%m-%d").to_string()),
                ))
            }

            MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => {
                let time = extract_time(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Time,
                    GeneratedValue::String(time.format("%H:%M:%S").to_string()),
                ))
            }

            MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => {
                let dt = extract_datetime(&mv.value)?;
                Ok(TypedValue::datetime(dt))
            }

            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => {
                let dt = extract_datetime(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::TimestampTz,
                    GeneratedValue::DateTime(dt),
                ))
            }

            MYSQL_TYPE_YEAR => {
                let i = extract_int(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::SmallInt,
                    GeneratedValue::Int32(i as i32),
                ))
            }

            // JSON
            MYSQL_TYPE_JSON => {
                let s = extract_string(&mv.value)?;
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&s) {
                    let gv = json_to_generated_value(json);
                    Ok(TypedValue::new(SyncDataType::Json, gv))
                } else {
                    Ok(TypedValue::new(
                        SyncDataType::Json,
                        GeneratedValue::String(s),
                    ))
                }
            }

            // Enum and Set
            MYSQL_TYPE_ENUM => {
                let s = extract_string(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Enum { values: vec![] },
                    GeneratedValue::String(s),
                ))
            }

            MYSQL_TYPE_SET => {
                let s = extract_string(&mv.value)?;
                let values: Vec<GeneratedValue> = s
                    .split(',')
                    .map(|v| GeneratedValue::String(v.to_string()))
                    .collect();
                Ok(TypedValue::new(
                    SyncDataType::Set { values: vec![] },
                    GeneratedValue::Array(values),
                ))
            }

            // Geometry
            MYSQL_TYPE_GEOMETRY => {
                let bytes = extract_bytes(&mv.value)?;
                Ok(TypedValue::new(
                    SyncDataType::Geometry {
                        geometry_type: sync_core::GeometryType::Point,
                    },
                    GeneratedValue::Bytes(bytes),
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
                        SyncDataType::Bytes,
                        GeneratedValue::Bytes(bytes),
                    ))
                }
            }

            _ => Err(ConversionError::UnsupportedType(mv.column_type)),
        }
    }
}

/// Convert MySQL column type to SyncDataType.
fn column_type_to_sync_type(col_type: ColumnType, mv: &MySQLValueWithSchema) -> SyncDataType {
    use ColumnType::*;
    match col_type {
        MYSQL_TYPE_TINY => {
            // Check if this is a boolean column
            if mv.is_boolean_column() {
                SyncDataType::Bool
            } else {
                SyncDataType::TinyInt {
                    width: mv.column_length.unwrap_or(4) as u8,
                }
            }
        }
        MYSQL_TYPE_SHORT => SyncDataType::SmallInt,
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => SyncDataType::Int,
        MYSQL_TYPE_LONGLONG => SyncDataType::BigInt,
        MYSQL_TYPE_FLOAT => SyncDataType::Float,
        MYSQL_TYPE_DOUBLE => SyncDataType::Double,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => SyncDataType::Decimal {
            precision: mv.precision.unwrap_or(10),
            scale: mv.scale.unwrap_or(0),
        },
        MYSQL_TYPE_STRING => SyncDataType::Char {
            length: mv.column_length.unwrap_or(255) as u16,
        },
        MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR => SyncDataType::VarChar {
            length: mv.column_length.unwrap_or(255) as u16,
        },
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_BLOB | MYSQL_TYPE_LONG_BLOB => {
            if mv.column_flags.contains(ColumnFlags::BINARY_FLAG) {
                SyncDataType::Blob
            } else {
                SyncDataType::Text
            }
        }
        MYSQL_TYPE_DATE => SyncDataType::Date,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => SyncDataType::Time,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => SyncDataType::DateTime,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => SyncDataType::TimestampTz,
        MYSQL_TYPE_YEAR => SyncDataType::SmallInt,
        MYSQL_TYPE_JSON => SyncDataType::Json,
        MYSQL_TYPE_ENUM => SyncDataType::Enum { values: vec![] },
        MYSQL_TYPE_SET => SyncDataType::Set { values: vec![] },
        MYSQL_TYPE_GEOMETRY => SyncDataType::Geometry {
            geometry_type: sync_core::GeometryType::Point,
        },
        MYSQL_TYPE_BIT => SyncDataType::Bytes,
        _ => SyncDataType::Text,
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
            NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                .ok_or(ConversionError::InvalidDateTime)
        }
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|_| ConversionError::InvalidDateTime)
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
                .ok_or(ConversionError::InvalidDateTime)
        }
        Value::Bytes(b) => {
            let s = String::from_utf8(b.clone())?;
            NaiveTime::parse_from_str(&s, "%H:%M:%S").map_err(|_| ConversionError::InvalidDateTime)
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
            let naive = chrono::NaiveDateTime::new(
                NaiveDate::from_ymd_opt(*year as i32, *month as u32, *day as u32)
                    .ok_or(ConversionError::InvalidDateTime)?,
                NaiveTime::from_hms_micro_opt(*hour as u32, *min as u32, *sec as u32, *micro)
                    .ok_or(ConversionError::InvalidDateTime)?,
            );
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
            Err(ConversionError::InvalidDateTime)
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

/// Convert serde_json::Value to GeneratedValue.
fn json_to_generated_value(json: serde_json::Value) -> GeneratedValue {
    match json {
        serde_json::Value::Null => GeneratedValue::Null,
        serde_json::Value::Bool(b) => GeneratedValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                GeneratedValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                GeneratedValue::Float64(f)
            } else {
                GeneratedValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => GeneratedValue::String(s),
        serde_json::Value::Array(arr) => {
            GeneratedValue::Array(arr.into_iter().map(json_to_generated_value).collect())
        }
        serde_json::Value::Object(obj) => GeneratedValue::Object(
            obj.into_iter()
                .map(|(k, v)| (k, json_to_generated_value(v)))
                .collect(),
        ),
    }
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
    let columns = row.columns();
    let mut result = std::collections::HashMap::new();

    for (index, column) in columns.iter().enumerate() {
        let column_name = column.name_str().to_string();
        let column_type = column.column_type();
        let column_flags = column.flags();

        // Get the raw value
        let raw_value = row
            .as_ref(index)
            .ok_or_else(|| ConversionError::TypeMismatch {
                expected: "any value".to_string(),
                actual: Value::NULL,
            })?
            .clone();

        // Check if this column should be treated as boolean
        let is_boolean = boolean_columns
            .map(|cols| cols.contains(&column_name.as_str()))
            .unwrap_or(false);

        // For JSON columns with config, use special conversion
        if column_type == ColumnType::MYSQL_TYPE_JSON {
            if let Some(config) = json_config {
                if let Value::Bytes(bytes) = &raw_value {
                    if let Ok(s) = String::from_utf8(bytes.clone()) {
                        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&s) {
                            let typed_value =
                                json_to_typed_value_with_config(json_value, "", config);
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
        assert!(matches!(tv.sync_type, SyncDataType::Int));
        assert!(matches!(tv.value, GeneratedValue::Int32(42)));
    }

    #[test]
    fn test_bigint_conversion() {
        let mv = MySQLValueWithSchema::new(
            Value::Int(9_223_372_036_854_775_807),
            ColumnType::MYSQL_TYPE_LONGLONG,
            ColumnFlags::empty(),
        );
        let tv = mv.to_typed_value().unwrap();
        assert!(matches!(tv.sync_type, SyncDataType::BigInt));
        assert!(matches!(
            tv.value,
            GeneratedValue::Int64(9_223_372_036_854_775_807)
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
        assert!(matches!(tv.sync_type, SyncDataType::VarChar { .. }));
        if let GeneratedValue::String(s) = tv.value {
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
        assert!(matches!(tv.sync_type, SyncDataType::Uuid));
        if let GeneratedValue::Uuid(u) = tv.value {
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
        assert!(matches!(tv.sync_type, SyncDataType::DateTime));
        if let GeneratedValue::DateTime(dt) = tv.value {
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
        assert!(matches!(tv.sync_type, SyncDataType::Int));
        assert!(matches!(tv.value, GeneratedValue::Null));
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
        assert!(matches!(tv.sync_type, SyncDataType::Json));
        if let GeneratedValue::Object(obj) = tv.value {
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
        if let SyncDataType::Decimal { precision, scale } = tv.sync_type {
            assert_eq!(precision, 10);
            assert_eq!(scale, 3);
        } else {
            panic!("Expected Decimal type");
        }
        if let GeneratedValue::Decimal { value, .. } = tv.value {
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
        assert!(matches!(tv.sync_type, SyncDataType::Blob));
        if let GeneratedValue::Bytes(b) = tv.value {
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
        assert!(matches!(tv.sync_type, SyncDataType::Text));
        if let GeneratedValue::String(s) = tv.value {
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
        assert!(matches!(tv.sync_type, SyncDataType::Bool));
        assert!(matches!(tv.value, GeneratedValue::Bool(true)));
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
        assert!(matches!(tv.sync_type, SyncDataType::Bool));
        assert!(matches!(tv.value, GeneratedValue::Bool(false)));
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
        assert!(matches!(tv.sync_type, SyncDataType::TinyInt { .. }));
        assert!(matches!(tv.value, GeneratedValue::Int32(1)));
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
        assert!(matches!(tv.sync_type, SyncDataType::Bool));
        assert!(matches!(tv.value, GeneratedValue::Bool(true)));
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
        assert!(matches!(tv.sync_type, SyncDataType::Bool));
        assert!(matches!(tv.value, GeneratedValue::Bool(false)));
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
        assert!(matches!(tv.sync_type, SyncDataType::TinyInt { .. }));
        assert!(matches!(tv.value, GeneratedValue::Int32(5)));
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
        assert!(matches!(tv.sync_type, SyncDataType::Bool));
        assert!(matches!(tv.value, GeneratedValue::Null));
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

        if let GeneratedValue::Object(root) = tv.value {
            // Check settings.enabled is now boolean true
            if let Some(GeneratedValue::Object(settings)) = root.get("settings") {
                assert!(matches!(
                    settings.get("enabled"),
                    Some(GeneratedValue::Bool(true))
                ));
                // count should still be an integer
                assert!(matches!(
                    settings.get("count"),
                    Some(GeneratedValue::Int64(5))
                ));
            } else {
                panic!("Expected settings object");
            }

            // Check flags.is_active is now boolean false
            if let Some(GeneratedValue::Object(flags)) = root.get("flags") {
                assert!(matches!(
                    flags.get("is_active"),
                    Some(GeneratedValue::Bool(false))
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

        if let GeneratedValue::Object(root) = tv.value {
            // Check permissions is now an array
            if let Some(GeneratedValue::Array(perms)) = root.get("permissions") {
                assert_eq!(perms.len(), 3);
                assert!(matches!(&perms[0], GeneratedValue::String(s) if s == "read"));
                assert!(matches!(&perms[1], GeneratedValue::String(s) if s == "write"));
                assert!(matches!(&perms[2], GeneratedValue::String(s) if s == "execute"));
            } else {
                panic!("Expected Array value for permissions");
            }

            // name should still be a string
            assert!(matches!(root.get("name"), Some(GeneratedValue::String(s)) if s == "admin"));
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

        if let GeneratedValue::Object(root) = tv.value {
            assert!(matches!(
                root.get("enabled"),
                Some(GeneratedValue::Int64(1))
            ));
            assert!(matches!(
                root.get("disabled"),
                Some(GeneratedValue::Int64(0))
            ));
        } else {
            panic!("Expected Object value");
        }
    }
}
