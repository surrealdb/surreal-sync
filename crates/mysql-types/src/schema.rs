//! MySQL schema column type conversion.
//!
//! This module provides conversion from MySQL INFORMATION_SCHEMA column types
//! to `UniversalType` for schema introspection during incremental sync.

use sync_core::UniversalType;

/// Convert MySQL INFORMATION_SCHEMA column type to UniversalType.
///
/// This function maps MySQL data types (as returned by `information_schema.columns`)
/// to the appropriate `UniversalType` for use in schema-aware type conversion.
///
/// # Arguments
///
/// * `data_type` - The MySQL data type name (e.g., "INT", "VARCHAR", "TIMESTAMP")
/// * `column_type` - The full column type string (e.g., "int(11)", "varchar(255)", "tinyint(1)")
/// * `precision` - Optional numeric precision
/// * `scale` - Optional numeric scale
///
/// # Returns
///
/// The corresponding `UniversalType` for the MySQL column type.
///
/// # Example
///
/// ```
/// use mysql_types::mysql_column_to_universal_type;
/// use sync_core::UniversalType;
///
/// let ut = mysql_column_to_universal_type("INT", "int(11)", None, None);
/// assert_eq!(ut, UniversalType::Int32);
///
/// // TINYINT(1) is treated as boolean in MySQL
/// let ut = mysql_column_to_universal_type("TINYINT", "tinyint(1)", None, None);
/// assert_eq!(ut, UniversalType::Bool);
/// ```
pub fn mysql_column_to_universal_type(
    data_type: &str,
    column_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> UniversalType {
    match data_type.to_uppercase().as_str() {
        // Numeric types
        "TINYINT" => {
            // TINYINT(1) is commonly used for boolean in MySQL
            if column_type.to_lowercase().starts_with("tinyint(1)") {
                UniversalType::Bool
            } else {
                // Extract width from column_type (e.g., "tinyint(4)" -> 4)
                let width = extract_length_from_column_type(column_type)
                    .map(|l| l as u8)
                    .unwrap_or(4);
                UniversalType::Int8 { width }
            }
        }
        "SMALLINT" => UniversalType::Int16,
        "MEDIUMINT" | "INT" | "INTEGER" => UniversalType::Int32,
        "BIGINT" => UniversalType::Int64,
        "FLOAT" => UniversalType::Float32,
        "DOUBLE" | "REAL" => UniversalType::Float64,
        "DECIMAL" | "NUMERIC" => UniversalType::Decimal {
            // precision/scale are u32 but UniversalType expects u8, cap at 38
            precision: precision.map(|p| p.min(38) as u8).unwrap_or(10),
            scale: scale.map(|s| s.min(38) as u8).unwrap_or(0),
        },

        // Boolean
        "BOOLEAN" | "BOOL" => UniversalType::Bool,

        // String types
        "VARCHAR" => {
            // Try to extract length from column_type
            let length = extract_length_from_column_type(column_type);
            match length {
                Some(len) => UniversalType::VarChar { length: len },
                None => UniversalType::Text,
            }
        }
        "CHAR" => {
            let length = extract_length_from_column_type(column_type).unwrap_or(1);
            UniversalType::Char { length }
        }
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => UniversalType::Text,

        // Binary types
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            UniversalType::Bytes
        }

        // Date/Time types
        "DATE" => UniversalType::Date,
        "TIME" => UniversalType::Time,
        "TIMESTAMP" | "DATETIME" => UniversalType::LocalDateTime,

        // JSON
        "JSON" => UniversalType::Json,

        // Geometry types
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
        | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => UniversalType::Geometry {
            geometry_type: sync_core::GeometryType::Point, // Generic fallback
        },

        // SET type - stores list of allowed values
        "SET" => {
            // Extract values from column_type (e.g., "set('a','b','c')")
            let values = extract_set_or_enum_values(column_type);
            UniversalType::Set { values }
        }

        // ENUM type - stores list of allowed values
        "ENUM" => {
            // Extract values from column_type (e.g., "enum('small','medium','large')")
            let values = extract_set_or_enum_values(column_type);
            UniversalType::Enum { values }
        }

        // UUID if using a UUID column type (MySQL 8.0+)
        "UUID" => UniversalType::Uuid,

        // Fallback to Text for unknown types
        _ => UniversalType::Text,
    }
}

/// Extract length from a MySQL column type string.
///
/// E.g., "varchar(255)" -> Some(255), "int(11)" -> Some(11)
fn extract_length_from_column_type(column_type: &str) -> Option<u16> {
    let column_type_lower = column_type.to_lowercase();
    if let Some(start) = column_type_lower.find('(') {
        if let Some(end) = column_type_lower.find(')') {
            if start < end {
                let len_str = &column_type_lower[start + 1..end];
                // Handle comma-separated values (e.g., DECIMAL(10,2))
                let first_part = len_str.split(',').next().unwrap_or(len_str);
                return first_part.trim().parse().ok();
            }
        }
    }
    None
}

/// Extract values from a MySQL SET or ENUM column type string.
///
/// E.g., "set('a','b','c')" -> vec!["a", "b", "c"]
/// E.g., "enum('small','medium','large')" -> vec!["small", "medium", "large"]
fn extract_set_or_enum_values(column_type: &str) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(start) = column_type.find('(') {
        if let Some(end) = column_type.rfind(')') {
            if start < end {
                let content = &column_type[start + 1..end];
                // Split by comma, but be careful with quoted strings
                for part in content.split(',') {
                    let trimmed = part.trim();
                    // Remove surrounding quotes
                    let value = trimmed.trim_matches('\'').trim_matches('"');
                    if !value.is_empty() {
                        values.push(value.to_string());
                    }
                }
            }
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_int_types() {
        assert!(matches!(
            mysql_column_to_universal_type("TINYINT", "tinyint(4)", None, None),
            UniversalType::Int8 { width: 4 }
        ));
        assert_eq!(
            mysql_column_to_universal_type("SMALLINT", "smallint(6)", None, None),
            UniversalType::Int16
        );
        assert_eq!(
            mysql_column_to_universal_type("INT", "int(11)", None, None),
            UniversalType::Int32
        );
        assert_eq!(
            mysql_column_to_universal_type("INTEGER", "int(11)", None, None),
            UniversalType::Int32
        );
        assert_eq!(
            mysql_column_to_universal_type("BIGINT", "bigint(20)", None, None),
            UniversalType::Int64
        );
    }

    #[test]
    fn test_mysql_tinyint1_bool() {
        // TINYINT(1) is treated as boolean in MySQL
        assert_eq!(
            mysql_column_to_universal_type("TINYINT", "tinyint(1)", None, None),
            UniversalType::Bool
        );
        // But TINYINT(4) is a regular integer
        assert!(matches!(
            mysql_column_to_universal_type("TINYINT", "tinyint(4)", None, None),
            UniversalType::Int8 { width: 4 }
        ));
    }

    #[test]
    fn test_mysql_decimal_types() {
        let ut = mysql_column_to_universal_type("DECIMAL", "decimal(10,2)", Some(10), Some(2));
        assert!(matches!(
            ut,
            UniversalType::Decimal {
                precision: 10,
                scale: 2
            }
        ));

        let ut = mysql_column_to_universal_type("NUMERIC", "numeric(18,4)", Some(18), Some(4));
        assert!(matches!(
            ut,
            UniversalType::Decimal {
                precision: 18,
                scale: 4
            }
        ));
    }

    #[test]
    fn test_mysql_string_types() {
        assert_eq!(
            mysql_column_to_universal_type("VARCHAR", "varchar(255)", None, None),
            UniversalType::VarChar { length: 255 }
        );
        assert_eq!(
            mysql_column_to_universal_type("CHAR", "char(10)", None, None),
            UniversalType::Char { length: 10 }
        );
        assert_eq!(
            mysql_column_to_universal_type("TEXT", "text", None, None),
            UniversalType::Text
        );
        assert_eq!(
            mysql_column_to_universal_type("LONGTEXT", "longtext", None, None),
            UniversalType::Text
        );
    }

    #[test]
    fn test_mysql_datetime_types() {
        assert_eq!(
            mysql_column_to_universal_type("TIMESTAMP", "timestamp", None, None),
            UniversalType::LocalDateTime
        );
        assert_eq!(
            mysql_column_to_universal_type("DATETIME", "datetime", None, None),
            UniversalType::LocalDateTime
        );
        assert_eq!(
            mysql_column_to_universal_type("DATE", "date", None, None),
            UniversalType::Date
        );
        assert_eq!(
            mysql_column_to_universal_type("TIME", "time", None, None),
            UniversalType::Time
        );
    }

    #[test]
    fn test_mysql_json_type() {
        assert_eq!(
            mysql_column_to_universal_type("JSON", "json", None, None),
            UniversalType::Json
        );
    }

    #[test]
    fn test_mysql_set_type() {
        let ut = mysql_column_to_universal_type("SET", "set('a','b','c')", None, None);
        assert!(matches!(ut, UniversalType::Set { .. }));
    }

    #[test]
    fn test_mysql_bool_types() {
        assert_eq!(
            mysql_column_to_universal_type("BOOLEAN", "boolean", None, None),
            UniversalType::Bool
        );
        assert_eq!(
            mysql_column_to_universal_type("BOOL", "bool", None, None),
            UniversalType::Bool
        );
    }

    #[test]
    fn test_mysql_bytes_types() {
        assert_eq!(
            mysql_column_to_universal_type("BLOB", "blob", None, None),
            UniversalType::Bytes
        );
        assert_eq!(
            mysql_column_to_universal_type("BINARY", "binary(16)", None, None),
            UniversalType::Bytes
        );
        assert_eq!(
            mysql_column_to_universal_type("VARBINARY", "varbinary(255)", None, None),
            UniversalType::Bytes
        );
    }

    #[test]
    fn test_mysql_geometry_types() {
        for geom_type in ["GEOMETRY", "POINT", "POLYGON", "LINESTRING"] {
            let ut = mysql_column_to_universal_type(
                geom_type,
                geom_type.to_lowercase().as_str(),
                None,
                None,
            );
            assert!(matches!(ut, UniversalType::Geometry { .. }));
        }
    }

    #[test]
    fn test_extract_length_from_column_type() {
        assert_eq!(extract_length_from_column_type("varchar(255)"), Some(255));
        assert_eq!(extract_length_from_column_type("int(11)"), Some(11));
        assert_eq!(extract_length_from_column_type("decimal(10,2)"), Some(10));
        assert_eq!(extract_length_from_column_type("text"), None);
        assert_eq!(extract_length_from_column_type("CHAR(1)"), Some(1));
    }
}
