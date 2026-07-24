//! MySQL schema column type conversion.
//!
//! This module provides conversion from MySQL INFORMATION_SCHEMA column types
//! to `Type` for schema introspection during incremental sync.

use sync_core::Type;

/// Convert MySQL INFORMATION_SCHEMA column type to Type.
///
/// This function maps MySQL data types (as returned by `information_schema.columns`)
/// to the appropriate `Type` for use in schema-aware type conversion.
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
/// The corresponding `Type` for the MySQL column type.
///
/// # Example
///
/// ```
/// use mysql_types::mysql_column_to_universal_type;
/// use sync_core::Type;
///
/// let ut = mysql_column_to_universal_type("INT", "int(11)", None, None);
/// assert_eq!(ut, Type::Int32);
///
/// // TINYINT(1) is treated as boolean in MySQL
/// let ut = mysql_column_to_universal_type("TINYINT", "tinyint(1)", None, None);
/// assert_eq!(ut, Type::Bool);
/// ```
pub fn mysql_column_to_universal_type(
    data_type: &str,
    column_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> Type {
    match data_type.to_uppercase().as_str() {
        // Numeric types
        "TINYINT" => {
            // TINYINT(1) is commonly used for boolean in MySQL
            if column_type.to_lowercase().starts_with("tinyint(1)") {
                Type::Bool
            } else {
                // Extract width from column_type (e.g., "tinyint(4)" -> 4)
                let width = extract_length_from_column_type(column_type)
                    .map(|l| l as u8)
                    .unwrap_or(4);
                Type::Int8 { width }
            }
        }
        "SMALLINT" => Type::Int16,
        "MEDIUMINT" | "INT" | "INTEGER" => Type::Int32,
        "BIGINT" => Type::Int64,
        "FLOAT" => Type::Float32,
        "DOUBLE" | "REAL" => Type::Float64,
        "DECIMAL" | "NUMERIC" => Type::Decimal {
            // precision/scale are u32 but Type expects u8, cap at 38
            precision: precision.map(|p| p.min(38) as u8).unwrap_or(10),
            scale: scale.map(|s| s.min(38) as u8).unwrap_or(0),
        },

        // Boolean
        "BOOLEAN" | "BOOL" => Type::Bool,

        // String types
        "VARCHAR" => {
            // Try to extract length from column_type
            let length = extract_length_from_column_type(column_type);
            match length {
                Some(len) => Type::VarChar { length: len },
                None => Type::Text,
            }
        }
        "CHAR" => {
            let length = extract_length_from_column_type(column_type).unwrap_or(1);
            Type::Char { length }
        }
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => Type::Text,

        // Binary types
        "BINARY" | "VARBINARY" | "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => Type::Bytes,

        // Date/Time types
        "DATE" => Type::Date,
        "TIME" => Type::Time,
        "TIMESTAMP" | "DATETIME" => Type::LocalDateTime,

        // JSON
        "JSON" => Type::Json,

        // Geometry types
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
        | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => Type::Geometry {
            geometry_type: sync_core::GeometryType::Point, // Generic fallback
        },

        // SET type - stores list of allowed values
        "SET" => {
            // Extract values from column_type (e.g., "set('a','b','c')")
            let values = extract_set_or_enum_values(column_type);
            Type::Set { values }
        }

        // ENUM type - stores list of allowed values
        "ENUM" => {
            // Extract values from column_type (e.g., "enum('small','medium','large')")
            let values = extract_set_or_enum_values(column_type);
            Type::Enum { values }
        }

        // UUID if using a UUID column type (MySQL 8.0+)
        "UUID" => Type::Uuid,

        // Fallback to Text for unknown types
        _ => Type::Text,
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
            Type::Int8 { width: 4 }
        ));
        assert_eq!(
            mysql_column_to_universal_type("SMALLINT", "smallint(6)", None, None),
            Type::Int16
        );
        assert_eq!(
            mysql_column_to_universal_type("INT", "int(11)", None, None),
            Type::Int32
        );
        assert_eq!(
            mysql_column_to_universal_type("INTEGER", "int(11)", None, None),
            Type::Int32
        );
        assert_eq!(
            mysql_column_to_universal_type("BIGINT", "bigint(20)", None, None),
            Type::Int64
        );
    }

    #[test]
    fn test_mysql_tinyint1_bool() {
        // TINYINT(1) is treated as boolean in MySQL
        assert_eq!(
            mysql_column_to_universal_type("TINYINT", "tinyint(1)", None, None),
            Type::Bool
        );
        // But TINYINT(4) is a regular integer
        assert!(matches!(
            mysql_column_to_universal_type("TINYINT", "tinyint(4)", None, None),
            Type::Int8 { width: 4 }
        ));
    }

    #[test]
    fn test_mysql_decimal_types() {
        let ut = mysql_column_to_universal_type("DECIMAL", "decimal(10,2)", Some(10), Some(2));
        assert!(matches!(
            ut,
            Type::Decimal {
                precision: 10,
                scale: 2
            }
        ));

        let ut = mysql_column_to_universal_type("NUMERIC", "numeric(18,4)", Some(18), Some(4));
        assert!(matches!(
            ut,
            Type::Decimal {
                precision: 18,
                scale: 4
            }
        ));
    }

    #[test]
    fn test_mysql_string_types() {
        assert_eq!(
            mysql_column_to_universal_type("VARCHAR", "varchar(255)", None, None),
            Type::VarChar { length: 255 }
        );
        assert_eq!(
            mysql_column_to_universal_type("CHAR", "char(10)", None, None),
            Type::Char { length: 10 }
        );
        assert_eq!(
            mysql_column_to_universal_type("TEXT", "text", None, None),
            Type::Text
        );
        assert_eq!(
            mysql_column_to_universal_type("LONGTEXT", "longtext", None, None),
            Type::Text
        );
    }

    #[test]
    fn test_mysql_datetime_types() {
        assert_eq!(
            mysql_column_to_universal_type("TIMESTAMP", "timestamp", None, None),
            Type::LocalDateTime
        );
        assert_eq!(
            mysql_column_to_universal_type("DATETIME", "datetime", None, None),
            Type::LocalDateTime
        );
        assert_eq!(
            mysql_column_to_universal_type("DATE", "date", None, None),
            Type::Date
        );
        assert_eq!(
            mysql_column_to_universal_type("TIME", "time", None, None),
            Type::Time
        );
    }

    #[test]
    fn test_mysql_json_type() {
        assert_eq!(
            mysql_column_to_universal_type("JSON", "json", None, None),
            Type::Json
        );
    }

    #[test]
    fn test_mysql_set_type() {
        let ut = mysql_column_to_universal_type("SET", "set('a','b','c')", None, None);
        assert!(matches!(ut, Type::Set { .. }));
    }

    #[test]
    fn test_mysql_bool_types() {
        assert_eq!(
            mysql_column_to_universal_type("BOOLEAN", "boolean", None, None),
            Type::Bool
        );
        assert_eq!(
            mysql_column_to_universal_type("BOOL", "bool", None, None),
            Type::Bool
        );
    }

    #[test]
    fn test_mysql_bytes_types() {
        assert_eq!(
            mysql_column_to_universal_type("BLOB", "blob", None, None),
            Type::Bytes
        );
        assert_eq!(
            mysql_column_to_universal_type("BINARY", "binary(16)", None, None),
            Type::Bytes
        );
        assert_eq!(
            mysql_column_to_universal_type("VARBINARY", "varbinary(255)", None, None),
            Type::Bytes
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
            assert!(matches!(ut, Type::Geometry { .. }));
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
