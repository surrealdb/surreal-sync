//! PostgreSQL schema column type conversion.
//!
//! This module provides conversion from PostgreSQL INFORMATION_SCHEMA column types
//! to `UniversalType` for schema introspection during incremental sync.

use sync_core::UniversalType;

/// Convert PostgreSQL INFORMATION_SCHEMA column type to UniversalType.
///
/// This function maps PostgreSQL data types (as returned by `information_schema.columns`)
/// to the appropriate `UniversalType` for use in schema-aware type conversion.
///
/// # Arguments
///
/// * `data_type` - The PostgreSQL data type name (e.g., "integer", "varchar", "timestamp")
/// * `precision` - Optional numeric precision for decimal types
/// * `scale` - Optional numeric scale for decimal types
///
/// # Returns
///
/// The corresponding `UniversalType` for the PostgreSQL column type.
///
/// # Example
///
/// ```
/// use postgresql_types::postgresql_column_to_universal_type;
/// use sync_core::UniversalType;
///
/// let ut = postgresql_column_to_universal_type("integer", None, None);
/// assert_eq!(ut, UniversalType::Int32);
///
/// let ut = postgresql_column_to_universal_type("numeric", Some(10), Some(2));
/// assert!(matches!(ut, UniversalType::Decimal { precision: 10, scale: 2 }));
/// ```
pub fn postgresql_column_to_universal_type(
    data_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> UniversalType {
    match data_type.to_lowercase().as_str() {
        // Numeric types
        "smallint" | "int2" => UniversalType::Int16,
        "integer" | "int" | "int4" => UniversalType::Int32,
        "bigint" | "int8" => UniversalType::Int64,
        "real" | "float4" => UniversalType::Float32,
        "double precision" | "float8" => UniversalType::Float64,
        "numeric" | "decimal" => UniversalType::Decimal {
            // precision/scale are u32 but UniversalType expects u8, cap at 38
            precision: precision.map(|p| p.min(38) as u8).unwrap_or(38),
            scale: scale.map(|s| s.min(38) as u8).unwrap_or(10),
        },

        // Boolean
        "boolean" | "bool" => UniversalType::Bool,

        // String types
        "text" => UniversalType::Text,
        "varchar" | "character varying" => {
            // If no length specified, treat as unlimited (use Text)
            match precision {
                Some(p) => UniversalType::VarChar { length: p as u16 },
                None => UniversalType::Text,
            }
        }
        "char" | "character" => UniversalType::Char {
            length: precision.map(|p| p as u16).unwrap_or(1),
        },

        // Binary
        "bytea" => UniversalType::Bytes,

        // Date/Time types
        "date" => UniversalType::Date,
        "time" | "time without time zone" => UniversalType::Time,
        "timestamp" | "timestamp without time zone" => UniversalType::LocalDateTime,
        "timestamptz" | "timestamp with time zone" => UniversalType::ZonedDateTime,
        "interval" => UniversalType::Duration,

        // UUID
        "uuid" => UniversalType::Uuid,

        // JSON types
        "json" => UniversalType::Json,
        "jsonb" => UniversalType::Jsonb,

        // Geometry types
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            UniversalType::Geometry {
                geometry_type: sync_core::GeometryType::Point, // Generic fallback
            }
        }

        // Array types - PostgreSQL reports arrays as "_type" (e.g., "_int4" for int[])
        s if s.starts_with('_') => {
            let element_type = postgresql_column_to_universal_type(&s[1..], None, None);
            UniversalType::Array {
                element_type: Box::new(element_type),
            }
        }

        // Fallback to Text for unknown types
        _ => UniversalType::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgresql_int_types() {
        assert_eq!(
            postgresql_column_to_universal_type("smallint", None, None),
            UniversalType::Int16
        );
        assert_eq!(
            postgresql_column_to_universal_type("integer", None, None),
            UniversalType::Int32
        );
        assert_eq!(
            postgresql_column_to_universal_type("int4", None, None),
            UniversalType::Int32
        );
        assert_eq!(
            postgresql_column_to_universal_type("bigint", None, None),
            UniversalType::Int64
        );
        assert_eq!(
            postgresql_column_to_universal_type("int8", None, None),
            UniversalType::Int64
        );
    }

    #[test]
    fn test_postgresql_decimal_types() {
        let ut = postgresql_column_to_universal_type("numeric", Some(10), Some(2));
        assert!(matches!(
            ut,
            UniversalType::Decimal {
                precision: 10,
                scale: 2
            }
        ));

        let ut = postgresql_column_to_universal_type("decimal", Some(18), Some(4));
        assert!(matches!(
            ut,
            UniversalType::Decimal {
                precision: 18,
                scale: 4
            }
        ));

        // Test precision capping at 38
        let ut = postgresql_column_to_universal_type("numeric", Some(50), Some(10));
        assert!(matches!(
            ut,
            UniversalType::Decimal {
                precision: 38,
                scale: 10
            }
        ));
    }

    #[test]
    fn test_postgresql_string_types() {
        assert_eq!(
            postgresql_column_to_universal_type("text", None, None),
            UniversalType::Text
        );
        // VARCHAR without length is treated as Text
        assert_eq!(
            postgresql_column_to_universal_type("varchar", None, None),
            UniversalType::Text
        );
        assert_eq!(
            postgresql_column_to_universal_type("character varying", Some(255), None),
            UniversalType::VarChar { length: 255 }
        );
        assert_eq!(
            postgresql_column_to_universal_type("char", Some(10), None),
            UniversalType::Char { length: 10 }
        );
    }

    #[test]
    fn test_postgresql_datetime_types() {
        assert_eq!(
            postgresql_column_to_universal_type("timestamp", None, None),
            UniversalType::LocalDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("timestamp without time zone", None, None),
            UniversalType::LocalDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("timestamptz", None, None),
            UniversalType::ZonedDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("timestamp with time zone", None, None),
            UniversalType::ZonedDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("date", None, None),
            UniversalType::Date
        );
        assert_eq!(
            postgresql_column_to_universal_type("time", None, None),
            UniversalType::Time
        );
    }

    #[test]
    fn test_postgresql_uuid_type() {
        assert_eq!(
            postgresql_column_to_universal_type("uuid", None, None),
            UniversalType::Uuid
        );
    }

    #[test]
    fn test_postgresql_json_types() {
        assert_eq!(
            postgresql_column_to_universal_type("json", None, None),
            UniversalType::Json
        );
        assert_eq!(
            postgresql_column_to_universal_type("jsonb", None, None),
            UniversalType::Jsonb
        );
    }

    #[test]
    fn test_postgresql_geometry_types() {
        for geom_type in ["point", "line", "polygon", "box", "circle"] {
            let ut = postgresql_column_to_universal_type(geom_type, None, None);
            assert!(matches!(ut, UniversalType::Geometry { .. }));
        }
    }

    #[test]
    fn test_postgresql_bool_type() {
        assert_eq!(
            postgresql_column_to_universal_type("boolean", None, None),
            UniversalType::Bool
        );
        assert_eq!(
            postgresql_column_to_universal_type("bool", None, None),
            UniversalType::Bool
        );
    }

    #[test]
    fn test_postgresql_bytes_type() {
        assert_eq!(
            postgresql_column_to_universal_type("bytea", None, None),
            UniversalType::Bytes
        );
    }

    #[test]
    fn test_postgresql_array_types() {
        let ut = postgresql_column_to_universal_type("_int4", None, None);
        assert!(matches!(
            ut,
            UniversalType::Array { element_type } if *element_type == UniversalType::Int32
        ));

        let ut = postgresql_column_to_universal_type("_text", None, None);
        assert!(matches!(
            ut,
            UniversalType::Array { element_type } if *element_type == UniversalType::Text
        ));
    }

    #[test]
    fn test_postgresql_interval_type() {
        assert_eq!(
            postgresql_column_to_universal_type("interval", None, None),
            UniversalType::Duration
        );
    }
}
