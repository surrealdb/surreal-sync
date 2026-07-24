//! PostgreSQL schema column type conversion.
//!
//! This module provides conversion from PostgreSQL INFORMATION_SCHEMA column types
//! to `Type` for schema introspection during incremental sync.

use surreal_sync_core::Type;

/// Convert PostgreSQL INFORMATION_SCHEMA column type to Type.
///
/// This function maps PostgreSQL data types (as returned by `information_schema.columns`)
/// to the appropriate `Type` for use in schema-aware type conversion.
///
/// # Arguments
///
/// * `data_type` - The PostgreSQL data type name (e.g., "integer", "varchar", "timestamp")
/// * `precision` - Optional numeric precision for decimal types
/// * `scale` - Optional numeric scale for decimal types
///
/// # Returns
///
/// The corresponding `Type` for the PostgreSQL column type.
///
/// # Example
///
/// ```
/// use surreal_sync_postgresql::types::postgresql_column_to_universal_type;
/// use surreal_sync_core::Type;
///
/// let ut = postgresql_column_to_universal_type("integer", None, None);
/// assert_eq!(ut, Type::Int32);
///
/// let ut = postgresql_column_to_universal_type("numeric", Some(10), Some(2));
/// assert!(matches!(ut, Type::Decimal { precision: 10, scale: 2 }));
/// ```
pub fn postgresql_column_to_universal_type(
    data_type: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> Type {
    match data_type.to_lowercase().as_str() {
        // Numeric types
        "smallint" | "int2" => Type::Int16,
        "integer" | "int" | "int4" => Type::Int32,
        "bigint" | "int8" => Type::Int64,
        "real" | "float4" => Type::Float32,
        "double precision" | "float8" => Type::Float64,
        "numeric" | "decimal" => Type::Decimal {
            // precision/scale are u32 but Type expects u8, cap at 38
            precision: precision.map(|p| p.min(38) as u8).unwrap_or(38),
            scale: scale.map(|s| s.min(38) as u8).unwrap_or(10),
        },

        // Boolean
        "boolean" | "bool" => Type::Bool,

        // String types
        "text" => Type::Text,
        "varchar" | "character varying" => {
            // If no length specified, treat as unlimited (use Text)
            match precision {
                Some(p) => Type::VarChar { length: p as u16 },
                None => Type::Text,
            }
        }
        "char" | "character" => Type::Char {
            length: precision.map(|p| p as u16).unwrap_or(1),
        },

        // Binary
        "bytea" => Type::Bytes,

        // Date/Time types
        "date" => Type::Date,
        "time" | "time without time zone" => Type::Time,
        "timestamp" | "timestamp without time zone" => Type::LocalDateTime,
        "timestamptz" | "timestamp with time zone" => Type::ZonedDateTime,
        "interval" => Type::Duration,

        // UUID
        "uuid" => Type::Uuid,

        // JSON types
        "json" => Type::Json,
        "jsonb" => Type::Jsonb,

        // Geometry types
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            Type::Geometry {
                geometry_type: surreal_sync_core::GeometryType::Point, // Generic fallback
            }
        }

        // Array types - PostgreSQL reports arrays as "_type" (e.g., "_int4" for int[])
        s if s.starts_with('_') => {
            let element_type = postgresql_column_to_universal_type(&s[1..], None, None);
            Type::Array {
                element_type: Box::new(element_type),
            }
        }

        // Fallback to Text for unknown types
        _ => Type::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgresql_int_types() {
        assert_eq!(
            postgresql_column_to_universal_type("smallint", None, None),
            Type::Int16
        );
        assert_eq!(
            postgresql_column_to_universal_type("integer", None, None),
            Type::Int32
        );
        assert_eq!(
            postgresql_column_to_universal_type("int4", None, None),
            Type::Int32
        );
        assert_eq!(
            postgresql_column_to_universal_type("bigint", None, None),
            Type::Int64
        );
        assert_eq!(
            postgresql_column_to_universal_type("int8", None, None),
            Type::Int64
        );
    }

    #[test]
    fn test_postgresql_decimal_types() {
        let ut = postgresql_column_to_universal_type("numeric", Some(10), Some(2));
        assert!(matches!(
            ut,
            Type::Decimal {
                precision: 10,
                scale: 2
            }
        ));

        let ut = postgresql_column_to_universal_type("decimal", Some(18), Some(4));
        assert!(matches!(
            ut,
            Type::Decimal {
                precision: 18,
                scale: 4
            }
        ));

        // Test precision capping at 38
        let ut = postgresql_column_to_universal_type("numeric", Some(50), Some(10));
        assert!(matches!(
            ut,
            Type::Decimal {
                precision: 38,
                scale: 10
            }
        ));
    }

    #[test]
    fn test_postgresql_string_types() {
        assert_eq!(
            postgresql_column_to_universal_type("text", None, None),
            Type::Text
        );
        // VARCHAR without length is treated as Text
        assert_eq!(
            postgresql_column_to_universal_type("varchar", None, None),
            Type::Text
        );
        assert_eq!(
            postgresql_column_to_universal_type("character varying", Some(255), None),
            Type::VarChar { length: 255 }
        );
        assert_eq!(
            postgresql_column_to_universal_type("char", Some(10), None),
            Type::Char { length: 10 }
        );
    }

    #[test]
    fn test_postgresql_datetime_types() {
        assert_eq!(
            postgresql_column_to_universal_type("timestamp", None, None),
            Type::LocalDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("timestamp without time zone", None, None),
            Type::LocalDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("timestamptz", None, None),
            Type::ZonedDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("timestamp with time zone", None, None),
            Type::ZonedDateTime
        );
        assert_eq!(
            postgresql_column_to_universal_type("date", None, None),
            Type::Date
        );
        assert_eq!(
            postgresql_column_to_universal_type("time", None, None),
            Type::Time
        );
    }

    #[test]
    fn test_postgresql_uuid_type() {
        assert_eq!(
            postgresql_column_to_universal_type("uuid", None, None),
            Type::Uuid
        );
    }

    #[test]
    fn test_postgresql_json_types() {
        assert_eq!(
            postgresql_column_to_universal_type("json", None, None),
            Type::Json
        );
        assert_eq!(
            postgresql_column_to_universal_type("jsonb", None, None),
            Type::Jsonb
        );
    }

    #[test]
    fn test_postgresql_geometry_types() {
        for geom_type in ["point", "line", "polygon", "box", "circle"] {
            let ut = postgresql_column_to_universal_type(geom_type, None, None);
            assert!(matches!(ut, Type::Geometry { .. }));
        }
    }

    #[test]
    fn test_postgresql_bool_type() {
        assert_eq!(
            postgresql_column_to_universal_type("boolean", None, None),
            Type::Bool
        );
        assert_eq!(
            postgresql_column_to_universal_type("bool", None, None),
            Type::Bool
        );
    }

    #[test]
    fn test_postgresql_bytes_type() {
        assert_eq!(
            postgresql_column_to_universal_type("bytea", None, None),
            Type::Bytes
        );
    }

    #[test]
    fn test_postgresql_array_types() {
        let ut = postgresql_column_to_universal_type("_int4", None, None);
        assert!(matches!(
            ut,
            Type::Array { element_type } if *element_type == Type::Int32
        ));

        let ut = postgresql_column_to_universal_type("_text", None, None);
        assert!(matches!(
            ut,
            Type::Array { element_type } if *element_type == Type::Text
        ));
    }

    #[test]
    fn test_postgresql_interval_type() {
        assert_eq!(
            postgresql_column_to_universal_type("interval", None, None),
            Type::Duration
        );
    }
}
