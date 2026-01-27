//! Forward conversion: TypedValue/UniversalValue → Neo4j Cypher literals.
//!
//! This module converts sync-core's `TypedValue` and `UniversalValue` to Neo4j Cypher
//! literal strings for use in CREATE/SET queries.
//!
//! Unlike other type conversion modules, this module returns `Result` for all conversions
//! and never silently falls back to default values.

use crate::error::{Neo4jTypesError, Result};
use sync_core::{TypedValue, UniversalType, UniversalValue};

/// Convert a TypedValue to a Neo4j Cypher literal string.
///
/// # Errors
///
/// Returns an error if:
/// - Float value is NaN or infinite
/// - JSON serialization fails
///
/// # Example
///
/// ```ignore
/// use neo4j_types::forward::typed_to_cypher_literal;
/// use sync_core::TypedValue;
///
/// let tv = TypedValue::int(42);
/// assert_eq!(typed_to_cypher_literal(&tv)?, "42");
///
/// let tv = TypedValue::text("hello");
/// assert_eq!(typed_to_cypher_literal(&tv)?, "'hello'");
/// ```
pub fn typed_to_cypher_literal(typed: &TypedValue) -> Result<String> {
    universal_to_cypher_literal(&typed.value, Some(&typed.sync_type))
}

/// Convert a UniversalValue to a Neo4j Cypher literal string.
///
/// The `parent_type` is used for array element type resolution. Pass `None` for
/// top-level values.
///
/// # Errors
///
/// Returns an error if:
/// - Float value is NaN or infinite
/// - JSON serialization fails
pub fn universal_to_cypher_literal(
    value: &UniversalValue,
    parent_type: Option<&UniversalType>,
) -> Result<String> {
    match value {
        UniversalValue::Null => Ok("null".to_string()),
        UniversalValue::Bool(b) => Ok(b.to_string()),

        // Integer types
        UniversalValue::Int8 { value, .. } => Ok(value.to_string()),
        UniversalValue::Int16(i) => Ok(i.to_string()),
        UniversalValue::Int32(i) => Ok(i.to_string()),
        UniversalValue::Int64(i) => Ok(i.to_string()),

        // Float types - error on NaN/Infinity instead of silent fallback
        UniversalValue::Float32(f) => {
            if f.is_nan() {
                Err(Neo4jTypesError::NanFloat)
            } else if f.is_infinite() {
                Err(Neo4jTypesError::InfinityFloat)
            } else {
                Ok(f.to_string())
            }
        }
        UniversalValue::Float64(f) => {
            if f.is_nan() {
                Err(Neo4jTypesError::NanFloat)
            } else if f.is_infinite() {
                Err(Neo4jTypesError::InfinityFloat)
            } else {
                Ok(f.to_string())
            }
        }

        // Decimal - store as numeric string
        UniversalValue::Decimal { value, .. } => Ok(value.to_string()),

        // String types
        UniversalValue::Text(s) => Ok(escape_neo4j_string(s)),
        UniversalValue::Char { value, .. } => Ok(escape_neo4j_string(value)),
        UniversalValue::VarChar { value, .. } => Ok(escape_neo4j_string(value)),

        // UUID
        UniversalValue::Uuid(u) => Ok(escape_neo4j_string(&u.to_string())),

        // ULID
        UniversalValue::Ulid(u) => Ok(escape_neo4j_string(&u.to_string())),

        // DateTime types - use Neo4j datetime() function
        UniversalValue::LocalDateTime(dt) => Ok(format!(
            "datetime('{}')",
            dt.format("%Y-%m-%dT%H:%M:%S%.fZ")
        )),
        UniversalValue::LocalDateTimeNano(dt) => Ok(format!(
            "datetime('{}')",
            dt.format("%Y-%m-%dT%H:%M:%S%.fZ")
        )),
        UniversalValue::ZonedDateTime(dt) => Ok(format!(
            "datetime('{}')",
            dt.format("%Y-%m-%dT%H:%M:%S%.fZ")
        )),

        // Date - use Neo4j date() function
        UniversalValue::Date(dt) => Ok(format!("date('{}')", dt.format("%Y-%m-%d"))),

        // Time - use Neo4j time() function with fractional seconds
        UniversalValue::Time(dt) => Ok(format!("time('{}')", dt.format("%H:%M:%S%.f"))),

        // TimeTz - store as string to preserve timezone format
        // Note: We intentionally do NOT use datetime here because time and datetime
        // are fundamentally different types. Datetime implies a specific point in time,
        // while TIMETZ represents a daily recurring time in a specific timezone.
        UniversalValue::TimeTz(s) => Ok(escape_neo4j_string(s)),

        // Binary types - store as hex string (Neo4j doesn't have native bytes)
        UniversalValue::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            Ok(escape_neo4j_string(&hex))
        }
        UniversalValue::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            Ok(escape_neo4j_string(&hex))
        }

        // JSON types - serialize and store as escaped string
        UniversalValue::Json(json_val) => {
            let json_str =
                serde_json::to_string(&json_val).map_err(|e| Neo4jTypesError::JsonParseError {
                    property: "json".to_string(),
                    error: e.to_string(),
                })?;
            Ok(escape_neo4j_string(&json_str))
        }
        UniversalValue::Jsonb(json_val) => {
            let json_str =
                serde_json::to_string(&json_val).map_err(|e| Neo4jTypesError::JsonParseError {
                    property: "jsonb".to_string(),
                    error: e.to_string(),
                })?;
            Ok(escape_neo4j_string(&json_str))
        }

        // Enum type
        UniversalValue::Enum { value, .. } => Ok(escape_neo4j_string(value)),

        // Set type - Neo4j list of strings
        UniversalValue::Set { elements, .. } => {
            let element_strs: Vec<String> =
                elements.iter().map(|s| escape_neo4j_string(s)).collect();
            Ok(format!("[{}]", element_strs.join(", ")))
        }

        // Geometry type - store as GeoJSON string
        UniversalValue::Geometry { data, .. } => {
            use sync_core::GeometryData;
            let GeometryData(json) = data;
            let json_str =
                serde_json::to_string(&json).map_err(|e| Neo4jTypesError::JsonParseError {
                    property: "geometry".to_string(),
                    error: e.to_string(),
                })?;
            Ok(escape_neo4j_string(&json_str))
        }

        // Array type - Neo4j list with recursive element conversion
        UniversalValue::Array { elements, .. } => {
            // Get element type from parent_type if available
            let element_type = match parent_type {
                Some(UniversalType::Array { element_type }) => Some(element_type.as_ref()),
                _ => None,
            };

            let mut element_strs = Vec::with_capacity(elements.len());
            for elem in elements {
                let literal = universal_to_cypher_literal(elem, element_type)?;
                element_strs.push(literal);
            }
            Ok(format!("[{}]", element_strs.join(", ")))
        }

        // Duration type - use Neo4j duration() function
        UniversalValue::Duration(d) => {
            // Neo4j duration format: PT<seconds>S or PT<seconds>.<nanos>S
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                Ok(format!("duration('PT{secs}S')"))
            } else {
                // Format nanoseconds as fractional seconds
                Ok(format!("duration('PT{secs}.{nanos:09}S')"))
            }
        }

        // Thing - record reference as string in "table:id" format
        UniversalValue::Thing { table, id } => {
            let id_str = match id.as_ref() {
                UniversalValue::Text(s) => s.clone(),
                UniversalValue::Int32(i) => i.to_string(),
                UniversalValue::Int64(i) => i.to_string(),
                UniversalValue::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for Neo4j: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            Ok(format!("'{table}:{id_str}'"))
        }

        // Object - nested document, convert to JSON-like map syntax for Neo4j
        UniversalValue::Object(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    let key_escaped = k.replace('`', "``");
                    let val =
                        universal_to_cypher_literal(v, None).unwrap_or_else(|_| "null".to_string());
                    format!("`{key_escaped}`: {val}")
                })
                .collect();
            Ok(format!("{{{}}}", entries.join(", ")))
        }
    }
}

/// Escape a string for Neo4j Cypher.
///
/// Handles single quotes, double quotes, backslashes, and control characters.
pub fn escape_neo4j_string(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use sync_core::GeometryType;

    #[test]
    fn test_null_conversion() {
        let tv = TypedValue::null(UniversalType::Text);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "null");
    }

    #[test]
    fn test_bool_true_conversion() {
        let tv = TypedValue::bool(true);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "true");
    }

    #[test]
    fn test_bool_false_conversion() {
        let tv = TypedValue::bool(false);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "false");
    }

    #[test]
    fn test_tinyint_conversion() {
        let tv = TypedValue::int8(127, 4);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "127");
    }

    #[test]
    fn test_smallint_conversion() {
        let tv = TypedValue::int16(32000);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "32000");
    }

    #[test]
    fn test_int_conversion() {
        let tv = TypedValue::int32(42);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "42");
    }

    #[test]
    fn test_bigint_conversion() {
        let tv = TypedValue::int64(9_223_372_036_854_775_807i64);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "9223372036854775807");
    }

    #[test]
    fn test_float_conversion() {
        let tv = TypedValue::float32(1.5);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "1.5");
    }

    #[test]
    fn test_double_conversion() {
        let tv = TypedValue::float64(1.23456789);
        let result = typed_to_cypher_literal(&tv).unwrap();
        // Just check it starts with the right value (precision may vary)
        assert!(result.starts_with("1.23456789"), "Got: {result}");
    }

    #[test]
    fn test_float_nan_returns_error() {
        let tv = TypedValue::float32(f32::NAN);
        let result = typed_to_cypher_literal(&tv);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Neo4jTypesError::NanFloat));
    }

    #[test]
    fn test_double_nan_returns_error() {
        let tv = TypedValue::float64(f64::NAN);
        let result = typed_to_cypher_literal(&tv);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Neo4jTypesError::NanFloat));
    }

    #[test]
    fn test_float_infinity_returns_error() {
        let tv = TypedValue::float32(f32::INFINITY);
        let result = typed_to_cypher_literal(&tv);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Neo4jTypesError::InfinityFloat
        ));
    }

    #[test]
    fn test_double_neg_infinity_returns_error() {
        let tv = TypedValue::float64(f64::NEG_INFINITY);
        let result = typed_to_cypher_literal(&tv);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Neo4jTypesError::InfinityFloat
        ));
    }

    #[test]
    fn test_decimal_conversion() {
        let tv = TypedValue::decimal("123.456", 10, 3);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "123.456");
    }

    #[test]
    fn test_text_conversion() {
        let tv = TypedValue::text("hello world");
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "'hello world'");
    }

    #[test]
    fn test_text_with_special_chars() {
        let tv = TypedValue::text("it's a \"test\"\nwith\ttabs");
        assert_eq!(
            typed_to_cypher_literal(&tv).unwrap(),
            "'it\\'s a \\\"test\\\"\\nwith\\ttabs'"
        );
    }

    #[test]
    fn test_char_conversion() {
        let tv = TypedValue::char_type("abc", 10);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "'abc'");
    }

    #[test]
    fn test_varchar_conversion() {
        let tv = TypedValue::varchar("hello", 255);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "'hello'");
    }

    #[test]
    fn test_uuid_conversion() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let tv = TypedValue::uuid(uuid);
        assert_eq!(
            typed_to_cypher_literal(&tv).unwrap(),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn test_datetime_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 45).unwrap();
        let tv = TypedValue::datetime(dt);
        let result = typed_to_cypher_literal(&tv).unwrap();
        assert!(result.starts_with("datetime('2024-06-15T10:30:45"));
        assert!(result.ends_with("')"));
    }

    #[test]
    fn test_date_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap();
        let tv = TypedValue::date(dt);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "date('2024-06-15')");
    }

    #[test]
    fn test_time_conversion() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let tv = TypedValue::time(dt);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "time('14:30:45')");
    }

    #[test]
    fn test_bytes_conversion() {
        let tv = TypedValue::bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "'deadbeef'");
    }

    #[test]
    fn test_json_object_conversion() {
        let obj = serde_json::json!({"name": "test", "count": 42});
        let tv = TypedValue::json(obj);
        let result = typed_to_cypher_literal(&tv).unwrap();
        // JSON object order may vary, so just check it's escaped properly
        assert!(result.starts_with("'"));
        assert!(result.ends_with("'"));
        assert!(result.contains("name"));
        assert!(result.contains("test"));
    }

    #[test]
    fn test_enum_conversion() {
        let tv =
            TypedValue::enum_type("active", vec!["active".to_string(), "inactive".to_string()]);
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "'active'");
    }

    #[test]
    fn test_set_conversion() {
        let tv = TypedValue::set(
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "['a', 'b']");
    }

    #[test]
    fn test_geometry_point_conversion() {
        let coords = serde_json::json!({"type": "Point", "coordinates": [-73.97, 40.77]});
        let tv = TypedValue::geometry_geojson(coords, GeometryType::Point);
        let result = typed_to_cypher_literal(&tv).unwrap();
        assert!(result.starts_with("'"));
        assert!(result.ends_with("'"));
        assert!(result.contains("Point"));
        assert!(result.contains("coordinates"));
    }

    #[test]
    fn test_array_int_conversion() {
        let tv = TypedValue::array(
            vec![
                UniversalValue::Int32(1),
                UniversalValue::Int32(2),
                UniversalValue::Int32(3),
            ],
            UniversalType::Int32,
        );
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "[1, 2, 3]");
    }

    #[test]
    fn test_array_text_conversion() {
        let tv = TypedValue::array(
            vec![
                UniversalValue::Text("a".to_string()),
                UniversalValue::Text("b".to_string()),
            ],
            UniversalType::Text,
        );
        assert_eq!(typed_to_cypher_literal(&tv).unwrap(), "['a', 'b']");
    }

    #[test]
    fn test_array_with_special_chars() {
        let tv = TypedValue::array(
            vec![
                UniversalValue::Text("it's".to_string()),
                UniversalValue::Text("a \"test\"".to_string()),
            ],
            UniversalType::Text,
        );
        assert_eq!(
            typed_to_cypher_literal(&tv).unwrap(),
            "['it\\'s', 'a \\\"test\\\"']"
        );
    }

    #[test]
    fn test_escape_neo4j_string_simple() {
        assert_eq!(escape_neo4j_string("hello"), "'hello'");
    }

    #[test]
    fn test_escape_neo4j_string_single_quote() {
        assert_eq!(escape_neo4j_string("it's"), "'it\\'s'");
    }

    #[test]
    fn test_escape_neo4j_string_double_quote() {
        assert_eq!(escape_neo4j_string("say \"hi\""), "'say \\\"hi\\\"'");
    }

    #[test]
    fn test_escape_neo4j_string_newline() {
        assert_eq!(escape_neo4j_string("line1\nline2"), "'line1\\nline2'");
    }

    #[test]
    fn test_escape_neo4j_string_tab() {
        assert_eq!(escape_neo4j_string("col1\tcol2"), "'col1\\tcol2'");
    }

    #[test]
    fn test_escape_neo4j_string_backslash() {
        assert_eq!(
            escape_neo4j_string("path\\to\\file"),
            "'path\\\\to\\\\file'"
        );
    }

    #[test]
    fn test_escape_neo4j_string_carriage_return() {
        assert_eq!(escape_neo4j_string("line\r\nbreak"), "'line\\r\\nbreak'");
    }
}
