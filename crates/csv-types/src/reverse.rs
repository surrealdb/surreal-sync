//! Reverse conversion: CSV string → TypedValue.
//!
//! This module provides conversion from CSV string values to sync-core's `TypedValue`.

use base64::Engine;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use sync_core::{TypedValue, UniversalType, UniversalValue};

/// A CSV string with schema information for reverse conversion.
///
/// This struct enables converting CSV string values to `TypedValue` using
/// schema information to guide parsing.
#[derive(Debug, Clone)]
pub struct CsvStringWithSchema<'a> {
    /// The CSV string value
    pub value: &'a str,
    /// The target schema type
    pub schema_type: &'a UniversalType,
}

impl<'a> CsvStringWithSchema<'a> {
    /// Create a new CSV string with schema.
    pub fn new(value: &'a str, schema_type: &'a UniversalType) -> Self {
        Self { value, schema_type }
    }

    /// Convert to TypedValue based on schema.
    pub fn to_typed_value(&self) -> Result<TypedValue, CsvParseError> {
        csv_string_to_typed_value(self.value, self.schema_type)
    }
}

/// Error type for CSV parsing failures.
#[derive(Debug, Clone)]
pub struct CsvParseError {
    pub message: String,
    pub value: String,
    pub expected_type: String,
}

impl std::fmt::Display for CsvParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to parse '{}' as {}: {}",
            self.value, self.expected_type, self.message
        )
    }
}

impl std::error::Error for CsvParseError {}

/// Parse a CSV string value according to the schema type.
///
/// This is the reverse of `CsvValue::from(TypedValue)`.
pub fn csv_string_to_typed_value(
    value: &str,
    schema_type: &UniversalType,
) -> Result<TypedValue, CsvParseError> {
    // Handle empty string as null for most types
    if value.is_empty() {
        return Ok(TypedValue::null(schema_type.clone()));
    }

    match schema_type {
        // Boolean - lenient parsing
        UniversalType::Bool => match value.to_lowercase().as_str() {
            "true" | "1" | "yes" | "t" | "y" => Ok(TypedValue::bool(true)),
            "false" | "0" | "no" | "f" | "n" => Ok(TypedValue::bool(false)),
            _ => Err(CsvParseError {
                message: "Invalid boolean value".to_string(),
                value: value.to_string(),
                expected_type: "Bool".to_string(),
            }),
        },

        // TinyInt with width 1 - treat as boolean (MySQL pattern)
        UniversalType::Int8 { width: 1 } => match value.to_lowercase().as_str() {
            "true" | "1" | "yes" | "t" | "y" => Ok(TypedValue {
                sync_type: schema_type.clone(),
                value: UniversalValue::Bool(true),
            }),
            "false" | "0" | "no" | "f" | "n" => Ok(TypedValue {
                sync_type: schema_type.clone(),
                value: UniversalValue::Bool(false),
            }),
            _ => Err(CsvParseError {
                message: "Invalid boolean value".to_string(),
                value: value.to_string(),
                expected_type: "TinyInt(1)".to_string(),
            }),
        },

        // Integer types - strict 1:1 matching
        UniversalType::Int8 { width } => match value.parse::<i8>() {
            Ok(i) => Ok(TypedValue::int8(i, *width)),
            Err(_) => Err(CsvParseError {
                message: "Invalid tinyint".to_string(),
                value: value.to_string(),
                expected_type: "TinyInt".to_string(),
            }),
        },
        UniversalType::Int16 => match value.parse::<i16>() {
            Ok(i) => Ok(TypedValue::int16(i)),
            Err(_) => Err(CsvParseError {
                message: "Invalid smallint".to_string(),
                value: value.to_string(),
                expected_type: "SmallInt".to_string(),
            }),
        },
        UniversalType::Int32 => match value.parse::<i32>() {
            Ok(i) => Ok(TypedValue::int32(i)),
            Err(_) => Err(CsvParseError {
                message: "Invalid integer".to_string(),
                value: value.to_string(),
                expected_type: "Int".to_string(),
            }),
        },

        UniversalType::Int64 => match value.parse::<i64>() {
            Ok(i) => Ok(TypedValue::int64(i)),
            Err(_) => Err(CsvParseError {
                message: "Invalid bigint".to_string(),
                value: value.to_string(),
                expected_type: "BigInt".to_string(),
            }),
        },

        // Float types - strict 1:1 matching
        UniversalType::Float32 => match value.parse::<f32>() {
            Ok(f) => Ok(TypedValue::float32(f)),
            Err(_) => Err(CsvParseError {
                message: "Invalid float".to_string(),
                value: value.to_string(),
                expected_type: "Float".to_string(),
            }),
        },
        UniversalType::Float64 => match value.parse::<f64>() {
            Ok(f) => Ok(TypedValue::float64(f)),
            Err(_) => Err(CsvParseError {
                message: "Invalid double".to_string(),
                value: value.to_string(),
                expected_type: "Double".to_string(),
            }),
        },

        // Decimal
        UniversalType::Decimal { precision, scale } => {
            // Validate it's a valid decimal format
            if value.parse::<f64>().is_ok()
                || value
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
            {
                Ok(TypedValue::decimal(value, *precision, *scale))
            } else {
                Err(CsvParseError {
                    message: "Invalid decimal".to_string(),
                    value: value.to_string(),
                    expected_type: "Decimal".to_string(),
                })
            }
        }

        // String types - strict 1:1 matching
        UniversalType::Char { length } => Ok(TypedValue::char_type(value, *length)),
        UniversalType::VarChar { length } => Ok(TypedValue::varchar(value, *length)),
        UniversalType::Text => Ok(TypedValue::text(value)),

        // Binary types - strict 1:1 matching
        UniversalType::Blob => match base64::engine::general_purpose::STANDARD.decode(value) {
            Ok(bytes) => Ok(TypedValue::blob(bytes)),
            Err(_) => Err(CsvParseError {
                message: "Invalid base64".to_string(),
                value: value.to_string(),
                expected_type: "Blob".to_string(),
            }),
        },
        UniversalType::Bytes => match base64::engine::general_purpose::STANDARD.decode(value) {
            Ok(bytes) => Ok(TypedValue::bytes(bytes)),
            Err(_) => Err(CsvParseError {
                message: "Invalid base64".to_string(),
                value: value.to_string(),
                expected_type: "Bytes".to_string(),
            }),
        },

        // Date - strict 1:1 matching
        UniversalType::Date => match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            Ok(date) => {
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                Ok(TypedValue::date(Utc.from_utc_datetime(&dt)))
            }
            Err(_) => Err(CsvParseError {
                message: "Invalid date format (expected YYYY-MM-DD)".to_string(),
                value: value.to_string(),
                expected_type: "Date".to_string(),
            }),
        },

        // Time - strict 1:1 matching
        UniversalType::Time => {
            match NaiveTime::parse_from_str(value, "%H:%M:%S") {
                Ok(time) => {
                    // Use epoch date as placeholder
                    let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    Ok(TypedValue::time(Utc.from_utc_datetime(&dt)))
                }
                Err(_) => Err(CsvParseError {
                    message: "Invalid time format (expected HH:MM:SS)".to_string(),
                    value: value.to_string(),
                    expected_type: "Time".to_string(),
                }),
            }
        }

        // DateTime types - strict 1:1 matching
        UniversalType::LocalDateTime => {
            // Try RFC3339 first
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                return Ok(TypedValue::datetime(dt.with_timezone(&Utc)));
            }
            // Fallback: try parsing without timezone
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
                return Ok(TypedValue::datetime(ndt.and_utc()));
            }
            // Fallback: try parsing with space instead of T
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                return Ok(TypedValue::datetime(ndt.and_utc()));
            }
            Err(CsvParseError {
                message: "Invalid datetime format".to_string(),
                value: value.to_string(),
                expected_type: "DateTime".to_string(),
            })
        }
        UniversalType::LocalDateTimeNano => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                return Ok(TypedValue::datetime_nano(dt.with_timezone(&Utc)));
            }
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
                return Ok(TypedValue::datetime_nano(ndt.and_utc()));
            }
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                return Ok(TypedValue::datetime_nano(ndt.and_utc()));
            }
            Err(CsvParseError {
                message: "Invalid datetime format".to_string(),
                value: value.to_string(),
                expected_type: "DateTimeNano".to_string(),
            })
        }
        UniversalType::ZonedDateTime => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                return Ok(TypedValue::timestamptz(dt.with_timezone(&Utc)));
            }
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
                return Ok(TypedValue::timestamptz(ndt.and_utc()));
            }
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                return Ok(TypedValue::timestamptz(ndt.and_utc()));
            }
            Err(CsvParseError {
                message: "Invalid datetime format".to_string(),
                value: value.to_string(),
                expected_type: "TimestampTz".to_string(),
            })
        }

        // UUID
        UniversalType::Uuid => match uuid::Uuid::parse_str(value) {
            Ok(uuid) => Ok(TypedValue::uuid(uuid)),
            Err(_) => Err(CsvParseError {
                message: "Invalid UUID".to_string(),
                value: value.to_string(),
                expected_type: "Uuid".to_string(),
            }),
        },

        // ULID
        UniversalType::Ulid => match ulid::Ulid::from_string(value) {
            Ok(ulid) => Ok(TypedValue::ulid(ulid)),
            Err(_) => Err(CsvParseError {
                message: "Invalid ULID".to_string(),
                value: value.to_string(),
                expected_type: "Ulid".to_string(),
            }),
        },

        // JSON types - parse JSON string
        UniversalType::Json | UniversalType::Jsonb => {
            match serde_json::from_str::<serde_json::Value>(value) {
                Ok(json) => {
                    let gen_value = json_to_generated_value(&json);
                    Ok(TypedValue {
                        sync_type: schema_type.clone(),
                        value: gen_value,
                    })
                }
                Err(_) => Err(CsvParseError {
                    message: "Invalid JSON".to_string(),
                    value: value.to_string(),
                    expected_type: "Json".to_string(),
                }),
            }
        }

        // Array types - parse JSON array with element type coercion
        UniversalType::Array { element_type } => {
            match serde_json::from_str::<serde_json::Value>(value) {
                Ok(serde_json::Value::Array(arr)) => {
                    let items: Result<Vec<UniversalValue>, CsvParseError> = arr
                        .into_iter()
                        .map(|item| {
                            let item_str = match &item {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            csv_string_to_typed_value(&item_str, element_type).map(|tv| tv.value)
                        })
                        .collect();
                    Ok(TypedValue {
                        sync_type: schema_type.clone(),
                        value: UniversalValue::Array {
                            elements: items?,
                            element_type: element_type.clone(),
                        },
                    })
                }
                Ok(_) => Err(CsvParseError {
                    message: "Expected JSON array".to_string(),
                    value: value.to_string(),
                    expected_type: "Array".to_string(),
                }),
                Err(_) => Err(CsvParseError {
                    message: "Invalid JSON array".to_string(),
                    value: value.to_string(),
                    expected_type: "Array".to_string(),
                }),
            }
        }

        // Set type - strict 1:1 matching
        UniversalType::Set {
            values: allowed_values,
        } => {
            let items: Vec<String> = value.split(',').map(|s| s.trim().to_string()).collect();
            Ok(TypedValue::set(items, allowed_values.clone()))
        }

        // Enum - strict 1:1 matching
        UniversalType::Enum { values } => Ok(TypedValue::enum_type(value, values.clone())),

        // Geometry types - strict 1:1 matching
        UniversalType::Geometry { geometry_type } => {
            match serde_json::from_str::<serde_json::Value>(value) {
                Ok(json) => Ok(TypedValue::geometry_geojson(json, geometry_type.clone())),
                Err(_) => Err(CsvParseError {
                    message: "Invalid GeoJSON".to_string(),
                    value: value.to_string(),
                    expected_type: "Geometry".to_string(),
                }),
            }
        }

        UniversalType::Duration => {
            // Parse ISO 8601 duration format (e.g., "PT1H30M45S")
            // For simplicity, we only support "PTxS" or "PTx.xxxxxxxxxS" format
            let trimmed = value.trim();
            if let Some(secs_str) = trimmed.strip_prefix("PT").and_then(|s| s.strip_suffix('S')) {
                if let Some(dot_pos) = secs_str.find('.') {
                    let secs: u64 = secs_str[..dot_pos].parse().map_err(|_| CsvParseError {
                        message: "Invalid duration seconds".to_string(),
                        value: value.to_string(),
                        expected_type: "Duration".to_string(),
                    })?;
                    let nanos_str = &secs_str[dot_pos + 1..];
                    let nanos: u32 = nanos_str.parse().map_err(|_| CsvParseError {
                        message: "Invalid duration nanoseconds".to_string(),
                        value: value.to_string(),
                        expected_type: "Duration".to_string(),
                    })?;
                    Ok(TypedValue::duration(std::time::Duration::new(secs, nanos)))
                } else {
                    let secs: u64 = secs_str.parse().map_err(|_| CsvParseError {
                        message: "Invalid duration seconds".to_string(),
                        value: value.to_string(),
                        expected_type: "Duration".to_string(),
                    })?;
                    Ok(TypedValue::duration(std::time::Duration::from_secs(secs)))
                }
            } else {
                Err(CsvParseError {
                    message: "Invalid duration format, expected PTxS".to_string(),
                    value: value.to_string(),
                    expected_type: "Duration".to_string(),
                })
            }
        }

        // Thing - parse "table:id" format
        UniversalType::Thing => {
            if let Some((table, id_part)) = value.split_once(':') {
                Ok(TypedValue {
                    sync_type: UniversalType::Thing,
                    value: UniversalValue::Thing {
                        table: table.to_string(),
                        id: Box::new(UniversalValue::Text(id_part.to_string())),
                    },
                })
            } else {
                Err(CsvParseError {
                    message: "Invalid thing format, expected table:id".to_string(),
                    value: value.to_string(),
                    expected_type: "Thing".to_string(),
                })
            }
        }
    }
}

/// Convert a serde_json::Value to UniversalValue.
fn json_to_generated_value(json: &serde_json::Value) -> UniversalValue {
    match json {
        serde_json::Value::Null => UniversalValue::Null,
        serde_json::Value::Bool(b) => UniversalValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                UniversalValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                UniversalValue::Float64(f)
            } else {
                UniversalValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => UniversalValue::Text(s.clone()),
        serde_json::Value::Array(arr) => UniversalValue::Array {
            elements: arr.iter().map(json_to_generated_value).collect(),
            element_type: Box::new(UniversalType::Text),
        },
        serde_json::Value::Object(obj) => {
            UniversalValue::Json(Box::new(serde_json::Value::Object(obj.clone())))
        }
    }
}

/// Parse a CSV string without schema (best-effort type inference).
///
/// Tries to parse as: number, boolean, then falls back to string.
pub fn csv_string_to_typed_value_inferred(value: &str) -> TypedValue {
    if value.is_empty() {
        return TypedValue::null(UniversalType::Text);
    }

    // Try integer
    if let Ok(i) = value.parse::<i64>() {
        return TypedValue::int64(i);
    }

    // Try float
    if let Ok(f) = value.parse::<f64>() {
        return TypedValue::float64(f);
    }

    // Try boolean
    match value.to_lowercase().as_str() {
        "true" | "false" => return TypedValue::bool(value.to_lowercase() == "true"),
        _ => {}
    }

    // Default to string
    TypedValue::text(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::GeometryType;

    #[test]
    fn test_reverse_null() {
        let result = csv_string_to_typed_value("", &UniversalType::Text).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_reverse_bool_true() {
        for input in &["true", "TRUE", "1", "yes", "YES", "t", "T", "y", "Y"] {
            let result = csv_string_to_typed_value(input, &UniversalType::Bool).unwrap();
            assert_eq!(
                result.value.as_bool(),
                Some(true),
                "Failed for input: {input}"
            );
        }
    }

    #[test]
    fn test_reverse_bool_false() {
        for input in &["false", "FALSE", "0", "no", "NO", "f", "F", "n", "N"] {
            let result = csv_string_to_typed_value(input, &UniversalType::Bool).unwrap();
            assert_eq!(
                result.value.as_bool(),
                Some(false),
                "Failed for input: {input}"
            );
        }
    }

    #[test]
    fn test_reverse_int() {
        let result = csv_string_to_typed_value("12345", &UniversalType::Int32).unwrap();
        assert_eq!(result.value.as_i32(), Some(12345));
    }

    #[test]
    fn test_reverse_bigint() {
        let result = csv_string_to_typed_value("9876543210", &UniversalType::Int64).unwrap();
        assert_eq!(result.value.as_i64(), Some(9876543210i64));
    }

    #[test]
    fn test_reverse_float() {
        let result = csv_string_to_typed_value("1.23", &UniversalType::Float64).unwrap();
        assert!((result.value.as_f64().unwrap() - 1.23).abs() < 0.001);
    }

    #[test]
    fn test_reverse_decimal() {
        let result = csv_string_to_typed_value(
            "123.45",
            &UniversalType::Decimal {
                precision: 10,
                scale: 2,
            },
        )
        .unwrap();
        match &result.value {
            UniversalValue::Decimal { value, .. } => assert_eq!(value, "123.45"),
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_reverse_text() {
        let result = csv_string_to_typed_value("hello world", &UniversalType::Text).unwrap();
        assert_eq!(result.value.as_str(), Some("hello world"));
    }

    #[test]
    fn test_reverse_bytes() {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let result = csv_string_to_typed_value(&encoded, &UniversalType::Bytes).unwrap();
        assert_eq!(result.value.as_bytes(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn test_reverse_uuid() {
        let result =
            csv_string_to_typed_value("550e8400-e29b-41d4-a716-446655440000", &UniversalType::Uuid)
                .unwrap();
        assert_eq!(
            result.value.as_uuid().unwrap().to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_reverse_date() {
        let result = csv_string_to_typed_value("2024-06-15", &UniversalType::Date).unwrap();
        if let UniversalValue::Date(dt) = result.value {
            assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
        } else {
            panic!("Expected Date, got {:?}", result.value);
        }
    }

    #[test]
    fn test_reverse_time() {
        let result = csv_string_to_typed_value("14:30:45", &UniversalType::Time).unwrap();
        if let UniversalValue::Time(dt) = result.value {
            assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
        } else {
            panic!("Expected Time, got {:?}", result.value);
        }
    }

    #[test]
    fn test_reverse_datetime_rfc3339() {
        let result =
            csv_string_to_typed_value("2024-06-15T10:30:00+00:00", &UniversalType::LocalDateTime)
                .unwrap();
        let dt = result.value.as_datetime().unwrap();
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "2024-06-15T10:30:00"
        );
    }

    #[test]
    fn test_reverse_datetime_no_timezone() {
        let result =
            csv_string_to_typed_value("2024-06-15T10:30:00", &UniversalType::LocalDateTime)
                .unwrap();
        let dt = result.value.as_datetime().unwrap();
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            "2024-06-15T10:30:00"
        );
    }

    #[test]
    fn test_reverse_json_object() {
        let result =
            csv_string_to_typed_value(r#"{"key": "value", "count": 42}"#, &UniversalType::Json)
                .unwrap();
        match &result.value {
            UniversalValue::Json(json_val) => {
                if let serde_json::Value::Object(map) = json_val.as_ref() {
                    assert_eq!(
                        map.get("key"),
                        Some(&serde_json::Value::String("value".to_string()))
                    );
                    assert_eq!(
                        map.get("count"),
                        Some(&serde_json::Value::Number(serde_json::Number::from(42)))
                    );
                } else {
                    panic!("Expected Object");
                }
            }
            _ => panic!("Expected Json"),
        }
    }

    #[test]
    fn test_reverse_array_int() {
        let result = csv_string_to_typed_value(
            "[1, 2, 3]",
            &UniversalType::Array {
                element_type: Box::new(UniversalType::Int32),
            },
        )
        .unwrap();
        match &result.value {
            UniversalValue::Array { elements, .. } => {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0].as_i32(), Some(1));
                assert_eq!(elements[1].as_i32(), Some(2));
                assert_eq!(elements[2].as_i32(), Some(3));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_reverse_set() {
        let result = csv_string_to_typed_value(
            "a,b,c",
            &UniversalType::Set {
                values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
        )
        .unwrap();
        match &result.value {
            UniversalValue::Set { elements, .. } => {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], "a");
                assert_eq!(elements[1], "b");
                assert_eq!(elements[2], "c");
            }
            _ => panic!("Expected Set, got {:?}", result.value),
        }
    }

    #[test]
    fn test_reverse_enum() {
        let result = csv_string_to_typed_value(
            "active",
            &UniversalType::Enum {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        )
        .unwrap();
        if let UniversalValue::Enum { value, .. } = result.value {
            assert_eq!(value, "active");
        } else {
            panic!("Expected Enum, got {:?}", result.value);
        }
    }

    #[test]
    fn test_reverse_geometry() {
        use sync_core::values::GeometryData;
        let result = csv_string_to_typed_value(
            r#"{"type": "Point", "coordinates": [-73.97, 40.77]}"#,
            &UniversalType::Geometry {
                geometry_type: GeometryType::Point,
            },
        )
        .unwrap();
        match &result.value {
            UniversalValue::Geometry { data, .. } => {
                let GeometryData(geo_json) = data;
                if let serde_json::Value::Object(map) = geo_json {
                    assert!(map.contains_key("type"));
                    assert!(map.contains_key("coordinates"));
                } else {
                    panic!("Expected Object inside GeometryData");
                }
            }
            _ => panic!("Expected Geometry, got {:?}", result.value),
        }
    }

    #[test]
    fn test_reverse_inferred() {
        // Integer
        let result = csv_string_to_typed_value_inferred("42");
        assert_eq!(result.value.as_i64(), Some(42));

        // Float
        let result = csv_string_to_typed_value_inferred("3.15");
        assert!((result.value.as_f64().unwrap() - 3.15).abs() < 0.001);

        // Boolean
        let result = csv_string_to_typed_value_inferred("true");
        assert_eq!(result.value.as_bool(), Some(true));

        // String
        let result = csv_string_to_typed_value_inferred("hello");
        assert_eq!(result.value.as_str(), Some("hello"));
    }

    #[test]
    fn test_csv_string_with_schema() {
        let schema_type = UniversalType::Int32;
        let csv_str = CsvStringWithSchema::new("42", &schema_type);
        let result = csv_str.to_typed_value().unwrap();
        assert_eq!(result.value.as_i32(), Some(42));
    }
}
