//! Reverse conversion: CSV string → TypedValue.
//!
//! This module provides conversion from CSV string values to sync-core's `TypedValue`.

use base64::Engine;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use std::collections::HashMap;
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
        UniversalType::TinyInt { width: 1 } => match value.to_lowercase().as_str() {
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

        // Integer types
        UniversalType::TinyInt { .. } | UniversalType::SmallInt | UniversalType::Int => {
            match value.parse::<i32>() {
                Ok(i) => Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::Int32(i),
                }),
                Err(_) => Err(CsvParseError {
                    message: "Invalid integer".to_string(),
                    value: value.to_string(),
                    expected_type: format!("{schema_type:?}"),
                }),
            }
        }

        UniversalType::BigInt => match value.parse::<i64>() {
            Ok(i) => Ok(TypedValue::bigint(i)),
            Err(_) => Err(CsvParseError {
                message: "Invalid bigint".to_string(),
                value: value.to_string(),
                expected_type: "BigInt".to_string(),
            }),
        },

        // Float types
        UniversalType::Float | UniversalType::Double => match value.parse::<f64>() {
            Ok(f) => Ok(TypedValue {
                sync_type: schema_type.clone(),
                value: UniversalValue::Float64(f),
            }),
            Err(_) => Err(CsvParseError {
                message: "Invalid float".to_string(),
                value: value.to_string(),
                expected_type: format!("{schema_type:?}"),
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

        // String types
        UniversalType::Char { .. } | UniversalType::VarChar { .. } | UniversalType::Text => {
            Ok(TypedValue {
                sync_type: schema_type.clone(),
                value: UniversalValue::String(value.to_string()),
            })
        }

        // Binary types - base64 decode
        UniversalType::Blob | UniversalType::Bytes => {
            match base64::engine::general_purpose::STANDARD.decode(value) {
                Ok(bytes) => Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::Bytes(bytes),
                }),
                Err(_) => Err(CsvParseError {
                    message: "Invalid base64".to_string(),
                    value: value.to_string(),
                    expected_type: "Bytes".to_string(),
                }),
            }
        }

        // Date - YYYY-MM-DD format
        UniversalType::Date => match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
            Ok(date) => {
                let dt = date.and_hms_opt(0, 0, 0).unwrap();
                Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::DateTime(Utc.from_utc_datetime(&dt)),
                })
            }
            Err(_) => Err(CsvParseError {
                message: "Invalid date format (expected YYYY-MM-DD)".to_string(),
                value: value.to_string(),
                expected_type: "Date".to_string(),
            }),
        },

        // Time - HH:MM:SS format
        UniversalType::Time => {
            match NaiveTime::parse_from_str(value, "%H:%M:%S") {
                Ok(time) => {
                    // Use epoch date as placeholder
                    let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .and_time(time);
                    Ok(TypedValue {
                        sync_type: schema_type.clone(),
                        value: UniversalValue::DateTime(Utc.from_utc_datetime(&dt)),
                    })
                }
                Err(_) => Err(CsvParseError {
                    message: "Invalid time format (expected HH:MM:SS)".to_string(),
                    value: value.to_string(),
                    expected_type: "Time".to_string(),
                }),
            }
        }

        // DateTime types - RFC3339 format with fallback
        UniversalType::DateTime | UniversalType::DateTimeNano | UniversalType::TimestampTz => {
            // Try RFC3339 first
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                return Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::DateTime(dt.with_timezone(&Utc)),
                });
            }
            // Fallback: try parsing without timezone
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
                return Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::DateTime(ndt.and_utc()),
                });
            }
            // Fallback: try parsing with space instead of T
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                return Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::DateTime(ndt.and_utc()),
                });
            }
            Err(CsvParseError {
                message: "Invalid datetime format".to_string(),
                value: value.to_string(),
                expected_type: "DateTime".to_string(),
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
                        value: UniversalValue::Array(items?),
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

        // Set type - parse comma-separated values
        UniversalType::Set { values: _ } => {
            let items: Vec<UniversalValue> = value
                .split(',')
                .map(|s| UniversalValue::String(s.trim().to_string()))
                .collect();
            Ok(TypedValue {
                sync_type: schema_type.clone(),
                value: UniversalValue::Array(items),
            })
        }

        // Enum - validate against allowed values (or just pass through)
        UniversalType::Enum { values } => {
            if values.contains(&value.to_string()) {
                Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::String(value.to_string()),
                })
            } else {
                // Be lenient - allow any string value
                Ok(TypedValue {
                    sync_type: schema_type.clone(),
                    value: UniversalValue::String(value.to_string()),
                })
            }
        }

        // Geometry types - parse GeoJSON
        UniversalType::Geometry { geometry_type } => {
            match serde_json::from_str::<serde_json::Value>(value) {
                Ok(json) => {
                    let gen_value = json_to_generated_value(&json);
                    Ok(TypedValue {
                        sync_type: UniversalType::Geometry {
                            geometry_type: geometry_type.clone(),
                        },
                        value: gen_value,
                    })
                }
                Err(_) => Err(CsvParseError {
                    message: "Invalid GeoJSON".to_string(),
                    value: value.to_string(),
                    expected_type: "Geometry".to_string(),
                }),
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
                UniversalValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => UniversalValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            UniversalValue::Array(arr.iter().map(json_to_generated_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_generated_value(v));
            }
            UniversalValue::Object(map)
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
        return TypedValue::bigint(i);
    }

    // Try float
    if let Ok(f) = value.parse::<f64>() {
        return TypedValue::double(f);
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
        let result = csv_string_to_typed_value("12345", &UniversalType::Int).unwrap();
        assert_eq!(result.value.as_i32(), Some(12345));
    }

    #[test]
    fn test_reverse_bigint() {
        let result = csv_string_to_typed_value("9876543210", &UniversalType::BigInt).unwrap();
        assert_eq!(result.value.as_i64(), Some(9876543210i64));
    }

    #[test]
    fn test_reverse_float() {
        let result = csv_string_to_typed_value("1.23", &UniversalType::Double).unwrap();
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
        let dt = result.value.as_datetime().unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
    }

    #[test]
    fn test_reverse_time() {
        let result = csv_string_to_typed_value("14:30:45", &UniversalType::Time).unwrap();
        let dt = result.value.as_datetime().unwrap();
        assert_eq!(dt.format("%H:%M:%S").to_string(), "14:30:45");
    }

    #[test]
    fn test_reverse_datetime_rfc3339() {
        let result =
            csv_string_to_typed_value("2024-06-15T10:30:00+00:00", &UniversalType::DateTime)
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
            csv_string_to_typed_value("2024-06-15T10:30:00", &UniversalType::DateTime).unwrap();
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
            UniversalValue::Object(map) => {
                assert_eq!(
                    map.get("key"),
                    Some(&UniversalValue::String("value".to_string()))
                );
                assert_eq!(map.get("count"), Some(&UniversalValue::Int64(42)));
            }
            _ => panic!("Expected Object"),
        }
    }

    #[test]
    fn test_reverse_array_int() {
        let result = csv_string_to_typed_value(
            "[1, 2, 3]",
            &UniversalType::Array {
                element_type: Box::new(UniversalType::Int),
            },
        )
        .unwrap();
        match &result.value {
            UniversalValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0].as_i32(), Some(1));
                assert_eq!(arr[1].as_i32(), Some(2));
                assert_eq!(arr[2].as_i32(), Some(3));
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
            UniversalValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0].as_str(), Some("a"));
                assert_eq!(arr[1].as_str(), Some("b"));
                assert_eq!(arr[2].as_str(), Some("c"));
            }
            _ => panic!("Expected Array"),
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
        assert_eq!(result.value.as_str(), Some("active"));
    }

    #[test]
    fn test_reverse_geometry() {
        let result = csv_string_to_typed_value(
            r#"{"type": "Point", "coordinates": [-73.97, 40.77]}"#,
            &UniversalType::Geometry {
                geometry_type: GeometryType::Point,
            },
        )
        .unwrap();
        match &result.value {
            UniversalValue::Object(map) => {
                assert!(map.contains_key("type"));
                assert!(map.contains_key("coordinates"));
            }
            _ => panic!("Expected Object"),
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
        let schema_type = UniversalType::Int;
        let csv_str = CsvStringWithSchema::new("42", &schema_type);
        let result = csv_str.to_typed_value().unwrap();
        assert_eq!(result.value.as_i32(), Some(42));
    }
}
