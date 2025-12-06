//! Field comparison logic.

use serde_json;
use std::collections::HashMap;
use surrealdb::sql::Value as SurrealValue;
use sync_core::GeneratedValue;

/// Result of comparing two values.
#[derive(Debug, Clone, PartialEq)]
pub enum CompareResult {
    /// Values match.
    Match,
    /// Values don't match.
    Mismatch { expected: String, actual: String },
    /// Field is missing.
    Missing,
}

/// Options for configuring comparison behavior.
///
/// These options enable workarounds for sources that don't yet fully preserve
/// type information when syncing to SurrealDB. Each option should be removed
/// once the underlying source is enhanced to properly handle the data type.
#[derive(Debug, Clone, Default)]
pub struct CompareOptions {
    /// Accept JSON objects stored as strings (e.g., `{"key": "value"}` stored as `"{"key":"value"}"`).
    ///
    /// # When to use
    /// Enable this for Kafka sources where the protobuf encoder serializes JSON/Object
    /// fields as strings rather than native SurrealDB objects.
    ///
    /// # Future work: Remove this workaround
    /// The Kafka source should be enhanced to:
    /// 1. Read the SyncSchema to identify JSON/Object field types
    /// 2. Parse the protobuf string field as JSON
    /// 3. Store as native SurrealDB Object type instead of Strand
    ///
    /// Once implemented, this flag can be removed and tests will pass without it.
    pub accept_object_as_json_string: bool,

    /// Accept empty arrays as equivalent to missing fields.
    ///
    /// # When to use
    /// Enable this for Kafka sources where protobuf doesn't write empty repeated fields,
    /// causing the field to be entirely absent from the synced document.
    ///
    /// # Future work: Remove this workaround
    /// The Kafka source should be enhanced to:
    /// 1. Read the SyncSchema to identify Array field types
    /// 2. For array fields not present in the protobuf message, explicitly write an empty array `[]`
    ///
    /// Once implemented, this flag can be removed and tests will pass without it.
    pub accept_missing_as_empty_array: bool,
}

/// Compare a generated value with a SurrealDB value using default options.
pub fn compare_values(expected: &GeneratedValue, actual: &SurrealValue) -> CompareResult {
    compare_values_with_options(expected, actual, &CompareOptions::default())
}

/// Compare a generated value with a SurrealDB value with configurable options.
pub fn compare_values_with_options(
    expected: &GeneratedValue,
    actual: &SurrealValue,
    options: &CompareOptions,
) -> CompareResult {
    match (expected, actual) {
        // Null comparison
        (GeneratedValue::Null, SurrealValue::None) => CompareResult::Match,
        (GeneratedValue::Null, SurrealValue::Null) => CompareResult::Match,

        // Boolean comparison
        (GeneratedValue::Bool(e), SurrealValue::Bool(a)) => {
            if e == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }

        // Integer comparisons
        (GeneratedValue::Int32(e), SurrealValue::Number(n)) => {
            let a = n.as_int();
            if *e as i64 == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }
        (GeneratedValue::Int64(e), SurrealValue::Number(n)) => {
            let a = n.as_int();
            if *e == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }

        // Float comparison (with tolerance)
        (GeneratedValue::Float64(e), SurrealValue::Number(n)) => {
            let a = n.as_float();
            let tolerance = 1e-10;
            if (e - a).abs() < tolerance {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }

        // String comparison
        (GeneratedValue::String(e), SurrealValue::Strand(a)) => {
            if e == a.as_str() {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: a.to_string(),
                }
            }
        }

        // Bytes comparison
        (GeneratedValue::Bytes(e), SurrealValue::Bytes(a)) => {
            if e == a.as_slice() {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: format!("{e:?}"),
                    actual: format!("{a:?}"),
                }
            }
        }

        // UUID comparison
        (GeneratedValue::Uuid(e), SurrealValue::Uuid(a)) => {
            // Convert SurrealDB UUID to standard uuid for comparison
            let expected_uuid = *e;
            let actual_uuid: uuid::Uuid = (*a).into();
            if expected_uuid == actual_uuid {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: expected_uuid.to_string(),
                    actual: actual_uuid.to_string(),
                }
            }
        }
        // UUID stored as string
        (GeneratedValue::Uuid(e), SurrealValue::Strand(a)) => {
            let expected_uuid = e.to_string();
            if expected_uuid == a.as_str() {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: expected_uuid,
                    actual: a.to_string(),
                }
            }
        }

        // DateTime comparison
        (GeneratedValue::DateTime(e), SurrealValue::Datetime(a)) => {
            // Compare timestamps (allowing for microsecond precision differences)
            let e_ts = e.timestamp_micros();
            let a_ts = a.timestamp_micros();
            if e_ts == a_ts {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_rfc3339(),
                    actual: a.to_string(),
                }
            }
        }

        // Decimal comparison
        (GeneratedValue::Decimal { value: e, .. }, SurrealValue::Number(n)) => {
            // Parse expected decimal string and compare as float with tolerance
            let expected_f64: f64 = match e.parse() {
                Ok(v) => v,
                Err(_) => {
                    return CompareResult::Mismatch {
                        expected: e.clone(),
                        actual: format!("{n}"),
                    };
                }
            };
            let actual_f64 = n.as_float();
            // Use tolerance appropriate for 2 decimal places
            let tolerance = 0.001;
            if (expected_f64 - actual_f64).abs() < tolerance {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: format!("{actual_f64}"),
                }
            }
        }
        // High-precision decimal stored as string
        (GeneratedValue::Decimal { value: e, .. }, SurrealValue::Strand(a)) => {
            if e == a.as_str() {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: a.to_string(),
                }
            }
        }

        // Array comparison
        (GeneratedValue::Array(e), SurrealValue::Array(a)) => {
            if e.len() != a.len() {
                return CompareResult::Mismatch {
                    expected: format!("array of length {}", e.len()),
                    actual: format!("array of length {}", a.len()),
                };
            }
            for (i, (exp_item, act_item)) in e.iter().zip(a.iter()).enumerate() {
                match compare_values_with_options(exp_item, act_item, options) {
                    CompareResult::Match => continue,
                    CompareResult::Mismatch { expected, actual } => {
                        return CompareResult::Mismatch {
                            expected: format!("[{i}]: {expected}"),
                            actual: format!("[{i}]: {actual}"),
                        };
                    }
                    CompareResult::Missing => {
                        return CompareResult::Missing;
                    }
                }
            }
            CompareResult::Match
        }
        // Empty array vs None - only match if accept_missing_as_empty_array is enabled
        // NOTE: This handles the case when the field exists but is None.
        // The case when the field is entirely missing is handled in verifier.rs compare_row.
        (GeneratedValue::Array(e), SurrealValue::None)
            if e.is_empty() && options.accept_missing_as_empty_array =>
        {
            CompareResult::Match
        }

        // Object comparison (for JSON)
        (GeneratedValue::Object(e), SurrealValue::Object(a)) => {
            for (key, exp_val) in e {
                let act_val = a.get(key);
                match act_val {
                    Some(av) => match compare_values_with_options(exp_val, av, options) {
                        CompareResult::Match => continue,
                        CompareResult::Mismatch { expected, actual } => {
                            return CompareResult::Mismatch {
                                expected: format!("{{{key}: {expected}}}"),
                                actual: format!("{{{key}: {actual}}}"),
                            };
                        }
                        CompareResult::Missing => {
                            return CompareResult::Missing;
                        }
                    },
                    None => {
                        return CompareResult::Mismatch {
                            expected: format!("{{{key}: {exp_val:?}}}"),
                            actual: format!("{{missing key: {key}}}"),
                        };
                    }
                }
            }
            CompareResult::Match
        }
        // Object stored as JSON string - only match if accept_object_as_json_string is enabled
        // This handles sources (like Kafka protobuf) that serialize JSON objects as strings
        (GeneratedValue::Object(e), SurrealValue::Strand(a))
            if options.accept_object_as_json_string =>
        {
            // Try to parse the string as JSON and compare
            match serde_json::from_str::<serde_json::Value>(a.as_str()) {
                Ok(parsed) => {
                    // Convert GeneratedValue::Object to serde_json::Value for comparison
                    let expected_json = generated_object_to_json(e);
                    if json_values_equal(&expected_json, &parsed) {
                        CompareResult::Match
                    } else {
                        CompareResult::Mismatch {
                            expected: serde_json::to_string(&expected_json).unwrap_or_default(),
                            actual: a.to_string(),
                        }
                    }
                }
                Err(_) => CompareResult::Mismatch {
                    expected: format!("{e:?}"),
                    actual: a.to_string(),
                },
            }
        }

        // Type mismatch
        (expected, actual) => CompareResult::Mismatch {
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        },
    }
}

/// Convert GeneratedValue::Object to serde_json::Value
fn generated_object_to_json(obj: &HashMap<String, GeneratedValue>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (k, v) in obj {
        map.insert(k.clone(), generated_value_to_json(v));
    }
    serde_json::Value::Object(map)
}

/// Convert GeneratedValue to serde_json::Value
fn generated_value_to_json(val: &GeneratedValue) -> serde_json::Value {
    match val {
        GeneratedValue::Null => serde_json::Value::Null,
        GeneratedValue::Bool(b) => serde_json::Value::Bool(*b),
        GeneratedValue::Int32(i) => serde_json::Value::Number((*i).into()),
        GeneratedValue::Int64(i) => serde_json::Value::Number((*i).into()),
        GeneratedValue::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        GeneratedValue::String(s) => serde_json::Value::String(s.clone()),
        GeneratedValue::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        GeneratedValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        GeneratedValue::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        GeneratedValue::Decimal { value, .. } => serde_json::Value::String(value.clone()),
        GeneratedValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(generated_value_to_json).collect())
        }
        GeneratedValue::Object(obj) => generated_object_to_json(obj),
    }
}

/// Compare two serde_json::Values
fn json_values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            // Compare as f64 with tolerance
            let a_f64 = a.as_f64().unwrap_or(0.0);
            let b_f64 = b.as_f64().unwrap_or(0.0);
            (a_f64 - b_f64).abs() < 1e-10
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a == b,
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            if a.len() != b.len() {
                return false;
            }
            a.iter().zip(b.iter()).all(|(a, b)| json_values_equal(a, b))
        }
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            if a.len() != b.len() {
                return false;
            }
            a.iter()
                .all(|(k, v)| b.get(k).is_some_and(|bv| json_values_equal(v, bv)))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb::sql::{Number, Strand};

    #[test]
    fn test_compare_null() {
        assert_eq!(
            compare_values(&GeneratedValue::Null, &SurrealValue::None),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_bool() {
        assert_eq!(
            compare_values(&GeneratedValue::Bool(true), &SurrealValue::Bool(true)),
            CompareResult::Match
        );
        assert!(matches!(
            compare_values(&GeneratedValue::Bool(true), &SurrealValue::Bool(false)),
            CompareResult::Mismatch { .. }
        ));
    }

    #[test]
    fn test_compare_int32() {
        assert_eq!(
            compare_values(
                &GeneratedValue::Int32(42),
                &SurrealValue::Number(Number::Int(42))
            ),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_int64() {
        assert_eq!(
            compare_values(
                &GeneratedValue::Int64(123456789),
                &SurrealValue::Number(Number::Int(123456789))
            ),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_float() {
        assert_eq!(
            compare_values(
                &GeneratedValue::Float64(1.23456),
                &SurrealValue::Number(Number::Float(1.23456))
            ),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_string() {
        assert_eq!(
            compare_values(
                &GeneratedValue::String("hello".to_string()),
                &SurrealValue::Strand(Strand::from("hello"))
            ),
            CompareResult::Match
        );
        assert!(matches!(
            compare_values(
                &GeneratedValue::String("hello".to_string()),
                &SurrealValue::Strand(Strand::from("world"))
            ),
            CompareResult::Mismatch { .. }
        ));
    }

    #[test]
    fn test_compare_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let surreal_uuid = surrealdb::sql::Uuid::from(u);
        assert_eq!(
            compare_values(&GeneratedValue::Uuid(u), &SurrealValue::Uuid(surreal_uuid)),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_array() {
        use surrealdb::sql::Array;

        let expected = GeneratedValue::Array(vec![
            GeneratedValue::Int32(1),
            GeneratedValue::Int32(2),
            GeneratedValue::Int32(3),
        ]);
        let actual = SurrealValue::Array(Array::from(vec![
            SurrealValue::Number(Number::Int(1)),
            SurrealValue::Number(Number::Int(2)),
            SurrealValue::Number(Number::Int(3)),
        ]));
        assert_eq!(compare_values(&expected, &actual), CompareResult::Match);
    }

    #[test]
    fn test_compare_array_length_mismatch() {
        use surrealdb::sql::Array;

        let expected =
            GeneratedValue::Array(vec![GeneratedValue::Int32(1), GeneratedValue::Int32(2)]);
        let actual = SurrealValue::Array(Array::from(vec![
            SurrealValue::Number(Number::Int(1)),
            SurrealValue::Number(Number::Int(2)),
            SurrealValue::Number(Number::Int(3)),
        ]));
        assert!(matches!(
            compare_values(&expected, &actual),
            CompareResult::Mismatch { .. }
        ));
    }
}
