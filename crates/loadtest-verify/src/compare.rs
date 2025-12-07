//! Field comparison logic.

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

/// Compare a generated value with a SurrealDB value.
pub fn compare_values(expected: &GeneratedValue, actual: &SurrealValue) -> CompareResult {
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
            // Parse both as floats and compare with tolerance
            // to handle trailing zero differences (e.g., "229.30" vs "229.3")
            let expected_f64: Result<f64, _> = e.parse();
            let actual_f64: Result<f64, _> = a.as_str().parse();
            match (expected_f64, actual_f64) {
                (Ok(exp), Ok(act)) => {
                    let tolerance = 0.001;
                    if (exp - act).abs() < tolerance {
                        CompareResult::Match
                    } else {
                        CompareResult::Mismatch {
                            expected: e.clone(),
                            actual: a.to_string(),
                        }
                    }
                }
                _ => {
                    // Fallback to exact string comparison if parsing fails
                    if e == a.as_str() {
                        CompareResult::Match
                    } else {
                        CompareResult::Mismatch {
                            expected: e.clone(),
                            actual: a.to_string(),
                        }
                    }
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
                match compare_values(exp_item, act_item) {
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

        // Object comparison (for JSON)
        (GeneratedValue::Object(e), SurrealValue::Object(a)) => {
            for (key, exp_val) in e {
                let act_val = a.get(key);
                match act_val {
                    Some(av) => match compare_values(exp_val, av) {
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

        // Type mismatch
        (expected, actual) => CompareResult::Mismatch {
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        },
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
