//! Field comparison logic.

use surrealdb::sql::Value as SurrealValue;
use sync_core::UniversalValue;

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
pub fn compare_values(expected: &UniversalValue, actual: &SurrealValue) -> CompareResult {
    match (expected, actual) {
        // Null comparison
        (UniversalValue::Null, SurrealValue::None) => CompareResult::Match,
        (UniversalValue::Null, SurrealValue::Null) => CompareResult::Match,

        // Boolean comparison
        (UniversalValue::Bool(e), SurrealValue::Bool(a)) => {
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
        (UniversalValue::Int32(e), SurrealValue::Number(n)) => {
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
        (UniversalValue::Int64(e), SurrealValue::Number(n)) => {
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
        (UniversalValue::Float64(e), SurrealValue::Number(n)) => {
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
        (UniversalValue::String(e), SurrealValue::Strand(a)) => {
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
        (UniversalValue::Bytes(e), SurrealValue::Bytes(a)) => {
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
        (UniversalValue::Uuid(e), SurrealValue::Uuid(a)) => {
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
        (UniversalValue::Uuid(e), SurrealValue::Strand(a)) => {
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
        (UniversalValue::DateTime(e), SurrealValue::Datetime(a)) => {
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
        (UniversalValue::Decimal { value: e, .. }, SurrealValue::Number(n)) => {
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
        (UniversalValue::Decimal { value: e, .. }, SurrealValue::Strand(a)) => {
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
        (UniversalValue::Array(e), SurrealValue::Array(a)) => {
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
        (UniversalValue::Object(e), SurrealValue::Object(a)) => {
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
            compare_values(&UniversalValue::Null, &SurrealValue::None),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_bool() {
        assert_eq!(
            compare_values(&UniversalValue::Bool(true), &SurrealValue::Bool(true)),
            CompareResult::Match
        );
        assert!(matches!(
            compare_values(&UniversalValue::Bool(true), &SurrealValue::Bool(false)),
            CompareResult::Mismatch { .. }
        ));
    }

    #[test]
    fn test_compare_int32() {
        assert_eq!(
            compare_values(
                &UniversalValue::Int32(42),
                &SurrealValue::Number(Number::Int(42))
            ),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_int64() {
        assert_eq!(
            compare_values(
                &UniversalValue::Int64(123456789),
                &SurrealValue::Number(Number::Int(123456789))
            ),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_float() {
        assert_eq!(
            compare_values(
                &UniversalValue::Float64(1.23456),
                &SurrealValue::Number(Number::Float(1.23456))
            ),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_string() {
        assert_eq!(
            compare_values(
                &UniversalValue::String("hello".to_string()),
                &SurrealValue::Strand(Strand::from("hello"))
            ),
            CompareResult::Match
        );
        assert!(matches!(
            compare_values(
                &UniversalValue::String("hello".to_string()),
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
            compare_values(&UniversalValue::Uuid(u), &SurrealValue::Uuid(surreal_uuid)),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_array() {
        use surrealdb::sql::Array;

        let expected = UniversalValue::Array(vec![
            UniversalValue::Int32(1),
            UniversalValue::Int32(2),
            UniversalValue::Int32(3),
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
            UniversalValue::Array(vec![UniversalValue::Int32(1), UniversalValue::Int32(2)]);
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
