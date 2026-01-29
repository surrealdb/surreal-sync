//! Field comparison logic for SurrealDB v3.

use surrealdb3::types::Value as SurrealValue;
use sync_core::{GeometryData, UniversalValue};

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

/// Helper to get integer value from Number (v3 doesn't have as_int())
fn number_as_int(n: &surrealdb3::types::Number) -> i64 {
    use surrealdb3::types::Number;
    match n {
        Number::Int(i) => *i,
        Number::Float(f) => *f as i64,
        Number::Decimal(d) => d.to_string().parse().unwrap_or(0),
    }
}

/// Helper to get float value from Number (v3 doesn't have as_float())
fn number_as_float(n: &surrealdb3::types::Number) -> f64 {
    use surrealdb3::types::Number;
    match n {
        Number::Int(i) => *i as f64,
        Number::Float(f) => *f,
        Number::Decimal(d) => d.to_string().parse().unwrap_or(0.0),
    }
}

/// Compare a generated value with a SurrealDB v3 value.
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
        (UniversalValue::Int8 { value: e, .. }, SurrealValue::Number(n)) => {
            let a = number_as_int(n);
            if *e as i64 == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }
        (UniversalValue::Int16(e), SurrealValue::Number(n)) => {
            let a = number_as_int(n);
            if *e as i64 == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }
        (UniversalValue::Int32(e), SurrealValue::Number(n)) => {
            let a = number_as_int(n);
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
            let a = number_as_int(n);
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
        (UniversalValue::Float32(e), SurrealValue::Number(n)) => {
            let a = number_as_float(n);
            let tolerance = 1e-6_f64;
            if ((*e as f64) - a).abs() < tolerance {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_string(),
                    actual: a.to_string(),
                }
            }
        }
        (UniversalValue::Float64(e), SurrealValue::Number(n)) => {
            let a = number_as_float(n);
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

        // String comparison (v3 uses Value::String instead of Value::Strand)
        (UniversalValue::Text(e), SurrealValue::String(a)) => {
            if e == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: a.clone(),
                }
            }
        }
        // Char comparison (strict 1:1)
        (UniversalValue::Char { value: e, .. }, SurrealValue::String(a)) => {
            if e == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: a.clone(),
                }
            }
        }
        // VarChar comparison (strict 1:1)
        (UniversalValue::VarChar { value: e, .. }, SurrealValue::String(a)) => {
            if e == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: a.clone(),
                }
            }
        }

        // Bytes comparison (v3 uses as_ref() instead of as_slice())
        (UniversalValue::Bytes(e), SurrealValue::Bytes(a)) => {
            if e == a.as_ref() {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: format!("{e:?}"),
                    actual: format!("{a:?}"),
                }
            }
        }
        // Blob comparison (strict 1:1)
        (UniversalValue::Blob(e), SurrealValue::Bytes(a)) => {
            if e == a.as_ref() {
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
        (UniversalValue::Uuid(e), SurrealValue::String(a)) => {
            let expected_uuid = e.to_string();
            if expected_uuid == *a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: expected_uuid,
                    actual: a.clone(),
                }
            }
        }

        // DateTime comparison
        (UniversalValue::LocalDateTime(e), SurrealValue::Datetime(a)) => {
            // Compare timestamps (allowing for microsecond precision differences)
            let e_ts = e.timestamp_micros();
            let a_ts = a.timestamp_micros();
            if e_ts == a_ts {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_rfc3339(),
                    actual: format!("{a:?}"),
                }
            }
        }
        // Date comparison (strict 1:1) - stored as string in SurrealDB
        (UniversalValue::Date(e), SurrealValue::String(a)) => {
            let expected = e.format("%Y-%m-%d").to_string();
            if expected == *a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected,
                    actual: a.clone(),
                }
            }
        }
        // Time comparison (strict 1:1) - stored as string in SurrealDB
        (UniversalValue::Time(e), SurrealValue::String(a)) => {
            let expected = e.format("%H:%M:%S").to_string();
            if expected == *a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected,
                    actual: a.clone(),
                }
            }
        }
        // DateTimeNano comparison (strict 1:1)
        (UniversalValue::LocalDateTimeNano(e), SurrealValue::Datetime(a)) => {
            let e_ts = e.timestamp_micros();
            let a_ts = a.timestamp_micros();
            if e_ts == a_ts {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_rfc3339(),
                    actual: format!("{a:?}"),
                }
            }
        }
        // TimestampTz comparison (strict 1:1)
        (UniversalValue::ZonedDateTime(e), SurrealValue::Datetime(a)) => {
            let e_ts = e.timestamp_micros();
            let a_ts = a.timestamp_micros();
            if e_ts == a_ts {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.to_rfc3339(),
                    actual: format!("{a:?}"),
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
                        actual: format!("{n:?}"),
                    };
                }
            };
            let actual_f64 = number_as_float(n);
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
        // Decimal stored as string is NOT valid - must be stored as Number (Decimal or Float)
        // TODO: could consider adding a verifier option to allow this, so that some sources without the schema files
        // that store decimals as strings can be verified using this verifier.
        (UniversalValue::Decimal { value: e, .. }, SurrealValue::String(a)) => {
            CompareResult::Mismatch {
                expected: format!("{e} (as Decimal or Float, not String)"),
                actual: format!("\"{a}\" (String)"),
            }
        }

        // Array comparison
        (UniversalValue::Array { elements: e, .. }, SurrealValue::Array(a)) => {
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

        // JSON/JSONB comparison - always stored as SurrealDB Object
        (UniversalValue::Json(e), SurrealValue::Object(a)) => compare_json_to_surreal_object(e, a),
        (UniversalValue::Jsonb(e), SurrealValue::Object(a)) => compare_json_to_surreal_object(e, a),

        // Enum comparison (strict 1:1) - stored as string in SurrealDB
        (UniversalValue::Enum { value: e, .. }, SurrealValue::String(a)) => {
            if e == a {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: e.clone(),
                    actual: a.clone(),
                }
            }
        }

        // Set comparison (strict 1:1) - stored as array in SurrealDB
        (UniversalValue::Set { elements: e, .. }, SurrealValue::Array(a)) => {
            if e.len() != a.len() {
                return CompareResult::Mismatch {
                    expected: format!("set of {} elements", e.len()),
                    actual: format!("array of {} elements", a.len()),
                };
            }
            // Compare as unordered set (simplified: just check all elements present)
            for exp_elem in e.iter() {
                let found = a.iter().any(|act_elem| {
                    if let SurrealValue::String(s) = act_elem {
                        s == exp_elem
                    } else {
                        false
                    }
                });
                if !found {
                    return CompareResult::Mismatch {
                        expected: format!("set containing {exp_elem}"),
                        actual: format!("{a:?}"),
                    };
                }
            }
            CompareResult::Match
        }

        // Geometry comparison - always stored as JSON object in SurrealDB
        (UniversalValue::Geometry { data, .. }, SurrealValue::Object(a)) => {
            let GeometryData(expected_json) = data;
            // Convert SurrealDB Object to JSON and compare
            let actual_json = surreal_object_to_json(a);
            if json_values_equal(expected_json, &actual_json) {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: serde_json::to_string(expected_json)
                        .unwrap_or_else(|_| format!("{expected_json:?}")),
                    actual: serde_json::to_string(&actual_json)
                        .unwrap_or_else(|_| format!("{actual_json:?}")),
                }
            }
        }

        // Duration comparison - compare as seconds
        (UniversalValue::Duration(e), SurrealValue::Duration(a)) => {
            // Convert both to std::time::Duration for comparison
            let expected_secs = e.as_secs();
            let expected_nanos = e.subsec_nanos();
            let actual_std: std::time::Duration = (*a).into();
            let actual_secs = actual_std.as_secs();
            let actual_nanos = actual_std.subsec_nanos();
            if expected_secs == actual_secs && expected_nanos == actual_nanos {
                CompareResult::Match
            } else {
                CompareResult::Mismatch {
                    expected: format!("{expected_secs}s {expected_nanos}ns"),
                    actual: format!("{actual_secs}s {actual_nanos}ns"),
                }
            }
        }

        // Type mismatch
        (expected, actual) => CompareResult::Mismatch {
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        },
    }
}

/// Compare a serde_json::Value to a SurrealDB v3 Object.
fn compare_json_to_surreal_object(
    expected: &serde_json::Value,
    actual: &surrealdb3::types::Object,
) -> CompareResult {
    // Convert SurrealDB Object to serde_json::Value for comparison
    let actual_json = surreal_object_to_json(actual);

    if json_values_equal(expected, &actual_json) {
        CompareResult::Match
    } else {
        CompareResult::Mismatch {
            expected: serde_json::to_string(expected).unwrap_or_else(|_| format!("{expected:?}")),
            actual: serde_json::to_string(&actual_json)
                .unwrap_or_else(|_| format!("{actual_json:?}")),
        }
    }
}

/// Convert a SurrealDB v3 Object to serde_json::Value.
fn surreal_object_to_json(obj: &surrealdb3::types::Object) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, value) in obj.iter() {
        map.insert(key.clone(), surreal_value_to_json(value));
    }
    serde_json::Value::Object(map)
}

/// Convert a SurrealDB v3 Value to serde_json::Value.
fn surreal_value_to_json(value: &SurrealValue) -> serde_json::Value {
    match value {
        SurrealValue::None | SurrealValue::Null => serde_json::Value::Null,
        SurrealValue::Bool(b) => serde_json::json!(*b),
        SurrealValue::Number(n) => {
            // Try as integer first, then fall back to float
            let int_val = number_as_int(n);
            if number_as_float(n) == int_val as f64 {
                serde_json::json!(int_val)
            } else {
                serde_json::json!(number_as_float(n))
            }
        }
        SurrealValue::String(s) => serde_json::json!(s),
        SurrealValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(surreal_value_to_json).collect())
        }
        SurrealValue::Object(obj) => surreal_object_to_json(obj),
        SurrealValue::Uuid(u) => {
            let uuid: uuid::Uuid = (*u).into();
            serde_json::json!(uuid.to_string())
        }
        SurrealValue::Datetime(dt) => serde_json::json!(format!("{dt:?}")),
        SurrealValue::Bytes(b) => {
            use base64::Engine;
            serde_json::json!(base64::engine::general_purpose::STANDARD.encode(b.as_ref()))
        }
        // Fallback for other types - use debug format
        other => serde_json::json!(format!("{other:?}")),
    }
}

/// Compare two serde_json::Value instances for equality.
/// Uses semantic comparison (handles numeric type differences, etc.)
fn json_values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            // Compare as f64 to handle integer vs float differences
            let a_f64 = a.as_f64().unwrap_or(f64::NAN);
            let b_f64 = b.as_f64().unwrap_or(f64::NAN);
            if a_f64.is_nan() && b_f64.is_nan() {
                true
            } else {
                (a_f64 - b_f64).abs() < 1e-10
            }
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
                .all(|(key, val)| b.get(key).is_some_and(|bval| json_values_equal(val, bval)))
        }
        // Different types
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb3::types::Number;

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
        // V3 uses Value::String instead of Value::Strand
        assert_eq!(
            compare_values(
                &UniversalValue::Text("hello".to_string()),
                &SurrealValue::String("hello".to_string())
            ),
            CompareResult::Match
        );
        assert!(matches!(
            compare_values(
                &UniversalValue::Text("hello".to_string()),
                &SurrealValue::String("world".to_string())
            ),
            CompareResult::Mismatch { .. }
        ));
    }

    #[test]
    fn test_compare_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let surreal_uuid = surrealdb3::types::Uuid::from(u);
        assert_eq!(
            compare_values(&UniversalValue::Uuid(u), &SurrealValue::Uuid(surreal_uuid)),
            CompareResult::Match
        );
    }

    #[test]
    fn test_compare_array() {
        use surrealdb3::types::Array;

        let expected = UniversalValue::Array {
            elements: vec![
                UniversalValue::Int32(1),
                UniversalValue::Int32(2),
                UniversalValue::Int32(3),
            ],
            element_type: Box::new(sync_core::UniversalType::Int32),
        };
        let actual = SurrealValue::Array(Array::from(vec![
            SurrealValue::Number(Number::Int(1)),
            SurrealValue::Number(Number::Int(2)),
            SurrealValue::Number(Number::Int(3)),
        ]));
        assert_eq!(compare_values(&expected, &actual), CompareResult::Match);
    }

    #[test]
    fn test_compare_array_length_mismatch() {
        use surrealdb3::types::Array;

        let expected = UniversalValue::Array {
            elements: vec![UniversalValue::Int32(1), UniversalValue::Int32(2)],
            element_type: Box::new(sync_core::UniversalType::Int32),
        };
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

    #[test]
    fn test_compare_json_object_match() {
        use surrealdb3::types::Object;

        let expected_json = serde_json::json!({
            "name": "test",
            "value": 42,
            "active": true
        });
        let expected = UniversalValue::Json(Box::new(expected_json));

        let mut actual_obj = Object::default();
        actual_obj.insert("name".to_string(), SurrealValue::String("test".to_string()));
        actual_obj.insert("value".to_string(), SurrealValue::Number(Number::Int(42)));
        actual_obj.insert("active".to_string(), SurrealValue::Bool(true));
        let actual = SurrealValue::Object(actual_obj);

        assert_eq!(compare_values(&expected, &actual), CompareResult::Match);
    }

    #[test]
    fn test_compare_json_object_mismatch() {
        use surrealdb3::types::Object;

        let expected_json = serde_json::json!({
            "name": "test",
            "value": 42
        });
        let expected = UniversalValue::Json(Box::new(expected_json));

        let mut actual_obj = Object::default();
        actual_obj.insert(
            "name".to_string(),
            SurrealValue::String("different".to_string()),
        );
        actual_obj.insert("value".to_string(), SurrealValue::Number(Number::Int(42)));
        let actual = SurrealValue::Object(actual_obj);

        assert!(matches!(
            compare_values(&expected, &actual),
            CompareResult::Mismatch { .. }
        ));
    }

    #[test]
    fn test_compare_json_nested() {
        use surrealdb3::types::{Array, Object};

        let expected_json = serde_json::json!({
            "items": [1, 2, 3],
            "nested": {
                "key": "value"
            }
        });
        let expected = UniversalValue::Json(Box::new(expected_json));

        let mut nested_obj = Object::default();
        nested_obj.insert("key".to_string(), SurrealValue::String("value".to_string()));

        let mut actual_obj = Object::default();
        actual_obj.insert(
            "items".to_string(),
            SurrealValue::Array(Array::from(vec![
                SurrealValue::Number(Number::Int(1)),
                SurrealValue::Number(Number::Int(2)),
                SurrealValue::Number(Number::Int(3)),
            ])),
        );
        actual_obj.insert("nested".to_string(), SurrealValue::Object(nested_obj));
        let actual = SurrealValue::Object(actual_obj);

        assert_eq!(compare_values(&expected, &actual), CompareResult::Match);
    }

    #[test]
    fn test_compare_geometry_geojson() {
        use surrealdb3::types::Object;
        use sync_core::GeometryType;

        let geojson = serde_json::json!({
            "type": "Point",
            "coordinates": [-73.97, 40.77]
        });
        let expected = UniversalValue::Geometry {
            data: GeometryData(geojson),
            geometry_type: GeometryType::Point,
        };

        let mut coords = surrealdb3::types::Array::default();
        coords.push(SurrealValue::Number(Number::Float(-73.97)));
        coords.push(SurrealValue::Number(Number::Float(40.77)));

        let mut actual_obj = Object::default();
        actual_obj.insert(
            "type".to_string(),
            SurrealValue::String("Point".to_string()),
        );
        actual_obj.insert("coordinates".to_string(), SurrealValue::Array(coords));
        let actual = SurrealValue::Object(actual_obj);

        assert_eq!(compare_values(&expected, &actual), CompareResult::Match);
    }

    #[test]
    fn test_json_values_equal_numbers() {
        // Integer vs float that are numerically equal
        assert!(json_values_equal(
            &serde_json::json!(42),
            &serde_json::json!(42.0)
        ));
        // Different numbers
        assert!(!json_values_equal(
            &serde_json::json!(42),
            &serde_json::json!(43)
        ));
    }

    #[test]
    fn test_json_values_equal_objects() {
        let a = serde_json::json!({"a": 1, "b": 2});
        let b = serde_json::json!({"b": 2, "a": 1}); // Same but different order
        assert!(json_values_equal(&a, &b));

        let c = serde_json::json!({"a": 1, "b": 3}); // Different value
        assert!(!json_values_equal(&a, &c));
    }
}
