//! SurrealDB v3 SDK validation and data export functions
//!
//! This module provides validation functions that work with SurrealDB v3 SDK types.
//! The v3 SDK uses different type names:
//! - `RecordId` instead of `Thing`
//! - `RecordIdKey` instead of `Id`
//!
//! These functions mirror those in `surrealdb.rs` but use v3 SDK types.

#![allow(clippy::uninlined_format_args)]

use crate::testing::{
    table::{SourceDatabase, TestTable},
    value::SurrealDBValue,
};
use std::collections::HashMap;
use surrealdb3::engine::any::Any;
use surrealdb3::types::{RecordId, RecordIdKey};
use surrealdb3::Surreal;

/// Convert a SurrealDBValue ID to a v3 RecordIdKey
fn to_record_id_key(value: &SurrealDBValue) -> Result<RecordIdKey, Box<dyn std::error::Error>> {
    match value {
        SurrealDBValue::String(s) => Ok(RecordIdKey::String(s.clone())),
        SurrealDBValue::Int64(i) => Ok(RecordIdKey::Number(*i)),
        SurrealDBValue::Int32(i) => Ok(RecordIdKey::Number(*i as i64)),
        _ => Err(format!("Unsupported ID type for RecordIdKey: {:?}", value).into()),
    }
}

/// Build a SurrealQL field path, using bracket notation for fields with special characters
fn build_field_path(base: &str, field: &str) -> String {
    // Use bracket notation for fields starting with $ or containing special chars
    if field.starts_with('$') || field.contains('-') || field.contains(' ') {
        format!("{base}[\"{field}\"]")
    } else {
        format!("{base}.{field}")
    }
}

/// Recursively validate nested object structure using SurrealDB v3 .take() pattern
#[allow(unused_variables, unused_mut)]
async fn validate_nested_object_v3(
    surreal: &Surreal<Any>,
    record_id: &RecordId,
    base_field_path: &str,
    expected_obj: &HashMap<String, SurrealDBValue>,
    test_description: &str,
    doc_number: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for (nested_field_name, nested_expected_value) in expected_obj {
        let nested_field_path = build_field_path(base_field_path, nested_field_name);

        match nested_expected_value {
            SurrealDBValue::Object(nested_obj) => {
                // Recursively validate deeper nested objects
                Box::pin(validate_nested_object_v3(
                    surreal,
                    record_id,
                    &nested_field_path,
                    nested_obj,
                    test_description,
                    doc_number,
                ))
                .await?;
            }
            SurrealDBValue::String(expected_str) => {
                let query = format!("SELECT {nested_field_path} as nested_value FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;
                let actual: Option<String> = response.take((0, "nested_value"))?;
                if let Some(actual_str) = actual {
                    assert_eq!(
                        actual_str, *expected_str,
                        "{}: Document {}, Nested field '{}' mismatch",
                        test_description, doc_number, nested_field_path
                    );
                } else {
                    panic!(
                        "{}: Document {}, Nested string field '{}' not found",
                        test_description, doc_number, nested_field_path
                    );
                }
            }
            SurrealDBValue::Bool(expected_bool) => {
                let query = format!("SELECT {nested_field_path} as nested_value FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;
                let actual: Option<bool> = response.take((0, "nested_value"))?;
                if let Some(actual_bool) = actual {
                    assert_eq!(
                        actual_bool, *expected_bool,
                        "{}: Document {}, Nested field '{}' mismatch",
                        test_description, doc_number, nested_field_path
                    );
                } else {
                    panic!(
                        "{}: Document {}, Nested bool field '{}' not found",
                        test_description, doc_number, nested_field_path
                    );
                }
            }
            SurrealDBValue::Array(expected_array) => {
                let query = format!("SELECT {nested_field_path} as nested_array FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;

                if let Some(first_elem) = expected_array.first() {
                    match first_elem {
                        SurrealDBValue::String(_) => {
                            let actual = response
                                .take::<Option<Vec<String>>>((0, "nested_array"))?
                                .unwrap_or_else(|| {
                                    panic!(
                                        "{}: Document {}, Nested array field '{}' is None",
                                        test_description, doc_number, nested_field_path
                                    )
                                });
                            let expected_strs: Vec<String> = expected_array
                                .iter()
                                .filter_map(|v| {
                                    if let SurrealDBValue::String(s) = v {
                                        Some(s.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            assert_eq!(
                                actual, expected_strs,
                                "{}: Document {}, Nested array field '{}' mismatch",
                                test_description, doc_number, nested_field_path
                            );
                        }
                        SurrealDBValue::Int64(_) => {
                            let actual: Vec<i64> = response.take((0, "nested_array"))?;
                            assert_eq!(
                                actual.len(),
                                expected_array.len(),
                                "{}: Document {}, Nested array field '{}' length mismatch",
                                test_description,
                                doc_number,
                                nested_field_path
                            );
                            for (i, expected_val) in expected_array.iter().enumerate() {
                                if let SurrealDBValue::Int64(expected_int) = expected_val {
                                    assert_eq!(
                                        actual[i], *expected_int,
                                        "{}: Document {}, Nested array field '{}' element {} mismatch",
                                        test_description, doc_number, nested_field_path, i
                                    );
                                }
                            }
                        }
                        _ => {
                            panic!(
                                "{}: Document {}, Nested array field '{}' has unsupported element type: {:?}",
                                test_description, doc_number, nested_field_path, first_elem
                            );
                        }
                    }
                }
            }
            SurrealDBValue::Int32(expected_int) => {
                let query = format!("SELECT {nested_field_path} as nested_value FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;
                let actual: Option<i64> = response.take((0, "nested_value"))?;
                let actual_int = actual.unwrap_or_else(|| {
                    panic!(
                        "{}: Document {}, Nested int field '{}' not found",
                        test_description, doc_number, nested_field_path
                    )
                });
                assert_eq!(
                    actual_int, *expected_int as i64,
                    "{}: Document {}, Nested field '{}' mismatch",
                    test_description, doc_number, nested_field_path
                );
            }
            SurrealDBValue::Int64(expected_int) => {
                let query = format!("SELECT {nested_field_path} as nested_value FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;
                let actual: Option<i64> = response.take((0, "nested_value"))?;
                let actual_int = actual.unwrap_or_else(|| {
                    panic!(
                        "{}: Document {}, Nested int field '{}' not found",
                        test_description, doc_number, nested_field_path
                    )
                });
                assert_eq!(
                    actual_int, *expected_int,
                    "{}: Document {}, Nested field '{}' mismatch",
                    test_description, doc_number, nested_field_path
                );
            }
            _ => {
                return Err(format!(
                    "{test_description}: Unsupported nested field validation for '{nested_field_path}' with type {nested_expected_value:?}"
                ).into());
            }
        }
    }
    Ok(())
}

/// Compare sync results in SurrealDB v3 against expected data
///
/// This function validates that data synced to SurrealDB matches the expected
/// logical representation from the test dataset, field by field.
/// Uses v3 SDK types (RecordId instead of Thing).
pub async fn compare_sync_results_in_surrealdb_v3(
    surreal: &Surreal<Any>,
    table_name: &str,
    expected_table: &TestTable,
    test_description: &str,
    source: SourceDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    // First, validate the count
    tracing::info!(
        "{}: Validating record count in table '{}'",
        test_description,
        table_name
    );

    let count_query = format!("SELECT count() FROM {table_name} GROUP ALL");
    let mut count_response = surreal.query(count_query).await?;
    let count_result: Option<i64> = count_response.take((0, "count"))?;
    let actual_count = count_result.unwrap_or(0) as usize;

    assert_eq!(
        actual_count,
        expected_table.documents.len(),
        "{}: Record count mismatch - expected {}, found {}",
        test_description,
        expected_table.documents.len(),
        actual_count
    );

    // Print all record IDs for debugging
    let id_query = "SELECT id FROM type::table($tb)";
    let tb = table_name.to_string();
    let mut id_response = surreal.query(id_query).bind(("tb", tb.clone())).await?;
    let ids: Vec<RecordId> = id_response.take((0, "id"))?;
    tracing::info!(
        "{}: Found {} records in table '{}', IDs: {:?}",
        test_description,
        ids.len(),
        table_name,
        ids
    );

    // Validate each document
    for (doc_idx, expected_doc) in expected_table.documents.iter().enumerate() {
        tracing::info!(
            "{}: Validating document {}/{}",
            test_description,
            doc_idx + 1,
            expected_table.documents.len()
        );

        let expected_surrealdb_data = expected_doc.to_surrealdb_doc_for_source(source);

        // Get the ID field to query for the specific record
        if let Some(id_field) = expected_doc.get_field("id") {
            if let SurrealDBValue::Thing { table: tb, id } = &id_field.value {
                let id_key = to_record_id_key(id.as_ref())?;
                let record_id = RecordId::new(tb.as_str(), id_key);

                // For each field in the expected document, query it specifically
                for (field_name, expected_value) in &expected_surrealdb_data {
                    if field_name == "id" {
                        continue;
                    }

                    let field_query = format!("SELECT {field_name} FROM $record_id");

                    tracing::info!(
                        "Executing field query for document {}: {}",
                        doc_idx + 1,
                        field_query,
                    );

                    let mut field_response = surreal
                        .query(field_query)
                        .bind(("record_id", record_id.clone()))
                        .await?;

                    tracing::info!(
                        "Expected value for field '{}': {:?}",
                        field_name,
                        expected_value
                    );

                    // Validate based on expected type
                    match expected_value {
                        SurrealDBValue::String(_) => {
                            let actual: Option<String> =
                                field_response.take((0, field_name.as_str())).map_err(|e| {
                                    format!(
                                        "{}: Failed to take string field '{}' for document {}: {}",
                                        test_description,
                                        field_name,
                                        doc_idx + 1,
                                        e
                                    )
                                })?;

                            let actual_str = actual.unwrap_or_else(|| {
                                panic!(
                                    "{}: Document {}, Field '{}' expected String but field is missing/null",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                )
                            });
                            if let SurrealDBValue::String(expected_str) = expected_value {
                                assert_eq!(
                                    actual_str,
                                    *expected_str,
                                    "{}: Document {}, Field '{}' mismatch",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                            }
                        }
                        SurrealDBValue::Int32(_) | SurrealDBValue::Int64(_) => {
                            let actual: Option<i64> =
                                field_response.take((0, field_name.as_str()))?;
                            let expected_int = match expected_value {
                                SurrealDBValue::Int32(i) => *i as i64,
                                SurrealDBValue::Int64(i) => *i,
                                _ => unreachable!(),
                            };
                            let actual_int = actual.unwrap_or_else(|| {
                                panic!(
                                    "{}: Document {}, Field '{}' expected Int but field is missing/null",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                )
                            });
                            assert_eq!(
                                actual_int,
                                expected_int,
                                "{}: Document {}, Field '{}' mismatch",
                                test_description,
                                doc_idx + 1,
                                field_name
                            );
                        }
                        SurrealDBValue::Float32(_) | SurrealDBValue::Float64(_) => {
                            let actual: Option<f64> =
                                field_response.take((0, field_name.as_str()))?;
                            let expected_float = match expected_value {
                                SurrealDBValue::Float32(f) => *f as f64,
                                SurrealDBValue::Float64(f) => *f,
                                _ => unreachable!(),
                            };
                            let actual_float = actual.unwrap_or_else(|| {
                                panic!(
                                    "{}: Document {}, Field '{}' expected Float but field is missing/null",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                )
                            });
                            assert!(
                                (actual_float - expected_float).abs() < 0.0001,
                                "{}: Document {}, Field '{}' mismatch - expected {}, found {}",
                                test_description,
                                doc_idx + 1,
                                field_name,
                                expected_float,
                                actual_float
                            );
                        }
                        SurrealDBValue::Bool(_) => {
                            let actual: Option<bool> =
                                field_response.take((0, field_name.as_str()))?;
                            let actual_bool = actual.unwrap_or_else(|| {
                                panic!(
                                    "{}: Document {}, Field '{}' expected Bool but field is missing/null",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                )
                            });
                            if let SurrealDBValue::Bool(expected_bool) = expected_value {
                                assert_eq!(
                                    actual_bool,
                                    *expected_bool,
                                    "{}: Document {}, Field '{}' mismatch",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                            }
                        }
                        SurrealDBValue::Decimal { value, .. } => {
                            let actual_float_result: Result<Option<f64>, _> =
                                field_response.take((0, field_name.as_str()));

                            if let Ok(Some(actual_float)) = actual_float_result {
                                let expected_float: f64 = value
                                    .parse()
                                    .map_err(|_| format!("Invalid decimal value: {value}"))?;
                                assert!(
                                    (actual_float - expected_float).abs() < 0.0001,
                                    "{}: Document {}, Field '{}' decimal mismatch - expected {}, found {}",
                                    test_description,
                                    doc_idx + 1,
                                    field_name,
                                    expected_float,
                                    actual_float
                                );
                            } else {
                                let cast_query = format!(
                                    "SELECT <float> {field_name} as decimal_value FROM $record_id"
                                );
                                let mut cast_response = surreal
                                    .query(&cast_query)
                                    .bind(("record_id", record_id.clone()))
                                    .await?;
                                let actual_float: Option<f64> =
                                    cast_response.take((0, "decimal_value"))?;

                                if let Some(actual_float) = actual_float {
                                    let expected_float: f64 = value
                                        .parse()
                                        .map_err(|_| format!("Invalid decimal value: {value}"))?;
                                    assert!(
                                        (actual_float - expected_float).abs() < 0.0001,
                                        "{}: Document {}, Field '{}' decimal mismatch - expected {}, found {}",
                                        test_description,
                                        doc_idx + 1,
                                        field_name,
                                        expected_float,
                                        actual_float
                                    );
                                } else {
                                    panic!(
                                        "{}: Document {}, Field '{}' decimal value not found",
                                        test_description,
                                        doc_idx + 1,
                                        field_name
                                    );
                                }
                            }
                        }
                        SurrealDBValue::Object(expected_obj) => {
                            validate_nested_object_v3(
                                surreal,
                                &record_id,
                                field_name,
                                expected_obj,
                                test_description,
                                doc_idx + 1,
                            )
                            .await?;
                        }
                        SurrealDBValue::DateTime(_) => {
                            let actual: Option<chrono::DateTime<chrono::Utc>> =
                                field_response.take((0, field_name.as_str())).map_err(|e| {
                                    format!(
                                        "{}: Failed to take datetime field '{}' for document {}: {}",
                                        test_description,
                                        field_name,
                                        doc_idx + 1,
                                        e
                                    )
                                })?;
                            if actual.is_none() {
                                panic!(
                                    "{}: Document {}, Field '{}' datetime not found",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                            }
                        }
                        SurrealDBValue::Thing {
                            table: expected_table_ref,
                            id: expected_id,
                        } => {
                            let query =
                                format!("SELECT {field_name} as thing_field FROM $record_id");
                            let mut response = surreal
                                .query(&query)
                                .bind(("record_id", record_id.clone()))
                                .await?;

                            // Try to get as RecordId first (v3 equivalent of Thing)
                            let actual_record_id: Result<Option<RecordId>, _> =
                                response.take((0, "thing_field"));

                            if let Ok(Some(rid)) = actual_record_id {
                                assert_eq!(
                                    rid.table.to_string(),
                                    *expected_table_ref,
                                    "{}: Document {}, Field '{}' references wrong table",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                                let expected_id_str = match expected_id.as_ref() {
                                    SurrealDBValue::String(s) => s.clone(),
                                    SurrealDBValue::Int64(i) => i.to_string(),
                                    _ => panic!("Unsupported ID type in expected Thing"),
                                };
                                // V3 RecordIdKey doesn't implement Display, so match on variants
                                let actual_id_str = match &rid.key {
                                    RecordIdKey::String(s) => s.clone(),
                                    RecordIdKey::Number(n) => n.to_string(),
                                    _ => format!("{:?}", rid.key),
                                };
                                assert_eq!(
                                    actual_id_str,
                                    expected_id_str,
                                    "{}: Document {}, Field '{}' references wrong ID",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                            } else {
                                // Check as string
                                let query =
                                    format!("SELECT {field_name} as string_field FROM $record_id");
                                let mut response = surreal
                                    .query(&query)
                                    .bind(("record_id", record_id.clone()))
                                    .await?;
                                let actual_string: Option<String> =
                                    response.take((0, "string_field")).map_err(|e| {
                                        format!(
                                            "{}: Failed to take string reference id field '{}' for document {}: {}",
                                            test_description,
                                            field_name,
                                            doc_idx + 1,
                                            e
                                        )
                                    })?;

                                if let Some(actual_str) = actual_string {
                                    let expected_id_str = match expected_id.as_ref() {
                                        SurrealDBValue::String(s) => s.clone(),
                                        SurrealDBValue::Int64(i) => i.to_string(),
                                        _ => panic!("Unsupported ID type in expected Thing"),
                                    };
                                    assert_eq!(
                                        actual_str,
                                        expected_id_str,
                                        "{}: Document {}, Field '{}' reference ID mismatch",
                                        test_description,
                                        doc_idx + 1,
                                        field_name
                                    );
                                } else {
                                    panic!(
                                        "{}: Document {}, Field '{}' reference not found",
                                        test_description,
                                        doc_idx + 1,
                                        field_name
                                    );
                                }
                            }
                        }
                        SurrealDBValue::Array(expected_array) => {
                            let query =
                                format!("SELECT {field_name} as array_field FROM $record_id");
                            let mut response = surreal
                                .query(&query)
                                .bind(("record_id", record_id.clone()))
                                .await?;

                            if let Some(first_elem) = expected_array.first() {
                                match first_elem {
                                    SurrealDBValue::String(_) => {
                                        let actual = response
                                            .take::<Option<Vec<String>>>((0, "array_field"))?
                                            .unwrap_or_else(|| {
                                                panic!(
                                                    "{}: Document {}, Field '{}' array is None",
                                                    test_description,
                                                    doc_idx + 1,
                                                    field_name
                                                )
                                            });
                                        assert_eq!(
                                            actual.len(),
                                            expected_array.len(),
                                            "{}: Document {}, Field '{}' array length mismatch",
                                            test_description,
                                            doc_idx + 1,
                                            field_name
                                        );
                                        for (i, expected_val) in expected_array.iter().enumerate() {
                                            if let SurrealDBValue::String(expected_str) =
                                                expected_val
                                            {
                                                assert_eq!(
                                                    actual[i], *expected_str,
                                                    "{}: Document {}, Field '{}' array element {} mismatch",
                                                    test_description, doc_idx + 1, field_name, i
                                                );
                                            }
                                        }
                                    }
                                    SurrealDBValue::Int64(_) => {
                                        let actual: Vec<i64> = response.take((0, "array_field"))?;
                                        assert_eq!(
                                            actual.len(),
                                            expected_array.len(),
                                            "{}: Document {}, Field '{}' array length mismatch",
                                            test_description,
                                            doc_idx + 1,
                                            field_name
                                        );
                                        for (i, expected_val) in expected_array.iter().enumerate() {
                                            if let SurrealDBValue::Int64(expected_int) =
                                                expected_val
                                            {
                                                assert_eq!(
                                                    actual[i], *expected_int,
                                                    "{}: Document {}, Field '{}' array element {} mismatch",
                                                    test_description, doc_idx + 1, field_name, i
                                                );
                                            }
                                        }
                                    }
                                    SurrealDBValue::Float64(_) => {
                                        let actual: Vec<f64> = response.take((0, "array_field"))?;
                                        assert_eq!(
                                            actual.len(),
                                            expected_array.len(),
                                            "{}: Document {}, Field '{}' array length mismatch",
                                            test_description,
                                            doc_idx + 1,
                                            field_name
                                        );
                                        for (i, expected_val) in expected_array.iter().enumerate() {
                                            if let SurrealDBValue::Float64(expected_float) =
                                                expected_val
                                            {
                                                assert!(
                                                    (actual[i] - expected_float).abs() < 0.0001,
                                                    "{}: Document {}, Field '{}' array element {} mismatch",
                                                    test_description, doc_idx + 1, field_name, i
                                                );
                                            }
                                        }
                                    }
                                    SurrealDBValue::Bool(_) => {
                                        let actual: Vec<bool> =
                                            response.take((0, "array_field"))?;
                                        assert_eq!(
                                            actual.len(),
                                            expected_array.len(),
                                            "{}: Document {}, Field '{}' array length mismatch",
                                            test_description,
                                            doc_idx + 1,
                                            field_name
                                        );
                                        for (i, expected_val) in expected_array.iter().enumerate() {
                                            if let SurrealDBValue::Bool(expected_bool) =
                                                expected_val
                                            {
                                                assert_eq!(
                                                    actual[i], *expected_bool,
                                                    "{}: Document {}, Field '{}' array element {} mismatch",
                                                    test_description, doc_idx + 1, field_name, i
                                                );
                                            }
                                        }
                                    }
                                    _ => {
                                        panic!(
                                            "{}: Document {}, Field '{}' has unsupported array element type: {:?}",
                                            test_description,
                                            doc_idx + 1,
                                            field_name,
                                            first_elem
                                        );
                                    }
                                }
                            }
                        }
                        SurrealDBValue::Bytes(expected_bytes) => {
                            let query =
                                format!("SELECT {field_name} as bytes_field FROM $record_id");
                            let mut response = surreal
                                .query(&query)
                                .bind(("record_id", record_id.clone()))
                                .await?;
                            let actual: Vec<u8> = response.take((0, "bytes_field"))?;
                            assert_eq!(
                                actual,
                                *expected_bytes,
                                "{}: Document {}, Field '{}' bytes mismatch",
                                test_description,
                                doc_idx + 1,
                                field_name
                            );
                        }
                        SurrealDBValue::Uuid(expected_uuid) => {
                            let actual: Option<String> =
                                field_response.take((0, field_name.as_str())).map_err(|e| {
                                    format!(
                                        "{}: Failed to take UUID field '{}' for document {}: {}",
                                        test_description,
                                        field_name,
                                        doc_idx + 1,
                                        e
                                    )
                                })?;
                            if let Some(actual_str) = actual {
                                assert_eq!(
                                    actual_str,
                                    *expected_uuid,
                                    "{}: Document {}, Field '{}' UUID mismatch",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                            }
                        }
                        SurrealDBValue::Null | SurrealDBValue::None => {
                            // For null values, just check that the field query doesn't fail
                            let _actual: Option<serde_json::Value> =
                                field_response.take((0, field_name.as_str()))?;
                        }
                        _ => {
                            return Err(format!(
                                "{test_description}: Unsupported field validation for '{field_name}' with type {expected_value:?}"
                            ).into());
                        }
                    }
                }
            }
        }
    }

    println!("{test_description}: Validated {actual_count} records with field-by-field comparison");

    Ok(())
}

/// Validate synced table data matches expected dataset using v3 SDK
pub async fn validate_synced_table_in_surrealdb_v3(
    surreal: &Surreal<Any>,
    expected_table: &TestTable,
    test_description: &str,
    source: SourceDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    compare_sync_results_in_surrealdb_v3(
        surreal,
        &expected_table.name,
        expected_table,
        test_description,
        source,
    )
    .await
}

/// Assert that all tables in a dataset have been correctly synced to SurrealDB v3
pub async fn assert_synced_v3(
    surreal: &Surreal<Any>,
    dataset: &crate::testing::table::TestDataSet,
    test_prefix: &str,
    source: SourceDatabase,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Asserting all SurrealDB v3 tables in dataset are synced correctly...");

    for table in &dataset.tables {
        tracing::info!("Validating table '{}'", table.name);
        validate_synced_table_in_surrealdb_v3(
            surreal,
            table,
            &format!("{} - {}", test_prefix, table.name),
            source,
        )
        .await?;
    }
    Ok(())
}

/// Connect to SurrealDB v3 with the given configuration
///
/// This uses the v3 SDK (surrealdb3::) which requires the "flatbuffers" WebSocket subprotocol.
/// V3 SDK can only connect to v3 servers.
pub async fn connect_surrealdb_v3(
    config: &crate::testing::test_helpers::TestConfig,
) -> Result<Surreal<Any>, Box<dyn std::error::Error>> {
    let surreal = surrealdb3::engine::any::connect(&config.surreal_endpoint).await?;
    surreal
        .signin(surrealdb3::opt::auth::Root {
            username: "root".to_string(),
            password: "root".to_string(),
        })
        .await?;
    surreal
        .use_ns(&config.surreal_namespace)
        .use_db(&config.surreal_database)
        .await?;
    Ok(surreal)
}

/// Clean up test data from SurrealDB tables (v3 version)
///
/// This function deletes all records from the specified tables.
/// It gracefully handles the case where a table doesn't exist.
pub async fn cleanup_surrealdb_test_data_v3(
    surreal: &Surreal<Any>,
    tables: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for table in tables {
        // Use DELETE with RETURN NONE to avoid needing to deserialize response
        // Also catch errors for non-existent tables
        let query = format!("DELETE FROM {table} RETURN NONE");
        match surreal.query(&query).await {
            Ok(_) => {
                tracing::debug!("Cleaned up table '{}'", table);
            }
            Err(e) => {
                // Ignore "table does not exist" errors during cleanup
                let err_str = e.to_string();
                if err_str.contains("does not exist") {
                    tracing::debug!(
                        "Table '{}' does not exist, skipping cleanup: {}",
                        table,
                        err_str
                    );
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}
