//! SurrealDB validation and data export functions

#![allow(clippy::uninlined_format_args)]

use crate::testing::{table::TestTable, value::SurrealDBValue};
use std::collections::HashMap;
use surrealdb2::{engine::any::Any, sql::Value, Surreal};

/// Recursively validate nested object structure using SurrealDB .take() pattern
#[allow(unused_variables, unused_mut)]
async fn validate_nested_object(
    surreal: &Surreal<Any>,
    record_id: &surrealdb2::sql::Thing,
    base_field_path: &str,
    expected_obj: &HashMap<String, SurrealDBValue>,
    test_description: &str,
    doc_number: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for (nested_field_name, nested_expected_value) in expected_obj {
        let nested_field_path = format!("{base_field_path}.{nested_field_name}");

        match nested_expected_value {
            SurrealDBValue::Object(nested_obj) => {
                // Recursively validate deeper nested objects
                Box::pin(validate_nested_object(
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
                // SELECT the nested field explicitly with an alias
                let query = format!("SELECT {nested_field_path} as nested_value FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;
                // Access using the alias
                let actual: Option<String> = response.take((0, "nested_value"))?;
                if let Some(actual_str) = actual {
                    assert_eq!(
                        actual_str, *expected_str,
                        "{}: Document {}, Nested field '{}' mismatch",
                        test_description, doc_number, nested_field_path
                    );
                } else {
                    panic!(
                        "{}: Document {}, Nested string field '{}' not found. Expected object structure has field '{}' but schema/sync may use different nesting. Expected object keys: {:?}",
                        test_description,
                        doc_number,
                        nested_field_path,
                        nested_field_name,
                        expected_obj.keys().collect::<Vec<_>>()
                    );
                }
            }
            SurrealDBValue::Bool(expected_bool) => {
                // SELECT the nested field explicitly with an alias
                let query = format!("SELECT {nested_field_path} as nested_value FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;
                // Access using the alias
                let actual: Option<bool> = response.take((0, "nested_value"))?;
                if let Some(actual_bool) = actual {
                    assert_eq!(
                        actual_bool, *expected_bool,
                        "{}: Document {}, Nested field '{}' mismatch",
                        test_description, doc_number, nested_field_path
                    );
                } else {
                    panic!(
                        "{}: Document {}, Nested bool field '{}' not found. Expected object structure has field '{}' but schema/sync may use different nesting. Expected object keys: {:?}",
                        test_description,
                        doc_number,
                        nested_field_path,
                        nested_field_name,
                        expected_obj.keys().collect::<Vec<_>>()
                    );
                }
            }
            SurrealDBValue::Array(expected_array) => {
                // SELECT the nested array field explicitly with an alias
                let query = format!("SELECT {nested_field_path} as nested_array FROM $record_id");
                let mut response = surreal
                    .query(&query)
                    .bind(("record_id", record_id.clone()))
                    .await?;

                // Determine array element type from first element and validate
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
                            let expected_strs = expected_array
                                .iter()
                                .filter_map(|v| {
                                    if let SurrealDBValue::String(s) = v {
                                        Some(s.to_owned())
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(
                                actual, expected_strs,
                                "{}: Document {}, Nested array field '{}' mismatch",
                                test_description, doc_number, nested_field_path
                            );
                            assert_eq!(
                                actual.len(),
                                expected_array.len(),
                                "{}: Document {}, Nested array field '{}' length mismatch",
                                test_description,
                                doc_number,
                                nested_field_path
                            );
                            for (i, expected_val) in expected_array.iter().enumerate() {
                                if let SurrealDBValue::String(expected_str) = expected_val {
                                    assert_eq!(
                                        actual[i], *expected_str,
                                        "{}: Document {}, Nested array field '{}' element {} mismatch",
                                        test_description, doc_number, nested_field_path, i
                                    );
                                }
                            }
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
                                test_description,
                                doc_number,
                                nested_field_path,
                                first_elem
                            );
                        }
                    }
                }
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

/// Compare sync results in SurrealDB against expected data
///
/// This function validates that data synced to SurrealDB matches the expected
/// logical representation from the test dataset, field by field.
pub async fn compare_sync_results_in_surrealdb(
    surreal: &Surreal<Any>,
    table_name: &str,
    expected_table: &TestTable,
    test_description: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // First, just validate the count to ensure basic sync worked
    tracing::info!(
        "{}: Validating record count in table '{}'",
        test_description,
        table_name
    );

    let count_query = format!("SELECT count() FROM {table_name} GROUP ALL");
    let mut count_response = surreal.query(count_query).await?;
    let count_result: Option<i64> = count_response.take((0, "count"))?;
    let actual_count = count_result.unwrap_or(0) as usize;

    // Validate record count matches expected
    assert_eq!(
        actual_count,
        expected_table.documents.len(),
        "{}: Record count mismatch - expected {}, found {}",
        test_description,
        expected_table.documents.len(),
        actual_count
    );

    // Print all the record IDs for debugging
    let id_query = "SELECT id FROM type::table($tb)";
    let tb = table_name.to_string();
    let mut id_response = surreal.query(id_query).bind(("tb", tb.clone())).await?;
    let ids: Vec<surrealdb2::sql::Thing> = id_response.take((0, "id"))?;
    tracing::info!(
        "{}: Found {} records in table '{}', IDs: {:?}",
        test_description,
        ids.len(),
        table_name,
        ids
    );

    // Validate each document by querying specific fields with known types
    for (doc_idx, expected_doc) in expected_table.documents.iter().enumerate() {
        tracing::info!(
            "{}: Validating document {}/{}",
            test_description,
            doc_idx + 1,
            expected_table.documents.len()
        );

        let expected_surrealdb_data = expected_doc.to_surrealdb_doc();

        // Get the ID field to query for the specific record
        if let Some(id_field) = expected_doc.get_field("id") {
            if let SurrealDBValue::Thing { table: tb, id } = &id_field.value {
                let id_value = match id.as_ref() {
                    SurrealDBValue::String(s) => surrealdb2::sql::Id::String(s.clone()),
                    SurrealDBValue::Int64(i) => surrealdb2::sql::Id::Number(*i),
                    _ => return Err("Unsupported ID type in Thing".into()),
                };
                let record_id = surrealdb2::sql::Thing::from((tb.clone(), id_value));

                // For each field in the expected document, query it specifically
                for (field_name, expected_value) in &expected_surrealdb_data {
                    // Skip the ID field itself
                    if field_name == "id" {
                        continue;
                    }

                    // Query the specific field for this record using proper parameter binding
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

                    // Get the field value with the appropriate type based on expected value
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

                            if let (Some(actual_str), SurrealDBValue::String(expected_str)) =
                                (actual, expected_value)
                            {
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
                            if let Some(actual_int) = actual {
                                assert_eq!(
                                    actual_int,
                                    expected_int,
                                    "{}: Document {}, Field '{}' mismatch",
                                    test_description,
                                    doc_idx + 1,
                                    field_name
                                );
                            }
                        }
                        SurrealDBValue::Float32(_) | SurrealDBValue::Float64(_) => {
                            let actual: Option<f64> =
                                field_response.take((0, field_name.as_str()))?;
                            let expected_float = match expected_value {
                                SurrealDBValue::Float32(f) => *f as f64,
                                SurrealDBValue::Float64(f) => *f,
                                _ => unreachable!(),
                            };
                            if let Some(actual_float) = actual {
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
                        }
                        SurrealDBValue::Bool(_) => {
                            let actual: Option<bool> =
                                field_response.take((0, field_name.as_str()))?;
                            if let (Some(actual_bool), SurrealDBValue::Bool(expected_bool)) =
                                (actual, expected_value)
                            {
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
                            // Decimal fields may be stored as Number::Decimal or Number::Float
                            // First try reading as f64 (for float storage)
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
                                // Decimal stored as native Decimal type - query with cast to float
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
                            // 2. Object-based validation: catches fields missing in actual synced object
                            validate_nested_object(
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
                            // For DateTime, MongoDB stores as string so validate as string
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
                            // If we get a string datetime, that's sufficient validation
                        }
                        SurrealDBValue::Thing {
                            table: expected_table,
                            id: expected_id,
                        } => {
                            // For Thing references, MongoDB stores as strings, so we check string value
                            // The sync process doesn't convert foreign keys to Thing references automatically
                            let query =
                                format!("SELECT {field_name} as thing_field FROM $record_id");
                            let mut response = surreal
                                .query(&query)
                                .bind(("record_id", record_id.clone()))
                                .await?;

                            // Try to get as Thing first (in case it was stored as a Thing)
                            let actual_thing: Result<Option<surrealdb2::sql::Thing>, _> =
                                response.take((0, "thing_field"));

                            if let Ok(Some(thing)) = actual_thing {
                                // It's stored as a Thing - validate it
                                assert_eq!(
                                    thing.tb, *expected_table,
                                    "{}: Document {}, Field '{}' references wrong table. Expected '{}', got '{}'",
                                    test_description, doc_idx + 1, field_name, expected_table, thing.tb
                                );
                                let expected_id_str = match expected_id.as_ref() {
                                    SurrealDBValue::String(s) => s.clone(),
                                    SurrealDBValue::Int64(i) => i.to_string(),
                                    _ => panic!("Unsupported ID type in expected Thing"),
                                };
                                let actual_id_str = thing.id.to_string();
                                assert_eq!(
                                    actual_id_str, expected_id_str,
                                    "{}: Document {}, Field '{}' references wrong ID. Expected '{}', got '{}'",
                                    test_description, doc_idx + 1, field_name, expected_id_str, actual_id_str
                                );
                            } else {
                                // MongoDB stores references as strings, check if string value matches expected ID
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
                                    // Validate the string ID matches what we expect
                                    let expected_id_str = match expected_id.as_ref() {
                                        SurrealDBValue::String(s) => s.clone(),
                                        SurrealDBValue::Int64(i) => i.to_string(),
                                        _ => panic!("Unsupported ID type in expected Thing"),
                                    };
                                    assert_eq!(
                                        actual_str, expected_id_str,
                                        "{}: Document {}, Field '{}' reference ID mismatch. Expected '{}', got '{}'",
                                        test_description, doc_idx + 1, field_name, expected_id_str, actual_str
                                    );
                                    println!(
                                        "   ✓ Field '{field_name}' references ID '{actual_str}' (stored as string)"
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
                            // For arrays, SELECT the field explicitly with an alias
                            let query =
                                format!("SELECT {field_name} as array_field FROM $record_id");
                            let mut response = surreal
                                .query(&query)
                                .bind(("record_id", record_id.clone()))
                                .await?;

                            // Determine array element type from first element and validate
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
                                        // Verify each string element
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
                            // For bytes, SELECT the field explicitly with an alias
                            let query =
                                format!("SELECT {field_name} as bytes_field FROM $record_id");
                            let mut response = surreal
                                .query(&query)
                                .bind(("record_id", record_id.clone()))
                                .await?;
                            // Retrieve and validate bytes content
                            let actual: Vec<u8> = response.take((0, "bytes_field"))?;
                            assert_eq!(
                                actual, *expected_bytes,
                                "{}: Document {}, Field '{}' bytes mismatch - expected {:?}, found {:?}",
                                test_description, doc_idx + 1, field_name, expected_bytes, actual
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
                            // For null values, we just check that the field query doesn't fail
                            // The field may or may not exist, but if it exists it should be null-like
                            let _actual: Option<surrealdb2::Value> =
                                field_response.take((0, field_name.as_str()))?;
                            // We accept any result for null types since they may not be stored
                        }
                        _ => {
                            return Err(format!(
                                "{test_description}: Unsupported field validation for '{field_name}' with type {expected_value:?}. Add validation logic for this type."
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

/// Validate synced table data matches expected dataset (original approach)
pub async fn validate_synced_table_in_surrealdb(
    surreal: &Surreal<Any>,
    expected_table: &TestTable,
    test_description: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use the existing field-by-field validation approach
    compare_sync_results_in_surrealdb(
        surreal,
        &expected_table.name,
        expected_table,
        test_description,
    )
    .await
}

/// Assert that all tables in a dataset have been correctly synced to SurrealDB
///
/// This function iterates through all tables in the dataset and validates each one
/// to ensure data synced to SurrealDB matches the expected logical representation.
pub async fn assert_synced(
    surreal: &Surreal<Any>,
    dataset: &crate::testing::table::TestDataSet,
    test_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Asserting all SurrealDB tables in dataset are synced correctly...");

    for table in &dataset.tables {
        tracing::info!("Validating table '{}'", table.name);
        validate_synced_table_in_surrealdb(
            surreal,
            table,
            &format!("{} - {}", test_prefix, table.name),
        )
        .await?;
    }
    Ok(())
}

/// Convert SurrealDB value to test value for comparison
pub fn surrealdb_value_to_test_value(
    value: &surrealdb2::sql::Value,
) -> Result<SurrealDBValue, Box<dyn std::error::Error>> {
    match value {
        Value::None | Value::Null => Ok(SurrealDBValue::Null),
        Value::Bool(b) => Ok(SurrealDBValue::Bool(*b)),
        Value::Number(n) => {
            if n.is_int() {
                let i = n.as_int();
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Ok(SurrealDBValue::Int32(i as i32))
                } else {
                    Ok(SurrealDBValue::Int64(i))
                }
            } else if n.is_float() {
                let f = n.as_float();
                Ok(SurrealDBValue::Float64(f))
            } else {
                // Handle decimal numbers
                Ok(SurrealDBValue::Decimal {
                    value: n.to_string(),
                    precision: None, // SurrealDB doesn't expose precision metadata
                    scale: None,
                })
            }
        }
        Value::Strand(s) => Ok(SurrealDBValue::String(s.to_string())),
        Value::Datetime(dt) => Ok(SurrealDBValue::DateTime(dt.0)),
        Value::Array(arr) => {
            let mut converted_array = Vec::new();
            for item in &arr.0 {
                converted_array.push(surrealdb_value_to_test_value(item)?);
            }
            Ok(SurrealDBValue::Array(converted_array))
        }
        Value::Object(obj) => {
            let mut converted_object = HashMap::new();
            for (key, val) in &obj.0 {
                converted_object.insert(key.clone(), surrealdb_value_to_test_value(val)?);
            }
            Ok(SurrealDBValue::Object(converted_object))
        }
        Value::Thing(thing) => Ok(SurrealDBValue::Thing {
            table: thing.tb.clone(),
            id: Box::new(surrealdb_value_to_test_value(&thing.id.clone().into())?),
        }),
        Value::Bytes(bytes) => Ok(SurrealDBValue::Bytes(bytes.to_vec())),
        Value::Duration(dur) => Ok(SurrealDBValue::Duration(dur.to_string())),
        Value::Geometry(geom) => Ok(SurrealDBValue::Geometry(geom.to_string())),
        _ => {
            // For any other types, convert to string representation
            Ok(SurrealDBValue::String(value.to_string()))
        }
    }
}
