//! Streaming verifier implementation.

use crate::compare::{compare_values, CompareResult};
use crate::error::VerifyError;
use crate::report::{FieldMismatch, MismatchInfo, MissingInfo, VerificationReport};
use loadtest_generator::DataGenerator;
use std::time::{Duration, Instant};
use surrealdb::engine::any::Any;
use surrealdb::sql::Value as SurrealValue;
use surrealdb::Surreal;
use sync_core::{UniversalRow, Schema, TableDefinition};
use tracing::{debug, info, warn};

/// Streaming verifier that generates expected data and compares with SurrealDB.
pub struct StreamingVerifier {
    surreal: Surreal<Any>,
    schema: Schema,
    generator: DataGenerator,
    table_name: String,
    /// Fields to skip during verification.
    ///
    /// This is useful for non-deterministic fields like `updated_at` that use
    /// `timestamp_now` generator - the generated value will differ from the
    /// actual synced value since they are produced at different times.
    skip_fields: Vec<String>,
}

/// A record result that we manually extract field-by-field
/// since SurrealDB's sql::Value doesn't deserialize via serde in the expected way.
#[derive(Debug)]
struct RecordResult {
    #[allow(dead_code)]
    id: surrealdb::sql::Thing,
    fields: std::collections::HashMap<String, SurrealValue>,
}

impl StreamingVerifier {
    /// Create a new streaming verifier.
    ///
    /// # Arguments
    ///
    /// * `surreal` - Connected SurrealDB client
    /// * `schema` - Load test schema
    /// * `seed` - Random seed (must match the seed used for data generation)
    /// * `table_name` - Name of the table to verify
    pub fn new(
        surreal: Surreal<Any>,
        schema: Schema,
        seed: u64,
        table_name: &str,
    ) -> Result<Self, VerifyError> {
        // Validate table exists in schema
        if schema.get_table(table_name).is_none() {
            return Err(VerifyError::TableNotFound(table_name.to_string()));
        }

        let generator = DataGenerator::new(schema.clone(), seed);
        Ok(Self {
            surreal,
            schema,
            generator,
            table_name: table_name.to_string(),
            skip_fields: Vec::new(),
        })
    }

    /// Skip specific fields during verification.
    ///
    /// # When to use
    /// Use this for non-deterministic fields like `updated_at` that use `timestamp_now`
    /// generator. Since the generator produces the timestamp at generation time and the
    /// actual synced value is from when the data was inserted, these will always differ.
    ///
    /// # Example
    /// ```ignore
    /// let verifier = StreamingVerifier::new(surreal, schema, seed, "users")?
    ///     .with_skip_fields(vec!["updated_at".to_string()]);
    /// ```
    pub fn with_skip_fields(mut self, fields: Vec<String>) -> Self {
        self.skip_fields = fields;
        self
    }

    /// Set the starting index for verification (for incremental verification).
    pub fn with_start_index(mut self, index: u64) -> Self {
        self.generator = std::mem::replace(
            &mut self.generator,
            DataGenerator::new(self.schema.clone(), 0),
        )
        .with_start_index(index);
        self
    }

    /// Get the current generator index.
    pub fn current_index(&self) -> u64 {
        self.generator.current_index()
    }

    /// Verify a specific number of rows using streaming comparison.
    ///
    /// This method generates expected rows one by one and queries SurrealDB
    /// to verify each row matches. This approach is memory-efficient for
    /// large datasets.
    pub async fn verify_streaming(
        &mut self,
        count: u64,
    ) -> Result<VerificationReport, VerifyError> {
        let start_time = Instant::now();
        let mut report = VerificationReport {
            expected: count,
            ..Default::default()
        };

        let table_schema = self
            .schema
            .get_table(&self.table_name)
            .ok_or_else(|| VerifyError::TableNotFound(self.table_name.clone()))?
            .clone();

        info!(
            "Starting streaming verification of {} rows for table '{}'",
            count, self.table_name
        );

        let mut generation_time = Duration::ZERO;
        let mut query_time = Duration::ZERO;
        let mut compare_time = Duration::ZERO;

        for i in 0..count {
            // Generate expected row
            let gen_start = Instant::now();
            let expected_row = self
                .generator
                .next_internal_row(&self.table_name)
                .map_err(|e| VerifyError::Generator(e.to_string()))?;
            generation_time += gen_start.elapsed();

            // Query SurrealDB for this row
            let query_start = Instant::now();
            let actual = self.query_row(&expected_row, &table_schema).await?;
            query_time += query_start.elapsed();

            // Compare
            let compare_start = Instant::now();
            match actual {
                Some(actual_record) => {
                    report.found += 1;
                    let mismatches = self.compare_row(&expected_row, &actual_record, &table_schema);
                    if mismatches.is_empty() {
                        report.matched += 1;
                    } else {
                        report.mismatched += 1;
                        report.mismatched_rows.push(MismatchInfo {
                            record_id: format!(
                                "{}:{}",
                                self.table_name,
                                format_id(&expected_row.id)
                            ),
                            index: i,
                            field_mismatches: mismatches,
                        });
                    }
                }
                None => {
                    report.missing += 1;
                    report.missing_rows.push(MissingInfo {
                        expected_id: format!("{}:{}", self.table_name, format_id(&expected_row.id)),
                        index: i,
                    });
                }
            }
            compare_time += compare_start.elapsed();

            if (i + 1) % 1000 == 0 {
                debug!(
                    "Verified {}/{} rows ({} matched, {} missing, {} mismatched)",
                    i + 1,
                    count,
                    report.matched,
                    report.missing,
                    report.mismatched
                );
            }
        }

        report.total_duration = start_time.elapsed();
        report.generation_duration = generation_time;
        report.query_duration = query_time;
        report.compare_duration = compare_time;

        info!(
            "Verification complete: {} rows verified in {:?} - {} matched, {} missing, {} mismatched",
            count,
            report.total_duration,
            report.matched,
            report.missing,
            report.mismatched
        );

        Ok(report)
    }

    /// Verify a range of rows (for incremental verification).
    pub async fn verify_range(
        &mut self,
        start_index: u64,
        count: u64,
    ) -> Result<VerificationReport, VerifyError> {
        // Set generator to start at the specified index
        self.generator = std::mem::replace(
            &mut self.generator,
            DataGenerator::new(self.schema.clone(), 0),
        )
        .with_start_index(start_index);

        self.verify_streaming(count).await
    }

    /// Query a single row from SurrealDB by its ID.
    async fn query_row(
        &self,
        expected_row: &UniversalRow,
        table_schema: &TableDefinition,
    ) -> Result<Option<RecordResult>, VerifyError> {
        use surrealdb::sql::{Id, Thing};

        // Construct proper Thing based on ID type to match how sync stores records
        let thing = match &expected_row.id {
            sync_core::UniversalValue::Uuid(u) => Thing::from((
                self.table_name.as_str(),
                Id::Uuid(surrealdb::sql::Uuid::from(*u)),
            )),
            sync_core::UniversalValue::Int64(i) => {
                Thing::from((self.table_name.as_str(), Id::Number(*i)))
            }
            sync_core::UniversalValue::Int32(i) => {
                Thing::from((self.table_name.as_str(), Id::Number(*i as i64)))
            }
            sync_core::UniversalValue::String(s) => {
                Thing::from((self.table_name.as_str(), Id::String(s.clone())))
            }
            other => {
                // Fallback for other types - use string representation
                Thing::from((self.table_name.as_str(), Id::String(format_id(other))))
            }
        };

        // First check if the record exists by fetching just the ID
        let mut response = self
            .surreal
            .query("SELECT id FROM $record_id")
            .bind(("record_id", thing.clone()))
            .await?;

        let ids: Vec<surrealdb::sql::Thing> = response.take((0, "id"))?;
        debug!("Query result for {:?}: found {} ids", thing, ids.len());
        if ids.is_empty() {
            return Ok(None);
        }

        // Record exists, now fetch all fields individually
        let mut fields = std::collections::HashMap::new();
        for field_schema in &table_schema.fields {
            let field_name = &field_schema.name;
            // Query the full record and extract the specific field
            let mut field_response = self
                .surreal
                .query("SELECT * FROM $record_id")
                .bind(("record_id", thing.clone()))
                .await?;

            // Try to extract the field value as the appropriate SurrealDB type
            let value = self
                .extract_field_value(
                    &mut field_response,
                    field_name,
                    &field_schema.field_type,
                    &thing,
                )
                .await
                .map_err(|e| {
                    warn!(
                        "Failed to extract field '{}' (type {:?}) from record {:?}: {}",
                        field_name, field_schema.field_type, thing, e
                    );
                    e
                })?;
            debug!(
                "Extracted field '{}' with type {:?}: {:?}",
                field_name, field_schema.field_type, value
            );
            if let Some(v) = value {
                fields.insert(field_name.clone(), v);
            }
        }

        Ok(Some(RecordResult { id: thing, fields }))
    }

    /// Extract a field value from a query response based on its expected type.
    async fn extract_field_value(
        &self,
        response: &mut surrealdb::Response,
        field_name: &str,
        data_type: &sync_core::UniversalType,
        record_id: &surrealdb::sql::Thing,
    ) -> Result<Option<SurrealValue>, VerifyError> {
        use sync_core::UniversalType;

        match data_type {
            UniversalType::Bool => {
                let val: Option<bool> = response.take((0, field_name))?;
                Ok(val.map(SurrealValue::Bool))
            }
            UniversalType::TinyInt { .. }
            | UniversalType::SmallInt
            | UniversalType::Int
            | UniversalType::BigInt => {
                let val: Option<i64> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Number(surrealdb::sql::Number::Int(v))))
            }
            UniversalType::Float | UniversalType::Double => {
                let val: Option<f64> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Number(surrealdb::sql::Number::Float(v))))
            }
            UniversalType::Decimal { .. } => {
                // Different sources store decimals differently:
                // - CSV sync stores as floats
                // - JSONL/MongoDB store as strings
                // Try float first, then string
                match response.take::<Option<f64>>((0, field_name)) {
                    Ok(Some(v)) => Ok(Some(SurrealValue::Number(surrealdb::sql::Number::Float(v)))),
                    Ok(None) => Ok(None),
                    Err(_) => {
                        // Re-query and try as string
                        let mut retry_response = self
                            .surreal
                            .query("SELECT * FROM $record_id")
                            .bind(("record_id", record_id.clone()))
                            .await?;
                        let val: Option<String> =
                            retry_response.take((0, field_name)).unwrap_or(None);
                        Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
                    }
                }
            }
            UniversalType::Char { .. }
            | UniversalType::VarChar { .. }
            | UniversalType::Text
            | UniversalType::Enum { .. } => {
                let val: Option<String> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
            }
            UniversalType::Uuid => {
                // UUIDs can be stored as native UUID type or as string
                // Try native UUID first, fall back to string
                let result: Result<Option<uuid::Uuid>, _> = response.take((0, field_name));
                match result {
                    Ok(Some(v)) => Ok(Some(SurrealValue::Uuid(surrealdb::sql::Uuid::from(v)))),
                    Ok(None) => Ok(None),
                    Err(_) => {
                        // Try as string (for JSONL which stores UUIDs as strings)
                        let mut response2 = self
                            .surreal
                            .query(format!("SELECT {field_name} FROM $record_id"))
                            .bind(("record_id", record_id.clone()))
                            .await?;
                        let val: Option<String> = response2.take((0, field_name))?;
                        Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
                    }
                }
            }
            UniversalType::DateTime | UniversalType::DateTimeNano | UniversalType::TimestampTz => {
                let val: Option<chrono::DateTime<chrono::Utc>> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Datetime(surrealdb::sql::Datetime::from(v))))
            }
            UniversalType::Date => {
                let val: Option<String> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
            }
            UniversalType::Time => {
                let val: Option<String> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
            }
            UniversalType::Blob | UniversalType::Bytes => {
                let val: Option<Vec<u8>> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Bytes(surrealdb::sql::Bytes::from(v))))
            }
            UniversalType::Json | UniversalType::Jsonb => {
                // JSON values are stored as native Objects in SurrealDB
                // Use serde_json::Value for dynamic JSON extraction
                let val: Option<serde_json::Value> = response.take((0, field_name))?;
                Ok(val.map(|v| json_value_to_surreal(&v)))
            }
            UniversalType::Array { element_type } => {
                // Extract array based on element type
                match element_type.as_ref() {
                    UniversalType::Int | UniversalType::SmallInt | UniversalType::BigInt => {
                        let val: Option<Vec<i64>> = response.take((0, field_name))?;
                        Ok(val.map(|arr| {
                            let items: Vec<SurrealValue> = arr
                                .into_iter()
                                .map(|i| SurrealValue::Number(surrealdb::sql::Number::Int(i)))
                                .collect();
                            SurrealValue::Array(surrealdb::sql::Array::from(items))
                        }))
                    }
                    UniversalType::Text
                    | UniversalType::VarChar { .. }
                    | UniversalType::Char { .. } => {
                        let val: Option<Vec<String>> = response.take((0, field_name))?;
                        Ok(val.map(|arr| {
                            let items: Vec<SurrealValue> = arr
                                .into_iter()
                                .map(|s| SurrealValue::Strand(surrealdb::sql::Strand::from(s)))
                                .collect();
                            SurrealValue::Array(surrealdb::sql::Array::from(items))
                        }))
                    }
                    _ => {
                        // For other element types, skip for now
                        Ok(None)
                    }
                }
            }
            UniversalType::Set { .. } => {
                // Sets - for now just skip complex set handling
                Ok(None)
            }
            UniversalType::Geometry { .. } => {
                // Geometry - skip for now
                Ok(None)
            }
        }
    }

    /// Compare an expected row with an actual SurrealDB record.
    fn compare_row(
        &self,
        expected: &UniversalRow,
        actual: &RecordResult,
        table_schema: &TableDefinition,
    ) -> Vec<FieldMismatch> {
        let mut mismatches = Vec::new();

        for field_schema in &table_schema.fields {
            // Skip fields that are configured to be skipped (e.g., non-deterministic updated_at)
            if self.skip_fields.contains(&field_schema.name) {
                continue;
            }

            let expected_value = expected.get_field(&field_schema.name);
            let actual_value = actual.fields.get(&field_schema.name);

            match (expected_value, actual_value) {
                (Some(exp), Some(act)) => match compare_values(exp, act) {
                    CompareResult::Match => {}
                    CompareResult::Mismatch { expected, actual } => {
                        mismatches.push(FieldMismatch {
                            field: field_schema.name.clone(),
                            expected,
                            actual,
                        });
                    }
                    CompareResult::Missing => {
                        mismatches.push(FieldMismatch {
                            field: field_schema.name.clone(),
                            expected: format!("{exp:?}"),
                            actual: "MISSING".to_string(),
                        });
                    }
                },
                (Some(exp), None) => {
                    // Expected field but not found in SurrealDB
                    mismatches.push(FieldMismatch {
                        field: field_schema.name.clone(),
                        expected: format!("{exp:?}"),
                        actual: "MISSING".to_string(),
                    });
                }
                (None, Some(act)) => {
                    // Extra field in SurrealDB (this is usually okay, but log it)
                    warn!(
                        "Extra field '{}' in SurrealDB record: {:?}",
                        field_schema.name, act
                    );
                }
                (None, None) => {
                    // Both missing - this is fine (null field)
                }
            }
        }

        mismatches
    }
}

/// Format a UniversalValue ID for display.
fn format_id(value: &sync_core::UniversalValue) -> String {
    match value {
        sync_core::UniversalValue::Uuid(u) => u.to_string(),
        sync_core::UniversalValue::Int64(i) => i.to_string(),
        sync_core::UniversalValue::Int32(i) => i.to_string(),
        sync_core::UniversalValue::String(s) => s.clone(),
        _ => format!("{value:?}"),
    }
}

/// Convert a serde_json::Value to surrealdb::sql::Value for comparison.
fn json_value_to_surreal(v: &serde_json::Value) -> SurrealValue {
    match v {
        serde_json::Value::Null => SurrealValue::None,
        serde_json::Value::Bool(b) => SurrealValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SurrealValue::Number(surrealdb::sql::Number::Int(i))
            } else if let Some(f) = n.as_f64() {
                SurrealValue::Number(surrealdb::sql::Number::Float(f))
            } else {
                SurrealValue::Strand(surrealdb::sql::Strand::from(n.to_string()))
            }
        }
        serde_json::Value::String(s) => {
            SurrealValue::Strand(surrealdb::sql::Strand::from(s.clone()))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<SurrealValue> = arr.iter().map(json_value_to_surreal).collect();
            SurrealValue::Array(surrealdb::sql::Array::from(items))
        }
        serde_json::Value::Object(obj) => {
            let mut m = std::collections::BTreeMap::new();
            for (k, val) in obj {
                m.insert(k.clone(), json_value_to_surreal(val));
            }
            SurrealValue::Object(surrealdb::sql::Object::from(m))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
seed: 42
tables:
  - name: users
    id:
      type: uuid
      generator:
        type: uuid_v4
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"
      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_table_not_found() {
        // This test can't actually create a SurrealDB connection without async,
        // so we just verify the schema validation logic
        let schema = test_schema();
        assert!(schema.get_table("nonexistent").is_none());
        assert!(schema.get_table("users").is_some());
    }

    #[test]
    fn test_format_id() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            format_id(&sync_core::UniversalValue::Uuid(uuid)),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(format_id(&sync_core::UniversalValue::Int64(12345)), "12345");
        assert_eq!(
            format_id(&sync_core::UniversalValue::String("test-id".to_string())),
            "test-id"
        );
    }
}
