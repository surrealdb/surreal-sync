//! Streaming verifier implementation.

use crate::compare::{compare_values_with_options, CompareOptions, CompareResult};
use crate::error::VerifyError;
use crate::report::{FieldMismatch, MismatchInfo, MissingInfo, VerificationReport};
use loadtest_generator::DataGenerator;
use std::time::{Duration, Instant};
use surrealdb::engine::any::Any;
use surrealdb::sql::Value as SurrealValue;
use surrealdb::Surreal;
use sync_core::{InternalRow, SyncSchema, TableSchema};
use tracing::{debug, info, warn};

/// Streaming verifier that generates expected data and compares with SurrealDB.
pub struct StreamingVerifier {
    surreal: Surreal<Any>,
    schema: SyncSchema,
    generator: DataGenerator,
    table_name: String,
    /// When true, all IDs are converted to strings for SurrealDB lookups.
    /// This is needed for Neo4j sources which always store IDs as strings.
    force_string_ids: bool,
    /// Options for configuring comparison behavior.
    compare_options: CompareOptions,
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
        schema: SyncSchema,
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
            force_string_ids: false,
            compare_options: CompareOptions::default(),
        })
    }

    /// Enable forcing all IDs to be treated as strings.
    /// This is needed for Neo4j sources which always store IDs as strings in SurrealDB,
    /// and for MySQL incremental sync which stores row_id as VARCHAR in the audit table.
    ///
    /// # Future Work: Remove this workaround
    ///
    /// This method is a temporary workaround that should be removed once the following
    /// enhancements are implemented:
    ///
    /// 1. **Neo4j source schema support**: Enhance the Neo4j source to read and use schema
    ///    information, so it can convert Neo4j string IDs (which are required to be strings
    ///    in Neo4j) to the SurrealDB type that users specify in their schema.
    ///
    /// 2. **MySQL incremental source enhancements**:
    ///    - Support encoding primary keys in the tracking table while preserving value types
    ///      (currently stores as VARCHAR, losing type information)
    ///    - Support composite primary keys (currently only supports single-column PKs named 'id')
    ///    - Read/leverage schema information to convert primary key values from the tracking
    ///      table to the correct SurrealDB data types
    ///
    /// Once these enhancements are complete, sources will properly convert IDs to schema-
    /// defined types, and this `force_string_ids` workaround will no longer be needed.
    pub fn with_force_string_ids(mut self, force: bool) -> Self {
        self.force_string_ids = force;
        self
    }

    /// Accept JSON objects stored as strings during comparison.
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
    pub fn with_accept_object_as_json_string(mut self, accept: bool) -> Self {
        self.compare_options.accept_object_as_json_string = accept;
        self
    }

    /// Accept empty arrays as equivalent to missing fields during comparison.
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
    pub fn with_accept_missing_as_empty_array(mut self, accept: bool) -> Self {
        self.compare_options.accept_missing_as_empty_array = accept;
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
        expected_row: &InternalRow,
        table_schema: &TableSchema,
    ) -> Result<Option<RecordResult>, VerifyError> {
        use surrealdb::sql::{Id, Thing};

        // Construct proper Thing based on ID type to match how sync stores records
        // If force_string_ids is enabled, always use string representation for IDs
        let thing = if self.force_string_ids {
            // Force all IDs to strings (needed for Neo4j which stores all IDs as strings)
            let id_str = match &expected_row.id {
                sync_core::GeneratedValue::Int32(i) => i.to_string(),
                sync_core::GeneratedValue::Int64(i) => i.to_string(),
                sync_core::GeneratedValue::Uuid(u) => u.to_string(),
                sync_core::GeneratedValue::String(s) => s.clone(),
                other => format_id(other),
            };
            Thing::from((self.table_name.as_str(), Id::String(id_str)))
        } else {
            match &expected_row.id {
                sync_core::GeneratedValue::Uuid(u) => Thing::from((
                    self.table_name.as_str(),
                    Id::Uuid(surrealdb::sql::Uuid::from(*u)),
                )),
                sync_core::GeneratedValue::Int64(i) => {
                    Thing::from((self.table_name.as_str(), Id::Number(*i)))
                }
                sync_core::GeneratedValue::Int32(i) => {
                    Thing::from((self.table_name.as_str(), Id::Number(*i as i64)))
                }
                sync_core::GeneratedValue::String(s) => {
                    Thing::from((self.table_name.as_str(), Id::String(s.clone())))
                }
                other => {
                    // Fallback for other types - use string representation
                    Thing::from((self.table_name.as_str(), Id::String(format_id(other))))
                }
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
        data_type: &sync_core::SyncDataType,
        record_id: &surrealdb::sql::Thing,
    ) -> Result<Option<SurrealValue>, VerifyError> {
        use sync_core::SyncDataType;

        match data_type {
            SyncDataType::Bool => {
                let val: Option<bool> = response.take((0, field_name))?;
                Ok(val.map(SurrealValue::Bool))
            }
            SyncDataType::TinyInt { .. }
            | SyncDataType::SmallInt
            | SyncDataType::Int
            | SyncDataType::BigInt => {
                let val: Option<i64> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Number(surrealdb::sql::Number::Int(v))))
            }
            SyncDataType::Float | SyncDataType::Double => {
                let val: Option<f64> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Number(surrealdb::sql::Number::Float(v))))
            }
            SyncDataType::Decimal { .. } => {
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
            SyncDataType::Char { .. }
            | SyncDataType::VarChar { .. }
            | SyncDataType::Text
            | SyncDataType::Enum { .. } => {
                let val: Option<String> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
            }
            SyncDataType::Uuid => {
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
            SyncDataType::DateTime | SyncDataType::DateTimeNano | SyncDataType::TimestampTz => {
                let val: Option<chrono::DateTime<chrono::Utc>> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Datetime(surrealdb::sql::Datetime::from(v))))
            }
            SyncDataType::Date => {
                let val: Option<String> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
            }
            SyncDataType::Time => {
                let val: Option<String> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Strand(surrealdb::sql::Strand::from(v))))
            }
            SyncDataType::Blob | SyncDataType::Bytes => {
                let val: Option<Vec<u8>> = response.take((0, field_name))?;
                Ok(val.map(|v| SurrealValue::Bytes(surrealdb::sql::Bytes::from(v))))
            }
            SyncDataType::Json | SyncDataType::Jsonb => {
                // JSON values are stored as native Objects in SurrealDB
                // Use serde_json::Value for dynamic JSON extraction
                let val: Option<serde_json::Value> = response.take((0, field_name))?;
                Ok(val.map(|v| json_value_to_surreal(&v)))
            }
            SyncDataType::Array { element_type } => {
                // Extract array based on element type
                match element_type.as_ref() {
                    SyncDataType::Int | SyncDataType::SmallInt | SyncDataType::BigInt => {
                        let val: Option<Vec<i64>> = response.take((0, field_name))?;
                        Ok(val.map(|arr| {
                            let items: Vec<SurrealValue> = arr
                                .into_iter()
                                .map(|i| SurrealValue::Number(surrealdb::sql::Number::Int(i)))
                                .collect();
                            SurrealValue::Array(surrealdb::sql::Array::from(items))
                        }))
                    }
                    SyncDataType::Text
                    | SyncDataType::VarChar { .. }
                    | SyncDataType::Char { .. } => {
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
            SyncDataType::Set { .. } => {
                // Sets - for now just skip complex set handling
                Ok(None)
            }
            SyncDataType::Geometry { .. } => {
                // Geometry - skip for now
                Ok(None)
            }
        }
    }

    /// Compare an expected row with an actual SurrealDB record.
    fn compare_row(
        &self,
        expected: &InternalRow,
        actual: &RecordResult,
        table_schema: &TableSchema,
    ) -> Vec<FieldMismatch> {
        let mut mismatches = Vec::new();

        for field_schema in &table_schema.fields {
            let expected_value = expected.get_field(&field_schema.name);
            let actual_value = actual.fields.get(&field_schema.name);

            match (expected_value, actual_value) {
                (Some(exp), Some(act)) => {
                    match compare_values_with_options(exp, act, &self.compare_options) {
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
                    }
                }
                (Some(exp), None) => {
                    // Expected field but not found in SurrealDB
                    // When accept_missing_as_empty_array is enabled, treat missing field as
                    // equivalent to empty array (protobuf doesn't write empty repeated fields)
                    let is_empty_array =
                        matches!(exp, sync_core::GeneratedValue::Array(arr) if arr.is_empty());
                    if is_empty_array && self.compare_options.accept_missing_as_empty_array {
                        // Empty array expected and field missing - this is a match
                    } else {
                        mismatches.push(FieldMismatch {
                            field: field_schema.name.clone(),
                            expected: format!("{exp:?}"),
                            actual: "MISSING".to_string(),
                        });
                    }
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

/// Format a GeneratedValue ID for display.
fn format_id(value: &sync_core::GeneratedValue) -> String {
    match value {
        sync_core::GeneratedValue::Uuid(u) => u.to_string(),
        sync_core::GeneratedValue::Int64(i) => i.to_string(),
        sync_core::GeneratedValue::Int32(i) => i.to_string(),
        sync_core::GeneratedValue::String(s) => s.clone(),
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

    fn test_schema() -> SyncSchema {
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
        SyncSchema::from_yaml(yaml).unwrap()
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
            format_id(&sync_core::GeneratedValue::Uuid(uuid)),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(format_id(&sync_core::GeneratedValue::Int64(12345)), "12345");
        assert_eq!(
            format_id(&sync_core::GeneratedValue::String("test-id".to_string())),
            "test-id"
        );
    }
}
