//! JSONL populator for load testing.

use crate::error::JsonlPopulatorError;
use json_types::JsonValue;
use loadtest_generator::DataGenerator;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use sync_core::{UniversalRow, Schema, TableDefinition, TypedValue};
use tracing::{debug, info};

/// Default buffer size for JSONL writing.
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// Metrics from a populate operation.
#[derive(Debug, Clone, Default)]
pub struct PopulateMetrics {
    /// Number of rows written.
    pub rows_written: u64,
    /// Total time taken.
    pub total_duration: Duration,
    /// Time spent generating data.
    pub generation_duration: Duration,
    /// Time spent writing data.
    pub write_duration: Duration,
    /// Output file size in bytes.
    pub file_size_bytes: u64,
}

impl PopulateMetrics {
    /// Calculate rows per second.
    pub fn rows_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.rows_written as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate bytes per second.
    pub fn bytes_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.file_size_bytes as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

/// JSONL populator that generates test data files.
pub struct JsonlPopulator {
    schema: Schema,
    generator: DataGenerator,
}

impl JsonlPopulator {
    /// Create a new JSONL populator.
    ///
    /// # Arguments
    ///
    /// * `schema` - Load test schema defining tables and field generators
    /// * `seed` - Random seed for deterministic generation
    ///
    /// # Example
    ///
    /// ```ignore
    /// let populator = JsonlPopulator::new(schema, 42);
    /// ```
    pub fn new(schema: Schema, seed: u64) -> Self {
        let generator = DataGenerator::new(schema.clone(), seed);
        Self { schema, generator }
    }

    /// Set the starting index for generation (for incremental population).
    pub fn with_start_index(mut self, index: u64) -> Self {
        self.generator = std::mem::replace(
            &mut self.generator,
            DataGenerator::new(self.schema.clone(), 0),
        )
        .with_start_index(index);
        self
    }

    /// Get the current generation index.
    pub fn current_index(&self) -> u64 {
        self.generator.current_index()
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Generate JSONL file with the specified number of rows.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table schema to use for data generation
    /// * `output_path` - Path to the output JSONL file
    /// * `count` - Number of rows to generate
    ///
    /// # Returns
    ///
    /// Metrics about the populate operation.
    pub fn populate<P: AsRef<Path>>(
        &mut self,
        table_name: &str,
        output_path: P,
        count: u64,
    ) -> Result<PopulateMetrics, JsonlPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| JsonlPopulatorError::TableNotFound(table_name.to_string()))?
            .clone();

        let output_path = output_path.as_ref();
        info!(
            "Generating JSONL file '{}' with {} rows for table '{}'",
            output_path.display(),
            count,
            table_name
        );

        // Create writer
        let file = File::create(output_path)?;
        let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, file);

        let mut generation_time = Duration::ZERO;
        let mut write_time = Duration::ZERO;

        // Generate and write rows
        for _ in 0..count {
            // Generate row
            let gen_start = Instant::now();
            let row = self
                .generator
                .next_internal_row(table_name)
                .map_err(|e| JsonlPopulatorError::Generator(e.to_string()))?;
            generation_time += gen_start.elapsed();

            // Convert to JSON and write
            let write_start = Instant::now();
            let json = internal_row_to_json(&row, &table_schema);
            serde_json::to_writer(&mut writer, &json)?;
            writeln!(writer)?;
            write_time += write_start.elapsed();

            metrics.rows_written += 1;

            if metrics.rows_written % 10000 == 0 {
                debug!("Written {} rows", metrics.rows_written);
            }
        }

        // Flush and get file size
        writer.flush()?;
        drop(writer);

        metrics.file_size_bytes = std::fs::metadata(output_path)?.len();
        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.write_duration = write_time;

        info!(
            "JSONL generation complete: {} rows, {} bytes in {:?} ({:.2} rows/sec)",
            metrics.rows_written,
            metrics.file_size_bytes,
            metrics.total_duration,
            metrics.rows_per_second()
        );

        Ok(metrics)
    }

    /// Append rows to an existing JSONL file.
    ///
    /// This method continues from the current generator index, useful for
    /// testing incremental scenarios.
    pub fn populate_append<P: AsRef<Path>>(
        &mut self,
        table_name: &str,
        output_path: P,
        count: u64,
    ) -> Result<PopulateMetrics, JsonlPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| JsonlPopulatorError::TableNotFound(table_name.to_string()))?
            .clone();

        let output_path = output_path.as_ref();
        info!(
            "Appending {} rows to JSONL file '{}' starting at index {}",
            count,
            output_path.display(),
            self.generator.current_index()
        );

        // Open file for appending
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)?;
        let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, file);

        let mut generation_time = Duration::ZERO;
        let mut write_time = Duration::ZERO;

        // Generate and write rows
        for _ in 0..count {
            // Generate row
            let gen_start = Instant::now();
            let row = self
                .generator
                .next_internal_row(table_name)
                .map_err(|e| JsonlPopulatorError::Generator(e.to_string()))?;
            generation_time += gen_start.elapsed();

            // Convert to JSON and write
            let write_start = Instant::now();
            let json = internal_row_to_json(&row, &table_schema);
            serde_json::to_writer(&mut writer, &json)?;
            writeln!(writer)?;
            write_time += write_start.elapsed();

            metrics.rows_written += 1;

            if metrics.rows_written % 10000 == 0 {
                debug!("Appended {} rows", metrics.rows_written);
            }
        }

        // Flush and get file size
        writer.flush()?;
        drop(writer);

        metrics.file_size_bytes = std::fs::metadata(output_path)?.len();
        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.write_duration = write_time;

        info!(
            "JSONL append complete: {} rows, total {} bytes in {:?} ({:.2} rows/sec)",
            metrics.rows_written,
            metrics.file_size_bytes,
            metrics.total_duration,
            metrics.rows_per_second()
        );

        Ok(metrics)
    }
}

/// Convert an UniversalRow to a JSON object.
fn internal_row_to_json(
    row: &UniversalRow,
    table_schema: &TableDefinition,
) -> serde_json::Map<String, serde_json::Value> {
    let mut obj = serde_json::Map::new();

    // Add the ID
    let id_typed = TypedValue::new(table_schema.id.id_type.clone(), row.id.clone());
    let id_json: JsonValue = id_typed.into();
    obj.insert("id".to_string(), id_json.into_inner());

    // Add each field
    for field_schema in &table_schema.fields {
        let field_value = row.get_field(&field_schema.name);
        let typed_value = match field_value {
            Some(value) => TypedValue::new(field_schema.field_type.clone(), value.clone()),
            None => TypedValue::null(field_schema.field_type.clone()),
        };
        let json_value: JsonValue = typed_value.into();
        obj.insert(field_schema.name.clone(), json_value.into_inner());
    }

    obj
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::UniversalValue;
    use tempfile::TempDir;

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
    fn test_metrics() {
        let metrics = PopulateMetrics {
            rows_written: 1000,
            total_duration: Duration::from_secs(10),
            generation_duration: Duration::from_secs(2),
            write_duration: Duration::from_secs(8),
            file_size_bytes: 100000,
        };

        assert_eq!(metrics.rows_per_second(), 100.0);
        assert_eq!(metrics.bytes_per_second(), 10000.0);
    }

    #[test]
    fn test_internal_row_to_json() {
        let schema = test_schema();
        let table_schema = schema.get_table("users").unwrap();

        let row = UniversalRow::new(
            "users".to_string(),
            0,
            UniversalValue::Uuid(
                uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            ),
            [
                (
                    "email".to_string(),
                    UniversalValue::String("test@example.com".to_string()),
                ),
                ("age".to_string(), UniversalValue::Int32(25)),
            ]
            .into_iter()
            .collect(),
        );

        let json = internal_row_to_json(&row, table_schema);

        assert_eq!(json.len(), 3);
        assert_eq!(
            json.get("id").unwrap().as_str().unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(
            json.get("email").unwrap().as_str().unwrap(),
            "test@example.com"
        );
        assert_eq!(json.get("age").unwrap().as_i64().unwrap(), 25);
    }

    #[test]
    fn test_populate_jsonl() {
        let schema = test_schema();
        let mut populator = JsonlPopulator::new(schema, 42);

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.jsonl");

        let metrics = populator.populate("users", &output_path, 10).unwrap();

        assert_eq!(metrics.rows_written, 10);
        assert!(output_path.exists());

        // Verify file contents
        let content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 10);

        // Each line should be valid JSON
        for line in lines {
            let json: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(json.get("id").is_some());
            assert!(json.get("email").is_some());
            assert!(json.get("age").is_some());
        }
    }

    #[test]
    fn test_populate_append() {
        let schema = test_schema();
        let mut populator = JsonlPopulator::new(schema, 42);

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.jsonl");

        // Generate initial rows
        let metrics1 = populator.populate("users", &output_path, 5).unwrap();
        assert_eq!(metrics1.rows_written, 5);

        // Append more rows
        let metrics2 = populator.populate_append("users", &output_path, 5).unwrap();
        assert_eq!(metrics2.rows_written, 5);

        // Verify file contents
        let content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 10);
    }

    #[test]
    fn test_deterministic_generation() {
        let schema = test_schema();
        let temp_dir = TempDir::new().unwrap();

        // Generate with seed 42
        let mut pop1 = JsonlPopulator::new(schema.clone(), 42);
        let path1 = temp_dir.path().join("test1.jsonl");
        pop1.populate("users", &path1, 5).unwrap();

        // Generate again with same seed
        let mut pop2 = JsonlPopulator::new(schema, 42);
        let path2 = temp_dir.path().join("test2.jsonl");
        pop2.populate("users", &path2, 5).unwrap();

        // Files should be identical
        let content1 = std::fs::read_to_string(&path1).unwrap();
        let content2 = std::fs::read_to_string(&path2).unwrap();
        assert_eq!(content1, content2);
    }

    #[test]
    fn test_schema_validation() {
        let schema = test_schema();
        let mut populator = JsonlPopulator::new(schema, 42);

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.jsonl");

        // Try to populate non-existent table
        let result = populator.populate("nonexistent", &output_path, 10);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            JsonlPopulatorError::TableNotFound(_)
        ));
    }
}
