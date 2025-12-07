//! CSV populator for load testing.

use crate::error::CSVPopulatorError;
use csv::Writer;
use csv_types::CsvValue;
use loadtest_generator::DataGenerator;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::time::{Duration, Instant};
use sync_core::{Schema, TableDefinition, TypedValue, UniversalRow};
use tracing::{debug, info};

/// Default buffer size for CSV writing.
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

/// CSV populator that generates test data files.
pub struct CSVPopulator {
    schema: Schema,
    generator: DataGenerator,
    include_header: bool,
}

impl CSVPopulator {
    /// Create a new CSV populator.
    ///
    /// # Arguments
    ///
    /// * `schema` - Load test schema defining tables and field generators
    /// * `seed` - Random seed for deterministic generation
    ///
    /// # Example
    ///
    /// ```ignore
    /// let populator = CSVPopulator::new(schema, 42);
    /// ```
    pub fn new(schema: Schema, seed: u64) -> Self {
        let generator = DataGenerator::new(schema.clone(), seed);
        Self {
            schema,
            generator,
            include_header: true,
        }
    }

    /// Set whether to include a header row in the CSV output.
    pub fn with_header(mut self, include_header: bool) -> Self {
        self.include_header = include_header;
        self
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

    /// Generate CSV file with the specified number of rows.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table schema to use for data generation
    /// * `output_path` - Path to the output CSV file
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
    ) -> Result<PopulateMetrics, CSVPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| CSVPopulatorError::TableNotFound(table_name.to_string()))?
            .clone();

        let output_path = output_path.as_ref();
        info!(
            "Generating CSV file '{}' with {} rows for table '{}'",
            output_path.display(),
            count,
            table_name
        );

        // Create writer
        let file = File::create(output_path)?;
        let buf_writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, file);
        let mut writer = Writer::from_writer(buf_writer);

        let mut generation_time = Duration::ZERO;
        let mut write_time = Duration::ZERO;

        // Write header if requested
        if self.include_header {
            let write_start = Instant::now();
            let headers = get_column_names(&table_schema);
            writer.write_record(&headers)?;
            write_time += write_start.elapsed();
        }

        // Generate and write rows
        for _ in 0..count {
            // Generate row
            let gen_start = Instant::now();
            let row = self
                .generator
                .next_internal_row(table_name)
                .map_err(|e| CSVPopulatorError::Generator(e.to_string()))?;
            generation_time += gen_start.elapsed();

            // Convert to CSV record
            let write_start = Instant::now();
            let record = internal_row_to_csv_record(&row, &table_schema);
            writer.write_record(&record)?;
            write_time += write_start.elapsed();

            metrics.rows_written += 1;

            if metrics.rows_written % 10000 == 0 {
                debug!("Written {} rows", metrics.rows_written);
            }
        }

        // Flush and get file size
        writer.flush()?;
        let inner = writer
            .into_inner()
            .map_err(|e| CSVPopulatorError::Io(std::io::Error::other(e.to_string())))?;
        drop(inner);

        metrics.file_size_bytes = std::fs::metadata(output_path)?.len();
        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.write_duration = write_time;

        info!(
            "CSV generation complete: {} rows, {} bytes in {:?} ({:.2} rows/sec)",
            metrics.rows_written,
            metrics.file_size_bytes,
            metrics.total_duration,
            metrics.rows_per_second()
        );

        Ok(metrics)
    }

    /// Append rows to an existing CSV file.
    ///
    /// This method continues from the current generator index, useful for
    /// testing incremental scenarios.
    pub fn populate_append<P: AsRef<Path>>(
        &mut self,
        table_name: &str,
        output_path: P,
        count: u64,
    ) -> Result<PopulateMetrics, CSVPopulatorError> {
        let start_time = Instant::now();
        let mut metrics = PopulateMetrics::default();

        let table_schema = self
            .schema
            .get_table(table_name)
            .ok_or_else(|| CSVPopulatorError::TableNotFound(table_name.to_string()))?
            .clone();

        let output_path = output_path.as_ref();
        info!(
            "Appending {} rows to CSV file '{}' starting at index {}",
            count,
            output_path.display(),
            self.generator.current_index()
        );

        // Open file for appending
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)?;
        let buf_writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, file);
        let mut writer = Writer::from_writer(buf_writer);

        let mut generation_time = Duration::ZERO;
        let mut write_time = Duration::ZERO;

        // Generate and write rows
        for _ in 0..count {
            // Generate row
            let gen_start = Instant::now();
            let row = self
                .generator
                .next_internal_row(table_name)
                .map_err(|e| CSVPopulatorError::Generator(e.to_string()))?;
            generation_time += gen_start.elapsed();

            // Convert to CSV record
            let write_start = Instant::now();
            let record = internal_row_to_csv_record(&row, &table_schema);
            writer.write_record(&record)?;
            write_time += write_start.elapsed();

            metrics.rows_written += 1;
        }

        // Flush and get file size
        writer.flush()?;
        drop(writer);

        metrics.file_size_bytes = std::fs::metadata(output_path)?.len();
        metrics.total_duration = start_time.elapsed();
        metrics.generation_duration = generation_time;
        metrics.write_duration = write_time;

        info!(
            "CSV append complete: {} rows in {:?}",
            metrics.rows_written, metrics.total_duration
        );

        Ok(metrics)
    }
}

/// Get column names for a table schema (id + fields).
fn get_column_names(table_schema: &TableDefinition) -> Vec<String> {
    let mut columns = vec!["id".to_string()];
    columns.extend(table_schema.field_names().iter().map(|s| s.to_string()));
    columns
}

/// Convert an UniversalRow to a CSV record (vector of strings).
fn internal_row_to_csv_record(row: &UniversalRow, table_schema: &TableDefinition) -> Vec<String> {
    let mut record = Vec::new();

    // Add the ID
    let id_typed = TypedValue::try_with_type(table_schema.id.id_type.clone(), row.id.clone())
        .expect("generator produced invalid type-value combination for id");
    let id_csv: CsvValue = id_typed.into();
    record.push(id_csv.into_inner());

    // Add each field
    for field_schema in &table_schema.fields {
        let field_value = row.get_field(&field_schema.name);
        let typed_value = match field_value {
            Some(value) => {
                TypedValue::try_with_type(field_schema.field_type.clone(), value.clone())
                    .expect("generator produced invalid type-value combination for field")
            }
            None => TypedValue::null(field_schema.field_type.clone()),
        };
        let csv_value: CsvValue = typed_value.into();
        record.push(csv_value.into_inner());
    }

    record
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
    fn test_get_column_names() {
        let schema = test_schema();
        let table_schema = schema.get_table("users").unwrap();
        let columns = get_column_names(table_schema);

        assert_eq!(columns, vec!["id", "email", "age"]);
    }

    #[test]
    fn test_internal_row_to_csv_record() {
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
                    // Use VarChar to match schema (strict 1:1 type-value matching)
                    UniversalValue::VarChar {
                        value: "test@example.com".to_string(),
                        length: 255,
                    },
                ),
                ("age".to_string(), UniversalValue::Int(25)),
            ]
            .into_iter()
            .collect(),
        );

        let record = internal_row_to_csv_record(&row, table_schema);

        assert_eq!(record.len(), 3);
        assert_eq!(record[0], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(record[1], "test@example.com");
        assert_eq!(record[2], "25");
    }

    #[test]
    fn test_populate_csv() {
        let schema = test_schema();
        let mut populator = CSVPopulator::new(schema, 42);

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.csv");

        let metrics = populator.populate("users", &output_path, 10).unwrap();

        assert_eq!(metrics.rows_written, 10);
        assert!(output_path.exists());

        // Verify file contents
        let content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 11); // 1 header + 10 data rows
        assert_eq!(lines[0], "id,email,age");
    }

    #[test]
    fn test_populate_without_header() {
        let schema = test_schema();
        let mut populator = CSVPopulator::new(schema, 42).with_header(false);

        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("test.csv");

        let metrics = populator.populate("users", &output_path, 10).unwrap();

        assert_eq!(metrics.rows_written, 10);

        let content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 10); // No header, just 10 data rows
    }

    #[test]
    fn test_deterministic_generation() {
        let schema = test_schema();
        let temp_dir = TempDir::new().unwrap();

        // Generate with seed 42
        let mut pop1 = CSVPopulator::new(schema.clone(), 42);
        let path1 = temp_dir.path().join("test1.csv");
        pop1.populate("users", &path1, 5).unwrap();

        // Generate with same seed
        let mut pop2 = CSVPopulator::new(schema, 42);
        let path2 = temp_dir.path().join("test2.csv");
        pop2.populate("users", &path2, 5).unwrap();

        // Files should be identical
        let content1 = std::fs::read_to_string(&path1).unwrap();
        let content2 = std::fs::read_to_string(&path2).unwrap();
        assert_eq!(content1, content2);
    }
}
