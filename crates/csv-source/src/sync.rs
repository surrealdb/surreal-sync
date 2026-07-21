//! CSV synchronization implementation
//!
//! This module handles streaming CSV files from various sources and importing them into SurrealDB tables.

use anyhow::{Context, Result};
use csv_types::{csv_string_to_typed_value, csv_string_to_typed_value_inferred};
use std::collections::HashMap;
use std::path::PathBuf;
use surreal_sink::SurrealSink;
use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};
use sync_core::{
    GeneratorTableDefinition, Schema, TypedValue, UniversalRow, UniversalType, UniversalValue,
};
use sync_transform::{
    run_source_runtime, ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver,
    SourceRuntimeOpts,
};
use tracing::{debug, info, warn};

/// Configuration for CSV import
#[derive(Clone)]
pub struct Config {
    /// Unified file sources (files, directories, S3 URIs, HTTP URLs)
    /// Directories (paths ending with /) will be expanded to list all files
    pub sources: Vec<FileSource>,

    /// List of CSV file paths to import (legacy, use `sources` instead)
    pub files: Vec<PathBuf>,

    /// List of S3 URIs to import (legacy, use `sources` instead)
    pub s3_uris: Vec<String>,

    /// List of HTTP/HTTPS URLs to import (legacy, use `sources` instead)
    pub http_uris: Vec<String>,

    /// Target SurrealDB table name
    pub table: String,

    /// Number of rows to process in each batch
    pub batch_size: usize,

    /// Whether the CSV has headers (default: true)
    pub has_headers: bool,

    /// CSV delimiter character (default: ',')
    pub delimiter: u8,

    /// Optional field to use as record ID
    pub id_field: Option<String>,

    /// Optional column names when has_headers is false
    /// If provided, must match the number of columns in the CSV
    pub column_names: Option<Vec<String>>,

    /// Optional path to emit metrics during execution
    pub emit_metrics: Option<PathBuf>,

    /// Whether to perform a dry run without writing data
    pub dry_run: bool,

    /// Optional schema for type-aware conversion
    /// When provided, CSV string values will be parsed according to the schema's
    /// type definitions (e.g., JSON strings will be parsed to objects/arrays)
    pub schema: Option<Schema>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sources: vec![],
            files: vec![],
            s3_uris: vec![],
            http_uris: vec![],
            table: String::new(),
            batch_size: 1000,
            has_headers: true,
            delimiter: b',',
            id_field: None,
            column_names: None,
            emit_metrics: None,
            dry_run: false,
            schema: None,
        }
    }
}

/// Parse a CSV string value according to the schema type.
///
/// When a schema is provided, this function parses values based on their declared type.
/// Uses the unified json-types crate for type conversion.
fn parse_value_with_schema(value: &str, schema_type: Option<&UniversalType>) -> TypedValue {
    if let Some(data_type) = schema_type {
        // Use json-types for schema-aware conversion
        match csv_string_to_typed_value(value, data_type) {
            Ok(tv) => tv,
            Err(e) => {
                warn!("Failed to parse '{}' as {:?}: {}", value, data_type, e);
                // Fall back to inferred type
                csv_string_to_typed_value_inferred(value)
            }
        }
    } else {
        // No schema type - use inferred parsing
        csv_string_to_typed_value_inferred(value)
    }
}

/// No-op sink for CSV `--dry-run` (still exercises the apply window).
struct DryRunSink;

#[async_trait::async_trait]
impl SurrealSink for DryRunSink {
    async fn write_universal_rows(&self, _rows: &[UniversalRow]) -> Result<()> {
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[sync_core::UniversalRelation],
    ) -> Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, _change: &sync_core::UniversalChange) -> Result<()> {
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        _change: &sync_core::UniversalRelationChange,
    ) -> Result<()> {
        Ok(())
    }
}

/// Long-lived CSV reader that polls decode chunks into the apply window.
///
/// File reads continue under spare `max_in_flight` capacity — there is no
/// outer accumulate→`run_source_runtime` barrier per `batch_size`.
struct CsvStreamDriver {
    reader: csv::Reader<Box<dyn std::io::Read + Send>>,
    headers: Vec<String>,
    table: String,
    id_field: Option<String>,
    table_schema: Option<GeneratorTableDefinition>,
    poll_chunk: usize,
    record_count: u64,
    sunk_count: u64,
    finished: bool,
    /// First record already read (no-header column-count probe).
    pending_first: Option<csv::StringRecord>,
}

impl CsvStreamDriver {
    fn record_to_event(&mut self, record: &csv::StringRecord) -> Result<PositionedEvent<u64>> {
        if record.len() != self.headers.len() {
            anyhow::bail!(
                "Column count mismatch in CSV row {}: expected {} columns ({}), but found {} columns",
                self.record_count + 1,
                self.headers.len(),
                self.headers.join(", "),
                record.len()
            );
        }

        let mut data: HashMap<String, TypedValue> = HashMap::new();
        for (i, value) in record.iter().enumerate() {
            if i < self.headers.len() {
                let column_name = &self.headers[i];
                let schema_type = self
                    .table_schema
                    .as_ref()
                    .and_then(|ts| ts.get_field_type(column_name));
                let parsed_value = parse_value_with_schema(value, schema_type);
                data.insert(column_name.clone(), parsed_value);
            }
        }

        let id_value = if let Some(ref id_field) = self.id_field {
            data.get(id_field)
                .map(|tv| tv.value.clone())
                .unwrap_or_else(|| UniversalValue::Ulid(ulid::Ulid::new()))
        } else {
            UniversalValue::Ulid(ulid::Ulid::new())
        };

        let fields: HashMap<String, UniversalValue> =
            data.into_iter().map(|(k, tv)| (k, tv.value)).collect();
        let row = UniversalRow::new(self.table.clone(), self.record_count, id_value, fields);
        let pos = self.record_count;
        self.record_count = self.record_count.saturating_add(1);
        let change = sync_core::UniversalChange::update(row.table, row.id, row.fields);
        Ok(PositionedEvent::change(change, pos))
    }
}

#[async_trait::async_trait]
impl SourceDriver for CsvStreamDriver {
    type Position = u64;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.finished {
            return Ok(Vec::new());
        }

        let mut events = Vec::with_capacity(self.poll_chunk);
        while events.len() < self.poll_chunk {
            let record = if let Some(first) = self.pending_first.take() {
                first
            } else {
                let mut record = csv::StringRecord::new();
                match self.reader.read_record(&mut record) {
                    Ok(true) => record,
                    Ok(false) => {
                        self.finished = true;
                        break;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to read CSV record: {e}"));
                    }
                }
            };
            events.push(self.record_to_event(&record)?);
        }
        Ok(events)
    }

    async fn commit(&mut self, _position: Self::Position) -> Result<()> {
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::CommitOnly
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.sunk_count = self.sunk_count.saturating_add(count);
    }
}

/// Process CSV data from a reader and import into SurrealDB
///
/// This function handles all CSV parsing, data conversion, and SurrealDB insertion
/// for a single CSV source (file, S3, or HTTP).
async fn process_csv_reader<S: SurrealSink>(
    surreal: &S,
    config: &Config,
    reader: Box<dyn std::io::Read + Send>,
    source_name: &str,
    metrics_collector: Option<&super::metrics::MetricsCollector>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Processing CSV from: {source_name}");

    // Create CSV reader with configuration
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(config.has_headers)
        .delimiter(config.delimiter)
        .from_reader(reader);

    let (headers, pending_first) = if config.has_headers {
        let headers = csv_reader
            .headers()
            .context("Failed to read CSV headers")?
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>();
        (headers, None)
    } else if let Some(ref column_names) = config.column_names {
        (column_names.clone(), None)
    } else {
        // No headers / names: probe the first record for column count, then
        // stream that record + the rest (avoid loading the whole file).
        let mut first = csv::StringRecord::new();
        match csv_reader.read_record(&mut first) {
            Ok(true) => {
                let headers: Vec<String> = (0..first.len()).map(|i| format!("column_{i}")).collect();
                (headers, Some(first))
            }
            Ok(false) => {
                warn!("CSV file is empty");
                return Ok(());
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to read CSV records: {e}"));
            }
        }
    };

    debug!("CSV headers/columns: {headers:?}");

    let table_schema = config
        .schema
        .as_ref()
        .and_then(|s| s.get_table(&config.table))
        .cloned();

    let mut driver = CsvStreamDriver {
        reader: csv_reader,
        headers,
        table: config.table.clone(),
        id_field: config.id_field.clone(),
        table_schema,
        poll_chunk: config.batch_size.max(1),
        record_count: 0,
        sunk_count: 0,
        finished: false,
        pending_first,
    };

    if config.dry_run {
        let sink = DryRunSink;
        run_source_runtime(
            &mut driver,
            &sink,
            pipeline,
            apply_opts,
            &SourceRuntimeOpts::default(),
        )
        .await?;
    } else {
        run_source_runtime(
            &mut driver,
            surreal,
            pipeline,
            apply_opts,
            &SourceRuntimeOpts::default(),
        )
        .await?;
    }

    if let Some(collector) = metrics_collector {
        collector.add_rows(driver.sunk_count);
    }

    info!(
        "Processed {} records from {source_name} (sunk: {})",
        driver.record_count, driver.sunk_count,
    );

    Ok(())
}

/// Sync CSV files to SurrealDB with identity transforms.
///
/// This function streams CSV files from various sources and imports them into a SurrealDB table
/// in configurable batches.
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `config` - Configuration for the CSV import operation
///
/// # Returns
/// Returns Ok(()) on successful completion, or an error if the sync fails
pub async fn sync<S: SurrealSink>(surreal: &S, config: Config) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    sync_with_transforms(surreal, config, &pipeline, &apply_opts).await
}

/// Sync CSV files through a long-lived [`SourceDriver`] + [`run_source_runtime`].
///
/// The driver streams file reads into the apply window (`Config::batch_size`
/// rows per poll); the runtime owns `max_in_flight` windowing with no
/// per-chunk runtime restart barrier.
pub async fn sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    config: Config,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Starting CSV sync to SurrealDB");
    info!("Target table: {}", config.table);
    info!("Sources to process: {:?}", config.sources);
    info!("Files to process (legacy): {:?}", config.files);
    info!("S3 URIs to process (legacy): {:?}", config.s3_uris);
    info!(
        "HTTP/HTTPS URIs to process (legacy): {:?}",
        config.http_uris
    );
    info!("Batch size: {}", config.batch_size);
    if pipeline.is_identity() {
        debug!("CSV sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            "CSV sync using transform pipeline"
        );
    }

    if config.dry_run {
        warn!("Running in dry-run mode - no data will be written");
    }

    // Start metrics collection if requested
    let metrics_task = if let Some(ref metrics_path) = config.emit_metrics {
        info!("Metrics emission enabled: {}", metrics_path.display());
        let collector = super::metrics::MetricsCollector::new(metrics_path.clone());
        let task = collector.start_emission_task(std::time::Duration::from_secs(1));
        Some((collector, task))
    } else {
        None
    };

    // Get metrics collector reference for passing to process_csv_reader
    let metrics_ref = metrics_task.as_ref().map(|(collector, _)| collector);

    // Collect all resolved sources
    let mut all_resolved: Vec<ResolvedSource> = Vec::new();

    // Process new unified sources
    for source in &config.sources {
        let resolved = source
            .resolve()
            .await
            .with_context(|| format!("Failed to resolve source: {}", source.display_name()))?;

        // Filter by .csv extension
        let csv_files: Vec<_> = resolved
            .into_iter()
            .filter(|r| {
                r.extension()
                    .map(|e| e.eq_ignore_ascii_case("csv"))
                    .unwrap_or(false)
            })
            .collect();

        if csv_files.is_empty() && source.is_directory() {
            warn!("No CSV files found in directory: {}", source.display_name());
        }

        all_resolved.extend(csv_files);
    }

    // Also process legacy fields for backward compatibility
    for file_path in &config.files {
        all_resolved.push(ResolvedSource::Local(file_path.clone()));
    }

    for s3_uri in &config.s3_uris {
        let (bucket, key) = surreal_sync_file::parse_s3_uri(s3_uri)?;
        all_resolved.push(ResolvedSource::S3 { bucket, key });
    }

    for http_uri in &config.http_uris {
        all_resolved.push(ResolvedSource::Http(http_uri.clone()));
    }

    info!("Resolved {} CSV sources to process", all_resolved.len());

    // Process each resolved source
    for resolved_source in &all_resolved {
        let reader = resolved_source
            .open(DEFAULT_BUFFER_SIZE)
            .await
            .with_context(|| {
                format!(
                    "Failed to open CSV source: {}",
                    resolved_source.display_name()
                )
            })?;

        process_csv_reader(
            surreal,
            &config,
            reader,
            &resolved_source.display_name(),
            metrics_ref,
            pipeline,
            apply_opts,
        )
        .await?;
    }

    // Stop metrics collection if it was started
    if let Some((_collector, task)) = metrics_task {
        task.abort();
    }

    info!("CSV sync completed successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use sync_core::{UniversalChange, UniversalRelation};
    use tempfile::NamedTempFile;

    /// Mock SurrealDB sink for testing
    struct MockSink {
        rows_written: Arc<AtomicUsize>,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                rows_written: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn rows_written(&self) -> usize {
            self.rows_written.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl SurrealSink for MockSink {
        async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
            self.rows_written.fetch_add(rows.len(), Ordering::SeqCst);
            Ok(())
        }

        async fn write_universal_relations(
            &self,
            _relations: &[UniversalRelation],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn apply_universal_change(&self, _change: &UniversalChange) -> anyhow::Result<()> {
            Ok(())
        }

        async fn apply_universal_relation_change(
            &self,
            _change: &sync_core::UniversalRelationChange,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.batch_size, 1000);
        assert!(config.has_headers);
        assert_eq!(config.delimiter, b',');
        assert!(!config.dry_run);
    }

    #[tokio::test]
    async fn test_csv_parsing() {
        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,age").unwrap();
        writeln!(temp_file, "1,Alice,30").unwrap();
        writeln!(temp_file, "2,Bob,25").unwrap();
        temp_file.flush().unwrap();

        let config = Config {
            files: vec![temp_file.path().to_path_buf()],
            table: "test_table".to_string(),
            batch_size: 10,
            dry_run: false,
            ..Default::default()
        };

        let mock_sink = MockSink::new();
        let result = sync(&mock_sink, config).await;

        assert!(result.is_ok());
        assert_eq!(mock_sink.rows_written(), 2); // 2 data rows
    }

    #[test]
    fn test_parse_value_with_schema_int() {
        let result = parse_value_with_schema("42", Some(&UniversalType::Int32));
        assert_eq!(result.value.as_i32(), Some(42));
    }

    #[test]
    fn test_parse_value_with_schema_bool() {
        let result = parse_value_with_schema("true", Some(&UniversalType::Bool));
        assert_eq!(result.value.as_bool(), Some(true));
    }

    #[test]
    fn test_parse_value_with_schema_text() {
        let result = parse_value_with_schema("hello", Some(&UniversalType::Text));
        assert_eq!(result.value.as_str(), Some("hello"));
    }

    #[test]
    fn test_parse_value_inferred() {
        // Integer
        let result = parse_value_with_schema("42", None);
        assert_eq!(result.value.as_i64(), Some(42));

        // Float
        let result = parse_value_with_schema("3.15", None);
        assert!((result.value.as_f64().unwrap() - 3.15).abs() < 0.001);

        // String
        let result = parse_value_with_schema("hello", None);
        assert_eq!(result.value.as_str(), Some("hello"));
    }
}
