//! JSONL synchronization logic

use crate::conversion::ConversionRule;
use anyhow::{anyhow, Context, Result};
use json_types::JsonValueWithSchema;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use surreal_sink::SurrealSink;
use surreal_sync_file::{FileSource, DEFAULT_BUFFER_SIZE};
use sync_core::{DatabaseSchema, Row, TableDefinition, Type, TypedValue, Value};
use sync_transform::{
    run_source_runtime, ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver,
    SourceRuntimeOpts,
};

/// Source database connection options (JSONL-specific)
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub source_uri: String,
}

/// Configuration for JSONL import
#[derive(Clone)]
pub struct Config {
    /// List of file sources to import (supports local files, S3 URIs, HTTP URLs, and directories)
    /// When a source ends with '/', it is treated as a directory and all .jsonl files are imported
    pub sources: Vec<FileSource>,

    /// List of JSONL file paths to import (legacy, use `sources` instead)
    pub files: Vec<PathBuf>,

    /// List of S3 URIs to import (legacy, use `sources` instead)
    pub s3_uris: Vec<String>,

    /// List of HTTP/HTTPS URLs to import (legacy, use `sources` instead)
    pub http_uris: Vec<String>,

    /// Field to use as record ID (default: "id")
    pub id_field: String,

    /// Multi-column record ID fields (takes precedence over `id_field` when non-empty).
    /// When two or more are set, the ID is an [`Value::Array`].
    pub id_columns: Vec<String>,

    /// Conversion rules for transforming JSON objects to Thing references
    pub conversion_rules: Vec<String>,

    /// Number of records to process in each batch
    pub batch_size: usize,

    /// Whether to perform a dry run without writing data
    pub dry_run: bool,

    /// Optional schema for type-aware conversion (e.g., UUID, DateTime parsing)
    pub schema: Option<DatabaseSchema>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sources: vec![],
            files: vec![],
            s3_uris: vec![],
            http_uris: vec![],
            id_field: "id".to_string(),
            id_columns: Vec::new(),
            conversion_rules: vec![],
            batch_size: 1000,
            dry_run: false,
            schema: None,
        }
    }
}

/// No-op sink for JSONL `--dry-run` (still exercises the apply window).
struct DryRunSink;

#[async_trait::async_trait]
impl SurrealSink for DryRunSink {
    async fn write_rows(&self, _rows: &[Row]) -> Result<()> {
        Ok(())
    }

    async fn write_relations(&self, _relations: &[sync_core::Relation]) -> Result<()> {
        Ok(())
    }

    async fn apply_change(&self, _change: &sync_core::Change) -> Result<()> {
        Ok(())
    }

    async fn apply_relation_change(&self, _change: &sync_core::RelationChange) -> Result<()> {
        Ok(())
    }
}

/// Long-lived JSONL line reader that polls decode chunks into the apply window.
///
/// File reads continue under spare `max_in_flight` capacity — there is no
/// outer accumulate→`run_source_runtime` barrier per `batch_size`.
struct JsonlStreamDriver {
    lines: std::io::Lines<BufReader<Box<dyn std::io::Read + Send>>>,
    table_name: String,
    id_field: String,
    id_columns: Vec<String>,
    rules: Vec<ConversionRule>,
    table_schema: Option<TableDefinition>,
    poll_chunk: usize,
    line_count: u64,
    sunk_count: u64,
    finished: bool,
}

#[async_trait::async_trait]
impl SourceDriver for JsonlStreamDriver {
    type Position = u64;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.finished {
            return Ok(Vec::new());
        }

        let mut events = Vec::with_capacity(self.poll_chunk);
        while events.len() < self.poll_chunk {
            let Some(line_result) = self.lines.next() else {
                self.finished = true;
                break;
            };
            let line = line_result?;
            self.line_count = self.line_count.saturating_add(1);
            if line.trim().is_empty() {
                continue;
            }

            let json_value: JsonValue = serde_json::from_str(&line)
                .map_err(|e| anyhow!("Error parsing JSON at line {}: {e}", self.line_count))?;

            let row = convert_json_to_universal_row(
                &json_value,
                &self.table_name,
                &self.id_field,
                &self.id_columns,
                &self.rules,
                self.table_schema.as_ref(),
                self.line_count,
            )?;
            let pos = self.line_count;
            let change = sync_core::Change::update(row.table, row.id, row.fields);
            events.push(PositionedEvent::change(change, pos));
        }
        Ok(events)
    }

    async fn advance_watermark(&mut self, _position: Self::Position) -> Result<()> {
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::AdvanceOnly
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.sunk_count = self.sunk_count.saturating_add(count);
    }
}

/// Process JSONL data from a reader and import into SurrealDB
///
/// This function handles all JSONL parsing, data conversion, and SurrealDB insertion
/// for a single JSONL source (file, S3, or HTTP).
///
/// [`ConversionRule`]s are applied while building each [`Row`], before
/// the batch is passed through the transform [`Pipeline`].
async fn process_jsonl_reader<S: SurrealSink>(
    surreal: &S,
    config: &Config,
    reader: Box<dyn std::io::Read + Send>,
    source_name: &str,
    rules: &[ConversionRule],
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    tracing::info!("Processing JSONL from: {source_name}");

    // Determine table name from source name (filename without extension)
    let table_name = if source_name.starts_with("http://") || source_name.starts_with("https://") {
        // For HTTP URLs, extract filename from path
        source_name
            .rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".jsonl"))
            .unwrap_or("items")
            .to_string()
    } else if source_name.starts_with("s3://") {
        // For S3 URIs, extract filename from key
        source_name
            .rsplit('/')
            .next()
            .and_then(|s| s.strip_suffix(".jsonl"))
            .unwrap_or("items")
            .to_string()
    } else {
        // For local files, use the file stem
        let path = PathBuf::from(source_name);
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("items")
            .to_string()
    };

    tracing::info!("Target table: {table_name}");

    // Get table schema for type-aware conversion if available
    let table_schema = config
        .schema
        .as_ref()
        .and_then(|s| s.get_table(&table_name))
        .cloned();

    let mut driver = JsonlStreamDriver {
        lines: BufReader::new(reader).lines(),
        table_name: table_name.clone(),
        id_field: config.id_field.clone(),
        id_columns: config.id_columns.clone(),
        rules: rules.to_vec(),
        table_schema,
        poll_chunk: config.batch_size.max(1),
        line_count: 0,
        sunk_count: 0,
        finished: false,
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

    tracing::info!(
        "Completed migration of {} documents from {} to table {}",
        driver.sunk_count,
        source_name,
        table_name
    );

    Ok(())
}

/// Sync JSONL files to SurrealDB with identity transforms.
///
/// This function streams JSONL files from various sources and imports them into SurrealDB tables.
/// The table name is derived from the filename (without .jsonl extension).
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `config` - Configuration for the JSONL import operation
///
/// # Returns
/// Returns Ok(()) on successful completion, or an error if the sync fails
pub async fn sync<S: SurrealSink>(surreal: &S, config: Config) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    sync_with_transforms(surreal, config, &pipeline, &apply_opts).await
}

/// Sync JSONL files through a long-lived [`SourceDriver`] + [`run_source_runtime`].
///
/// The driver streams line reads into the apply window (`Config::batch_size`
/// rows per poll). [`Config::conversion_rules`] still run while decoding each
/// line into a [`Row`], before any Pipeline stages. **Multi-file
/// imports start a fresh runtime per file** (intentional — no cross-file
/// pipelining).
pub async fn sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    config: Config,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    tracing::info!("Starting JSONL migration");
    tracing::info!("Sources to process: {:?}", config.sources);
    tracing::info!("Files to process: {:?}", config.files);
    tracing::info!("S3 URIs to process: {:?}", config.s3_uris);
    tracing::info!("HTTP/HTTPS URIs to process: {:?}", config.http_uris);
    if pipeline.is_identity() {
        tracing::debug!("JSONL sync using identity transform pipeline");
    } else {
        tracing::info!(
            stages = pipeline.len(),
            max_in_flight = apply_opts.max_in_flight,
            "JSONL sync using transform pipeline"
        );
    }

    if config.dry_run {
        tracing::warn!("Running in dry-run mode - no data will be written");
    }

    // Parse conversion rules
    let mut rules = Vec::new();
    for rule_str in &config.conversion_rules {
        rules.push(ConversionRule::parse(rule_str)?);
    }
    tracing::debug!("Parsed {} conversion rules", rules.len());

    let mut total_sources = 0;

    // Process sources from the new unified interface
    for source in &config.sources {
        let resolved_sources = source.resolve().await?;

        // Filter for .jsonl files only
        let jsonl_sources: Vec<_> = resolved_sources
            .into_iter()
            .filter(|s| s.extension() == Some("jsonl"))
            .collect();

        if jsonl_sources.is_empty() && source.is_directory() {
            tracing::warn!("No .jsonl files found in directory: {:?}", source);
        }

        for resolved in jsonl_sources {
            let source_name = resolved.display_name();
            let reader = resolved
                .open(DEFAULT_BUFFER_SIZE)
                .await
                .with_context(|| format!("Failed to open JSONL source: {source_name}"))?;

            process_jsonl_reader(
                surreal,
                &config,
                reader,
                &source_name,
                &rules,
                pipeline,
                apply_opts,
            )
            .await?;
            total_sources += 1;
        }
    }

    // Legacy: Process each local JSONL file
    for file_path in &config.files {
        let source = FileSource::Local(file_path.clone());
        let resolved = source.resolve().await?;

        for r in resolved {
            let source_name = r.display_name();
            let reader = r
                .open(DEFAULT_BUFFER_SIZE)
                .await
                .context("Failed to open JSONL file")?;
            process_jsonl_reader(
                surreal,
                &config,
                reader,
                &source_name,
                &rules,
                pipeline,
                apply_opts,
            )
            .await?;
            total_sources += 1;
        }
    }

    // Legacy: Process each S3 JSONL file
    for s3_uri in &config.s3_uris {
        let source = FileSource::parse(s3_uri)?;
        let resolved = source.resolve().await?;

        for r in resolved {
            let source_name = r.display_name();
            let reader = r
                .open(DEFAULT_BUFFER_SIZE)
                .await
                .context("Failed to open S3 JSONL file")?;
            process_jsonl_reader(
                surreal,
                &config,
                reader,
                &source_name,
                &rules,
                pipeline,
                apply_opts,
            )
            .await?;
            total_sources += 1;
        }
    }

    // Legacy: Process each HTTP/HTTPS JSONL file
    for http_uri in &config.http_uris {
        let source = FileSource::parse(http_uri)?;
        let resolved = source.resolve().await?;

        for r in resolved {
            let source_name = r.display_name();
            let reader = r
                .open(DEFAULT_BUFFER_SIZE)
                .await
                .context("Failed to open HTTP/HTTPS JSONL file")?;
            process_jsonl_reader(
                surreal,
                &config,
                reader,
                &source_name,
                &rules,
                pipeline,
                apply_opts,
            )
            .await?;
            total_sources += 1;
        }
    }

    tracing::info!(
        "JSONL migration completed: processed {} sources",
        total_sources
    );
    Ok(())
}

fn convert_json_to_universal_row(
    value: &JsonValue,
    table_name: &str,
    id_field: &str,
    id_columns: &[String],
    rules: &[ConversionRule],
    table_schema: Option<&TableDefinition>,
    record_index: u64,
) -> Result<Row> {
    let effective_id_cols: Vec<&str> = if id_columns.is_empty() {
        vec![id_field]
    } else {
        id_columns.iter().map(|s| s.as_str()).collect()
    };

    if let JsonValue::Object(obj) = value {
        let mut fields: HashMap<String, Value> = HashMap::new();
        let mut id_parts: Vec<Option<Value>> = vec![None; effective_id_cols.len()];

        for (key, val) in obj {
            if let Some(idx) = effective_id_cols.iter().position(|c| *c == key) {
                let part = if let JsonValue::String(s) = val {
                    Value::Text(s.clone())
                } else if let JsonValue::Number(n) = val {
                    if let Some(i) = n.as_i64() {
                        Value::Int64(i)
                    } else if let Some(u) = n.as_u64() {
                        Value::Int64(u as i64)
                    } else {
                        anyhow::bail!("ID field number must be an integer: {n}");
                    }
                } else {
                    return Err(anyhow!("ID field must be a string or number"));
                };
                id_parts[idx] = Some(part);
            } else {
                let data_type = table_schema.and_then(|ts| ts.get_column_type(key));
                let v = convert_value_to_universal(val, rules, data_type);
                fields.insert(key.clone(), v);
            }
        }

        let mut parts = Vec::with_capacity(effective_id_cols.len());
        for (i, col) in effective_id_cols.iter().enumerate() {
            match id_parts[i].take() {
                Some(p) => parts.push(p),
                None => return Err(anyhow!("Missing ID field: {col}")),
            }
        }
        let id = sync_core::build_composite_record_id(parts);

        Ok(Row::new(table_name.to_string(), record_index, id, fields))
    } else {
        Err(anyhow!("JSONL line must be a JSON object"))
    }
}

/// Convert a JSON value to Value with optional schema type hint
fn convert_value_to_universal(
    value: &JsonValue,
    rules: &[ConversionRule],
    data_type: Option<&Type>,
) -> Value {
    // First check if this is an object that matches a conversion rule (Thing reference)
    // Convert Thing references to Text format "table:id"
    if let JsonValue::Object(obj) = value {
        if let Some(type_value) = obj.get("type").and_then(|v| v.as_str()) {
            for rule in rules {
                if rule.type_value == type_value {
                    // This object matches the rule, convert to Thing reference as text
                    if let Some(id_value) = obj.get(&rule.id_field).and_then(|v| v.as_str()) {
                        return Value::Thing {
                            table: rule.target_table.clone(),
                            id: Box::new(Value::Text(id_value.to_string())),
                        };
                    }
                }
            }
        }
    }

    // If we have a schema type hint, use json-types for type-aware conversion
    if let Some(dt) = data_type {
        let tv = JsonValueWithSchema::new(value.clone(), dt.clone()).to_typed_value();
        return tv.value;
    }

    // Fall back to generic conversion (inferred types)
    convert_value_inferred(value).value
}

/// Convert a JSON value to TypedValue with inferred types (no schema)
///
/// Note: This function does NOT handle Thing conversion rules.
/// Thing conversion is handled in convert_value_with_schema before calling this.
fn convert_value_inferred(value: &JsonValue) -> TypedValue {
    match value {
        JsonValue::Null => TypedValue::null(Type::Text),
        JsonValue::Bool(b) => TypedValue::bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                TypedValue::int64(i)
            } else if let Some(f) = n.as_f64() {
                TypedValue::float64(f)
            } else {
                TypedValue::null(Type::Int64)
            }
        }
        JsonValue::String(s) => {
            // Try to parse as UUID
            if let Ok(uuid) = uuid::Uuid::parse_str(s) {
                return TypedValue::uuid(uuid);
            }
            // Try to parse as DateTime
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                return TypedValue::datetime(dt.with_timezone(&chrono::Utc));
            }
            TypedValue::text(s)
        }
        JsonValue::Array(arr) => {
            let values: Vec<Value> = arr
                .iter()
                .map(|item| convert_value_inferred(item).value)
                .collect();
            TypedValue::array(values, Type::Text)
        }
        JsonValue::Object(obj) => {
            // Convert as regular object (Thing rules already checked in convert_value_with_schema)
            TypedValue {
                sync_type: Type::Json,
                value: Value::Json(Box::new(serde_json::Value::Object(obj.clone()))),
            }
        }
    }
}
