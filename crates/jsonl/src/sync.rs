//! JSONL synchronization logic

use crate::conversion::ConversionRule;
use crate::surreal::{FieldValue, Record, SourceOpts, SurrealOpts};
use anyhow::{anyhow, Context, Result};
use json_types::JsonValueWithSchema;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use surreal_sync_file::{FileSource, DEFAULT_BUFFER_SIZE};
use surrealdb::sql::Thing;
use sync_core::{Schema, TableDefinition, TypedValue, UniversalType, UniversalValue};

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

    /// Target namespace
    pub namespace: String,

    /// Target database
    pub database: String,

    /// SurrealDB connection options
    pub surreal_opts: SurrealOpts,

    /// Field to use as record ID (default: "id")
    pub id_field: String,

    /// Conversion rules for transforming JSON objects to Thing references
    pub conversion_rules: Vec<String>,

    /// Number of records to process in each batch
    pub batch_size: usize,

    /// Whether to perform a dry run without writing data
    pub dry_run: bool,

    /// Optional schema for type-aware conversion (e.g., UUID, DateTime parsing)
    pub schema: Option<Schema>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sources: vec![],
            files: vec![],
            s3_uris: vec![],
            http_uris: vec![],
            namespace: "test".to_string(),
            database: "test".to_string(),
            surreal_opts: SurrealOpts {
                surreal_endpoint: "ws://localhost:8000".to_string(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
                batch_size: 1000,
                dry_run: false,
            },
            id_field: "id".to_string(),
            conversion_rules: vec![],
            batch_size: 1000,
            dry_run: false,
            schema: None,
        }
    }
}

/// Process JSONL data from a reader and import into SurrealDB
///
/// This function handles all JSONL parsing, data conversion, and SurrealDB insertion
/// for a single JSONL source (file, S3, or HTTP).
async fn process_jsonl_reader(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
    reader: Box<dyn std::io::Read + Send>,
    source_name: &str,
    rules: &[ConversionRule],
) -> Result<()> {
    tracing::info!("Processing JSONL from: {source_name}");

    let buf_reader = BufReader::new(reader);
    let mut batch: Vec<Record> = Vec::new();
    let mut total_migrated = 0;

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
        .and_then(|s| s.get_table(&table_name));

    for (line_count, line) in buf_reader.lines().enumerate() {
        let line = line?;
        let line_count = line_count + 1; // Convert to 1-based line numbering for error messages

        if line.trim().is_empty() {
            continue;
        }

        // Parse JSON line
        let json_value: Value = serde_json::from_str(&line)
            .map_err(|e| anyhow!("Error parsing JSON at line {line_count}: {e}"))?;

        // Convert to surreal record with schema-aware conversion
        let record = convert_json_to_record(
            &json_value,
            &table_name,
            &config.id_field,
            rules,
            table_schema,
        )?;

        batch.push(record);

        // Process batch when it reaches the batch size
        if batch.len() >= config.batch_size {
            if !config.dry_run {
                crate::surreal::write_records(surreal, &table_name, &batch).await?;
            }
            total_migrated += batch.len();
            tracing::debug!("Migrated batch of {} documents", batch.len());
            batch.clear();
        }
    }

    // Process remaining documents
    if !batch.is_empty() {
        if !config.dry_run {
            crate::surreal::write_records(surreal, &table_name, &batch).await?;
        }
        total_migrated += batch.len();
        tracing::debug!("Migrated final batch of {} documents", batch.len());
    }

    tracing::info!(
        "Completed migration of {} documents from {} to table {}",
        total_migrated,
        source_name,
        table_name
    );

    Ok(())
}

/// Sync JSONL files to SurrealDB
///
/// This function streams JSONL files from various sources and imports them into SurrealDB tables.
/// The table name is derived from the filename (without .jsonl extension).
///
/// # Arguments
/// * `config` - Configuration for the JSONL import operation
///
/// # Returns
/// Returns Ok(()) on successful completion, or an error if the sync fails
pub async fn sync(config: Config) -> Result<()> {
    tracing::info!("Starting JSONL migration");
    tracing::info!("Sources to process: {:?}", config.sources);
    tracing::info!("Files to process: {:?}", config.files);
    tracing::info!("S3 URIs to process: {:?}", config.s3_uris);
    tracing::info!("HTTP/HTTPS URIs to process: {:?}", config.http_uris);

    if config.dry_run {
        tracing::warn!("Running in dry-run mode - no data will be written");
    }

    // Parse conversion rules
    let mut rules = Vec::new();
    for rule_str in &config.conversion_rules {
        rules.push(ConversionRule::parse(rule_str)?);
    }
    tracing::debug!("Parsed {} conversion rules", rules.len());

    // Connect to SurrealDB
    let surreal =
        crate::surreal::surreal_connect(&config.surreal_opts, &config.namespace, &config.database)
            .await?;
    tracing::info!("Connected to SurrealDB");

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

            process_jsonl_reader(&surreal, &config, reader, &source_name, &rules).await?;
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
            process_jsonl_reader(&surreal, &config, reader, &source_name, &rules).await?;
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
            process_jsonl_reader(&surreal, &config, reader, &source_name, &rules).await?;
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
            process_jsonl_reader(&surreal, &config, reader, &source_name, &rules).await?;
            total_sources += 1;
        }
    }

    tracing::info!(
        "JSONL migration completed: processed {} sources",
        total_sources
    );
    Ok(())
}

/// Migrate data from JSONL files to SurrealDB (legacy interface)
///
/// This function provides backward compatibility with the old directory-based interface.
/// It reads all .jsonl files from a directory and imports them into SurrealDB.
///
/// # Arguments
/// * `from_opts` - Source directory containing .jsonl files
/// * `to_namespace` - Target SurrealDB namespace
/// * `to_database` - Target SurrealDB database
/// * `to_opts` - SurrealDB connection options
/// * `id_field` - Field to use as record ID (default: "id")
/// * `conversion_rules` - Rules for converting JSON objects to Thing references
pub async fn migrate_from_jsonl(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    id_field: String,
    conversion_rules: Vec<String>,
) -> Result<()> {
    tracing::info!("Starting JSONL migration (legacy interface)");

    // Check if source is a directory or a single file
    let source_path = std::path::Path::new(&from_opts.source_uri);

    let files = if source_path.is_dir() {
        // Read all .jsonl files in the directory
        let mut jsonl_files = Vec::new();
        for entry in std::fs::read_dir(source_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                jsonl_files.push(path);
            }
        }
        jsonl_files
    } else if source_path.is_file() {
        // Single file
        vec![source_path.to_path_buf()]
    } else {
        return Err(anyhow!(
            "Source path must be a file or directory containing JSONL files"
        ));
    };

    // Use the new sync interface
    let config = Config {
        sources: vec![],
        files,
        s3_uris: vec![],
        http_uris: vec![],
        namespace: to_namespace,
        database: to_database,
        surreal_opts: to_opts,
        id_field,
        conversion_rules,
        batch_size: 1000,
        dry_run: false,
        schema: None, // Legacy interface doesn't support schema
    };

    sync(config).await
}

fn convert_json_to_record(
    value: &Value,
    table_name: &str,
    id_field: &str,
    rules: &[ConversionRule],
    table_schema: Option<&TableDefinition>,
) -> Result<Record> {
    let mut id: Option<surrealdb::sql::Id> = None;

    if let Value::Object(obj) = value {
        let mut data = HashMap::new();

        for (key, val) in obj {
            if key == id_field {
                // Extract ID for the record
                if let Value::String(s) = val {
                    id = Some(surrealdb::sql::Id::from(s));
                } else if let Value::Number(n) = val {
                    if let Some(i) = n.as_i64() {
                        id = Some(surrealdb::sql::Id::from(i));
                    } else if let Some(u) = n.as_u64() {
                        id = Some(surrealdb::sql::Id::from(u));
                    } else {
                        anyhow::bail!("ID field number must be an integer: {n}");
                    }
                } else {
                    return Err(anyhow!("ID field must be a string or number"));
                }
            } else {
                // Get schema type hint for this field if available
                let data_type = table_schema.and_then(|ts| ts.get_field_type(key));

                // Convert the value with schema-aware conversion using json-types
                let v = convert_value_with_schema(val, rules, data_type);
                data.insert(key.clone(), v);
            }
        }

        // Create proper SurrealDB Thing for the record
        let id = match id {
            Some(id) => surrealdb::sql::Thing::from((table_name.to_string(), id)),
            None => return Err(anyhow!("Missing ID field: {id_field}")),
        };

        Ok(Record { id, data })
    } else {
        Err(anyhow!("JSONL line must be a JSON object"))
    }
}

/// Convert a JSON value to FieldValue with optional schema type hint
fn convert_value_with_schema(
    value: &Value,
    rules: &[ConversionRule],
    data_type: Option<&UniversalType>,
) -> FieldValue {
    // First check if this is an object that matches a conversion rule (Thing reference)
    if let Value::Object(obj) = value {
        if let Some(type_value) = obj.get("type").and_then(|v| v.as_str()) {
            for rule in rules {
                if rule.type_value == type_value {
                    // This object matches the rule, convert to Thing reference
                    if let Some(id_value) = obj.get(&rule.id_field).and_then(|v| v.as_str()) {
                        let thing = Thing::from((rule.target_table.clone(), id_value.to_string()));
                        return FieldValue::Thing(thing);
                    }
                }
            }
        }
    }

    // If we have a schema type hint, use json-types for type-aware conversion
    if let Some(dt) = data_type {
        return FieldValue::Typed(
            JsonValueWithSchema::new(value.clone(), dt.clone()).to_typed_value(),
        );
    }

    // Fall back to generic conversion (inferred types)
    FieldValue::Typed(convert_value_inferred(value))
}

/// Convert a JSON value to TypedValue with inferred types (no schema)
///
/// Note: This function does NOT handle Thing conversion rules.
/// Thing conversion is handled in convert_value_with_schema before calling this.
fn convert_value_inferred(value: &Value) -> TypedValue {
    match value {
        Value::Null => TypedValue::null(UniversalType::Text),
        Value::Bool(b) => TypedValue::bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                TypedValue::bigint(i)
            } else if let Some(f) = n.as_f64() {
                TypedValue::double(f)
            } else {
                TypedValue::null(UniversalType::BigInt)
            }
        }
        Value::String(s) => {
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
        Value::Array(arr) => {
            let values: Vec<UniversalValue> = arr
                .iter()
                .map(|item| convert_value_inferred(item).value)
                .collect();
            TypedValue::array(values, UniversalType::Text)
        }
        Value::Object(obj) => {
            // Convert as regular object (Thing rules already checked in convert_value_with_schema)
            let mut map = HashMap::new();
            for (key, val) in obj {
                map.insert(key.clone(), convert_value_inferred(val).value);
            }
            TypedValue {
                sync_type: UniversalType::Json,
                value: UniversalValue::Object(map),
            }
        }
    }
}
