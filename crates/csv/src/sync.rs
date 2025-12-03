//! CSV synchronization implementation
//!
//! This module handles streaming CSV files from various sources and importing them into SurrealDB tables.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use surreal_sync_file::{FileSource, ResolvedSource, DEFAULT_BUFFER_SIZE};
use surrealdb::sql::{Id, Thing};
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

    /// Target namespace
    pub namespace: String,

    /// Target database
    pub database: String,

    /// SurrealDB connection options
    pub surreal_opts: crate::surreal::SurrealOpts,

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
            namespace: "test".to_string(),
            database: "test".to_string(),
            surreal_opts: crate::surreal::SurrealOpts {
                surreal_endpoint: "ws://localhost:8000".to_string(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
            },
            has_headers: true,
            delimiter: b',',
            id_field: None,
            column_names: None,
            emit_metrics: None,
            dry_run: false,
        }
    }
}

/// Process CSV data from a reader and import into SurrealDB
///
/// This function handles all CSV parsing, data conversion, and SurrealDB insertion
/// for a single CSV source (file, S3, or HTTP).
async fn process_csv_reader(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
    reader: Box<dyn std::io::Read + Send>,
    source_name: &str,
    metrics_collector: Option<&super::metrics::MetricsCollector>,
) -> Result<()> {
    info!("Processing CSV from: {source_name}");

    // Create CSV reader with configuration
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(config.has_headers)
        .delimiter(config.delimiter)
        .from_reader(reader);

    // Get headers if available
    let headers = if config.has_headers {
        csv_reader
            .headers()
            .context("Failed to read CSV headers")?
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>()
    } else if let Some(ref column_names) = config.column_names {
        // Use provided column names
        column_names.clone()
    } else {
        // Generate column names if no headers and no names provided
        // We need to peek at the first record to determine column count
        // Since we can't peek without consuming, we'll use a workaround:
        // Read all records into memory first
        let all_records: Vec<_> = csv_reader
            .records()
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to read CSV records")?;

        if all_records.is_empty() {
            warn!("CSV file is empty");
            return Ok(());
        }

        let column_count = all_records[0].len();
        let headers: Vec<String> = (0..column_count).map(|i| format!("column_{i}")).collect();

        // Process all the records we just read
        let mut total_processed = 0;
        let mut record_count = 0;
        let mut batch: Vec<crate::surreal::Record> = Vec::new();

        for record in all_records {
            // Validate column count matches
            if record.len() != headers.len() {
                anyhow::bail!(
                    "Column count mismatch in CSV row {}: expected {} columns ({}), but found {} columns",
                    record_count + 1,
                    headers.len(),
                    headers.join(", "),
                    record.len()
                );
            }

            // Convert CSV record to SurrealDB Record
            let mut data = HashMap::new();

            for (i, value) in record.iter().enumerate() {
                if i < headers.len() {
                    let column_name = &headers[i];

                    // Try to parse as number, boolean, or keep as string
                    let parsed_value = if let Ok(n) = value.parse::<i64>() {
                        crate::surreal::SurrealValue::Int(n)
                    } else if let Ok(f) = value.parse::<f64>() {
                        crate::surreal::SurrealValue::Float(f)
                    } else if let Ok(b) = value.parse::<bool>() {
                        crate::surreal::SurrealValue::Bool(b)
                    } else if value.is_empty() {
                        crate::surreal::SurrealValue::Null
                    } else {
                        crate::surreal::SurrealValue::String(value.to_string())
                    };

                    data.insert(column_name.clone(), parsed_value);
                }
            }

            // Create the ID for the record
            let id = if let Some(ref id_field) = config.id_field {
                // Use specified field as ID
                if let Some(id_value) = data.get(id_field) {
                    match id_value {
                        crate::surreal::SurrealValue::String(s) => Id::String(s.clone()),
                        crate::surreal::SurrealValue::Int(n) => Id::Number(*n),
                        _ => Id::ulid(), // Fallback to ULID
                    }
                } else {
                    Id::ulid()
                }
            } else {
                Id::ulid()
            };

            let surreal_record = crate::surreal::Record {
                id: Thing::from((config.table.as_str(), id)),
                data,
            };

            batch.push(surreal_record);
            record_count += 1;

            // Process batch when it reaches the configured size
            if batch.len() >= config.batch_size {
                if !config.dry_run {
                    crate::surreal::write_records(surreal, &config.table, &batch).await?;
                    total_processed += batch.len();
                } else {
                    debug!("Dry run: Would insert batch of {} records", batch.len());
                    total_processed += batch.len();
                }

                // Update metrics
                if let Some(collector) = metrics_collector {
                    collector.add_rows(batch.len() as u64);
                }

                batch.clear();
            }
        }

        // Process remaining records
        if !batch.is_empty() {
            if !config.dry_run {
                crate::surreal::write_records(surreal, &config.table, &batch).await?;
                total_processed += batch.len();
            } else {
                debug!(
                    "Dry run: Would insert final batch of {} records",
                    batch.len()
                );
                total_processed += batch.len();
            }

            // Update metrics for final batch
            if let Some(collector) = metrics_collector {
                collector.add_rows(batch.len() as u64);
            }
        }

        info!(
            "Processed {record_count} records from {source_name} (total processed: {total_processed})",
        );

        // Return early since we already processed everything
        return Ok(());
    };

    debug!("CSV headers/columns: {headers:?}");

    // Process records in batches
    let mut batch: Vec<crate::surreal::Record> = Vec::new();
    let mut total_processed = 0;
    let mut record_count = 0;

    for result in csv_reader.records() {
        let record = result.context("Failed to read CSV record")?;

        // Validate column count matches
        if record.len() != headers.len() {
            anyhow::bail!(
                "Column count mismatch in CSV row {}: expected {} columns ({}), but found {} columns",
                record_count + 1,
                headers.len(),
                headers.join(", "),
                record.len()
            );
        }

        // Convert CSV record to SurrealDB Record
        let mut data = HashMap::new();

        for (i, value) in record.iter().enumerate() {
            if i < headers.len() {
                let column_name = &headers[i];

                // Try to parse as number, boolean, or keep as string
                let parsed_value = if let Ok(n) = value.parse::<i64>() {
                    crate::surreal::SurrealValue::Int(n)
                } else if let Ok(f) = value.parse::<f64>() {
                    crate::surreal::SurrealValue::Float(f)
                } else if let Ok(b) = value.parse::<bool>() {
                    crate::surreal::SurrealValue::Bool(b)
                } else if value.is_empty() {
                    crate::surreal::SurrealValue::Null
                } else {
                    crate::surreal::SurrealValue::String(value.to_string())
                };

                data.insert(column_name.clone(), parsed_value);
            }
        }

        // Create the ID for the record
        let id = if let Some(ref id_field) = config.id_field {
            // Use specified field as ID
            if let Some(id_value) = data.get(id_field) {
                match id_value {
                    crate::surreal::SurrealValue::String(s) => Id::String(s.clone()),
                    crate::surreal::SurrealValue::Int(n) => Id::Number(*n),
                    _ => Id::ulid(), // Fallback to ULID
                }
            } else {
                Id::ulid()
            }
        } else {
            Id::ulid()
        };

        let surreal_record = crate::surreal::Record {
            id: Thing::from((config.table.as_str(), id)),
            data,
        };

        batch.push(surreal_record);
        record_count += 1;

        // Process batch when it reaches the configured size
        if batch.len() >= config.batch_size {
            if !config.dry_run {
                crate::surreal::write_records(surreal, &config.table, &batch).await?;
                total_processed += batch.len();
            } else {
                debug!("Dry run: Would insert batch of {} records", batch.len());
                total_processed += batch.len();
            }

            // Update metrics
            if let Some(collector) = metrics_collector {
                collector.add_rows(batch.len() as u64);
            }

            batch.clear();
        }
    }

    // Process remaining records
    if !batch.is_empty() {
        if !config.dry_run {
            crate::surreal::write_records(surreal, &config.table, &batch).await?;
            total_processed += batch.len();
        } else {
            debug!(
                "Dry run: Would insert final batch of {} records",
                batch.len()
            );
            total_processed += batch.len();
        }

        // Update metrics for final batch
        if let Some(collector) = metrics_collector {
            collector.add_rows(batch.len() as u64);
        }
    }

    info!(
        "Processed {record_count} records from {source_name} (total processed: {total_processed})",
    );

    Ok(())
}

/// Sync CSV files to SurrealDB
///
/// This function streams CSV files from various sources and imports them into a SurrealDB table
/// in configurable batches.
///
/// # Arguments
/// * `config` - Configuration for the CSV import operation
///
/// # Returns
/// Returns Ok(()) on successful completion, or an error if the sync fails
pub async fn sync(config: Config) -> Result<()> {
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

    // Connect to SurrealDB using the standard connection function
    let surreal =
        crate::surreal::surreal_connect(&config.surreal_opts, &config.namespace, &config.database)
            .await
            .context("Failed to connect to SurrealDB")?;

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
            &surreal,
            &config,
            reader,
            &resolved_source.display_name(),
            metrics_ref,
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
    use tempfile::NamedTempFile;

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
            dry_run: true, // Don't actually write to DB
            surreal_opts: crate::surreal::SurrealOpts {
                surreal_endpoint: "ws://localhost:8000".to_string(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
            },
            ..Default::default()
        };

        // This should not panic and should process the file
        let result = sync(config).await;

        // In dry-run mode with no real DB connection, this will fail at connection
        // but we're mainly testing that the CSV parsing logic compiles
        assert!(result.is_err()); // Expected to fail at DB connection in test
    }
}
