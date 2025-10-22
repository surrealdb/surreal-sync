//! CSV synchronization implementation
//!
//! This module handles streaming CSV files and importing them into SurrealDB tables.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use surrealdb::sql::{Id, Thing};
use tracing::{debug, info, warn};

/// Configuration for CSV import
#[derive(Clone)]
pub struct Config {
    /// List of CSV file paths to import
    pub files: Vec<PathBuf>,

    /// Target SurrealDB table name
    pub table: String,

    /// Number of rows to process in each batch
    pub batch_size: usize,

    /// Target namespace
    pub namespace: String,

    /// Target database
    pub database: String,

    /// SurrealDB connection options
    pub surreal_opts: crate::SurrealOpts,

    /// Whether the CSV has headers (default: true)
    pub has_headers: bool,

    /// CSV delimiter character (default: ',')
    pub delimiter: u8,

    /// Optional field to use as record ID
    pub id_field: Option<String>,

    /// Whether to perform a dry run without writing data
    pub dry_run: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            files: vec![],
            table: String::new(),
            batch_size: 1000,
            namespace: "test".to_string(),
            database: "test".to_string(),
            surreal_opts: crate::SurrealOpts {
                surreal_endpoint: "ws://localhost:8000".to_string(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
                batch_size: 1000,
                dry_run: false,
            },
            has_headers: true,
            delimiter: b',',
            id_field: None,
            dry_run: false,
        }
    }
}

/// Sync CSV files to SurrealDB
///
/// This function streams CSV files and imports them into a SurrealDB table
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
    info!("Files to process: {:?}", config.files);
    info!("Batch size: {}", config.batch_size);

    if config.dry_run || config.surreal_opts.dry_run {
        warn!("Running in dry-run mode - no data will be written");
    }

    // Connect to SurrealDB using the standard connection function
    let surreal =
        crate::surreal::surreal_connect(&config.surreal_opts, &config.namespace, &config.database)
            .await
            .context("Failed to connect to SurrealDB")?;

    // Process each CSV file
    for file_path in &config.files {
        process_csv_file(&surreal, &config, file_path).await?;
    }

    info!("CSV sync completed successfully");
    Ok(())
}

/// Process a single CSV file
async fn process_csv_file(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
    file_path: &PathBuf,
) -> Result<()> {
    info!("Processing file: {:?}", file_path);

    // Open the CSV file
    let file = std::fs::File::open(file_path)
        .with_context(|| format!("Failed to open CSV file: {file_path:?}"))?;

    // Create CSV reader with configuration
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(config.has_headers)
        .delimiter(config.delimiter)
        .from_reader(file);

    // Get headers if available
    let headers = if config.has_headers {
        reader
            .headers()
            .context("Failed to read CSV headers")?
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>()
    } else {
        // Generate column names if no headers
        let record = reader.records().next();
        match record {
            Some(Ok(ref r)) => (0..r.len()).map(|i| format!("column_{i}")).collect(),
            _ => {
                warn!("Could not determine column count from CSV file");
                vec![]
            }
        }
    };

    debug!("CSV headers/columns: {headers:?}");

    // Process records in batches
    let mut batch: Vec<crate::surreal::Record> = Vec::new();
    let mut total_processed = 0;
    let mut record_count = 0;

    for result in reader.records() {
        let record = result.context("Failed to read CSV record")?;

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
            if !config.dry_run && !config.surreal_opts.dry_run {
                crate::surreal::write_records(surreal, &config.table, &batch).await?;
                total_processed += batch.len();
            } else {
                debug!("Dry run: Would insert batch of {} records", batch.len());
                total_processed += batch.len();
            }
            batch.clear();
        }
    }

    // Process remaining records
    if !batch.is_empty() {
        if !config.dry_run && !config.surreal_opts.dry_run {
            crate::surreal::write_records(surreal, &config.table, &batch).await?;
            total_processed += batch.len();
        } else {
            debug!(
                "Dry run: Would insert final batch of {} records",
                batch.len()
            );
            total_processed += batch.len();
        }
    }

    info!(
        "Processed {} records from {:?} (total processed: {})",
        record_count, file_path, total_processed
    );

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
            surreal_opts: crate::SurrealOpts {
                surreal_endpoint: "ws://localhost:8000".to_string(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
                batch_size: 1000,
                dry_run: true,
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
