use super::Config;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use surrealdb::sql::{Id, Thing};
use tracing::{debug, info, warn};

/// Process a single CSV file
pub(super) async fn process_csv_file(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
    file_path: &PathBuf,
) -> Result<()> {
    info!("Processing file: {file_path:?}");

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
        "Processed {record_count} records from {file_path:?} (total processed: {total_processed})",
    );

    Ok(())
}
