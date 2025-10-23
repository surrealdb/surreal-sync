use super::Config;
use anyhow::{Context, Result};
use std::collections::HashMap;
use surrealdb::sql::{Id, Thing};
use tokio::io::AsyncReadExt;
use tracing::{debug, info, warn};

/// Parse S3 URI in the format: s3://bucket/key/to/file.csv
fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let uri = uri
        .strip_prefix("s3://")
        .context("S3 URI must start with 's3://'")?;

    let parts: Vec<&str> = uri.splitn(2, '/').collect();
    if parts.len() != 2 {
        anyhow::bail!("S3 URI must be in format 's3://bucket/key/to/file'");
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Process a single CSV file from S3
pub(super) async fn process_s3_uri(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
    s3_uri: &String,
) -> Result<()> {
    info!("Processing S3 URI: {s3_uri}");

    // Parse the S3 URI
    let (bucket, key) = parse_s3_uri(s3_uri).context("Failed to parse S3 URI")?;

    info!("Bucket: {bucket}, Key: {key}");

    // Create AWS S3 client
    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&sdk_config);

    // Get the object from S3
    let response = s3_client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .context("Failed to fetch object from S3")?;

    // Read the entire body into memory
    let mut body_bytes = Vec::new();
    let mut stream = response.body.into_async_read();
    stream
        .read_to_end(&mut body_bytes)
        .await
        .context("Failed to read S3 object body")?;

    // Create CSV reader from the bytes
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(config.has_headers)
        .delimiter(config.delimiter)
        .from_reader(body_bytes.as_slice());

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
                warn!("Could not determine column count from S3 CSV file");
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
        "Processed {} records from {s3_uri} (total processed: {})",
        record_count, total_processed
    );

    Ok(())
}
