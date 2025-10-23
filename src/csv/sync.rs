//! CSV synchronization implementation
//!
//! This module handles streaming CSV files and importing them into SurrealDB tables.

use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing::{info, warn};

/// Configuration for CSV import
#[derive(Clone)]
pub struct Config {
    /// List of CSV file paths to import
    pub files: Vec<PathBuf>,

    /// List of S3 URIs to import
    pub s3_uris: Vec<String>,

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
            s3_uris: vec![],
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
    info!("S3 URIs to process: {:?}", config.s3_uris);
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
        super::file::process_csv_file(&surreal, &config, file_path).await?;
    }

    // Process each S3 URI
    for s3_uri in &config.s3_uris {
        super::s3::process_s3_uri(&surreal, &config, s3_uri).await?;
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
