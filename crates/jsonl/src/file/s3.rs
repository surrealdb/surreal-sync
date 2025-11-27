//! S3 file reader implementation

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;

/// Reads a file from S3 with configurable buffering
pub struct S3FileReader;

impl S3FileReader {
    /// Open an S3 object and return a buffered, sync-compatible reader
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `key` - S3 object key
    /// * `buffer_size` - Size of the buffer in bytes (e.g., 1MB = 1024 * 1024)
    ///
    /// # Example
    /// ```ignore
    /// let reader = S3FileReader::open(
    ///     "my-bucket".to_string(),
    ///     "data/file.jsonl".to_string(),
    ///     1024 * 1024, // 1MB buffer
    /// ).await?;
    /// // Use reader with BufReader for line-by-line processing
    /// ```
    pub async fn open(
        bucket: String,
        key: String,
        buffer_size: usize,
    ) -> Result<Box<dyn std::io::Read + Send>> {
        // Load AWS configuration
        let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&sdk_config);

        // Get object from S3
        let response = client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .context("Failed to fetch object from S3")?;

        // Convert byte stream to async read
        let stream = response.body.into_async_read();

        // Wrap in buffered reader
        let buffered = tokio::io::BufReader::with_capacity(buffer_size, stream);

        // Bridge async to sync
        let reader = tokio_util::io::SyncIoBridge::new(buffered);

        Ok(Box::new(reader))
    }
}
