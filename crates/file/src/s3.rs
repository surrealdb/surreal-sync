//! S3 file reader implementation with prefix listing support

use crate::ResolvedSource;
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;

/// Shared S3 client for efficient operations
///
/// Creating an S3 client is relatively expensive, so this struct allows
/// reusing the client across multiple operations.
pub struct S3Client {
    client: aws_sdk_s3::Client,
}

impl S3Client {
    /// Create a new S3 client from AWS config
    pub async fn new() -> Result<Self> {
        let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&sdk_config);
        Ok(Self { client })
    }

    /// List all objects under a prefix (non-recursive, immediate level only)
    ///
    /// Note: S3 doesn't have true directories, so this lists all objects
    /// that start with the prefix but filters out "subdirectory" entries
    /// by excluding keys that have additional `/` characters after the prefix.
    pub async fn list_prefix(&self, bucket: &str, prefix: &str) -> Result<Vec<ResolvedSource>> {
        let mut results = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix)
                .delimiter("/"); // Use delimiter to get only immediate children

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .with_context(|| format!("Failed to list S3 prefix: s3://{bucket}/{prefix}"))?;

            // Process objects (files at this level)
            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        // Skip the prefix itself if it appears as an object
                        if key == prefix {
                            continue;
                        }

                        // Skip "directory" markers (keys ending with /)
                        if key.ends_with('/') {
                            continue;
                        }

                        results.push(ResolvedSource::S3 {
                            bucket: bucket.to_string(),
                            key,
                        });
                    }
                }
            }

            // Handle pagination
            if response.is_truncated == Some(true) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        // Sort for consistent ordering
        results.sort_by_key(|a| a.display_name());

        tracing::debug!(
            "Listed {} objects in S3 prefix: s3://{}/{}",
            results.len(),
            bucket,
            prefix
        );

        Ok(results)
    }

    /// Open an S3 object for reading
    pub async fn open(
        &self,
        bucket: &str,
        key: &str,
        buffer_size: usize,
    ) -> Result<Box<dyn std::io::Read + Send>> {
        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("Failed to fetch object from S3: s3://{bucket}/{key}"))?;

        // Convert byte stream to async read
        let stream = response.body.into_async_read();

        // Wrap in buffered reader
        let buffered = tokio::io::BufReader::with_capacity(buffer_size, stream);

        // Bridge async to sync
        let reader = tokio_util::io::SyncIoBridge::new(buffered);

        Ok(Box::new(reader))
    }
}

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
    ///     "data/file.csv".to_string(),
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
            .with_context(|| format!("Failed to fetch object from S3: s3://{bucket}/{key}"))?;

        // Convert byte stream to async read
        let stream = response.body.into_async_read();

        // Wrap in buffered reader
        let buffered = tokio::io::BufReader::with_capacity(buffer_size, stream);

        // Bridge async to sync
        let reader = tokio_util::io::SyncIoBridge::new(buffered);

        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_source_s3_extension() {
        let source = ResolvedSource::S3 {
            bucket: "bucket".to_string(),
            key: "path/to/file.csv".to_string(),
        };
        assert_eq!(source.extension(), Some("csv"));
    }

    #[test]
    fn test_resolved_source_s3_no_extension() {
        let source = ResolvedSource::S3 {
            bucket: "bucket".to_string(),
            key: "path/to/file".to_string(),
        };
        // For a file without extension, rsplit('.') returns the whole filename
        // which contains '/' so it's filtered out, returning None
        // This is correct behavior - no extension means None
        assert_eq!(source.extension(), None);
    }

    // Integration tests for S3 operations would require AWS credentials
    // or mocking, which is beyond the scope of unit tests
}
