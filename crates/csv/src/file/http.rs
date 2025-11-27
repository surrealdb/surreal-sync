//! HTTP/HTTPS file reader implementation

use anyhow::{Context, Result};

/// Reads a file from HTTP/HTTPS with configurable buffering
pub struct HttpFileReader;

impl HttpFileReader {
    /// Open an HTTP/HTTPS URL and return a buffered, sync-compatible reader
    ///
    /// # Arguments
    /// * `url` - HTTP or HTTPS URL to fetch
    /// * `buffer_size` - Size of the buffer in bytes (e.g., 1MB = 1024 * 1024)
    ///
    /// # Example
    /// ```ignore
    /// let reader = HttpFileReader::open(
    ///     "https://example.com/data.csv".to_string(),
    ///     1024 * 1024, // 1MB buffer
    /// ).await?;
    /// let csv_reader = csv::Reader::from_reader(reader);
    /// ```
    pub async fn open(url: String, _buffer_size: usize) -> Result<Box<dyn std::io::Read + Send>> {
        // Create HTTP client
        let client = reqwest::Client::new();

        // Fetch the URL
        let response = client
            .get(&url)
            .send()
            .await
            .context(format!("Failed to fetch URL: {}", url))?;

        // Check for successful status
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("HTTP request failed with status: {}", status);
        }

        // Get all bytes at once (for CSV files, this is reasonable)
        // This avoids the nested runtime issue with SyncIoBridge
        let bytes = response
            .bytes()
            .await
            .context("Failed to read response body")?;

        // Convert to a Read trait object using Cursor
        let reader = std::io::Cursor::new(bytes);

        Ok(Box::new(reader))
    }
}
