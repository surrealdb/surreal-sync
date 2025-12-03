//! HTTP/HTTPS file reader implementation

use anyhow::{Context, Result};

/// Reads a file from HTTP/HTTPS
pub struct HttpFileReader;

impl HttpFileReader {
    /// Open an HTTP/HTTPS URL and return a sync-compatible reader
    ///
    /// # Arguments
    /// * `url` - HTTP or HTTPS URL to fetch
    /// * `buffer_size` - Size of the buffer in bytes (currently unused, reads entire response)
    ///
    /// # Example
    /// ```ignore
    /// let reader = HttpFileReader::open(
    ///     "https://example.com/data.csv".to_string(),
    ///     1024 * 1024, // 1MB buffer
    /// ).await?;
    /// // Use reader with BufReader for line-by-line processing
    /// ```
    pub async fn open(url: String, _buffer_size: usize) -> Result<Box<dyn std::io::Read + Send>> {
        // Create HTTP client
        let client = reqwest::Client::new();

        // Fetch the URL
        let response = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to fetch URL: {url}"))?;

        // Check for successful status
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("HTTP request failed with status {status} for URL: {url}");
        }

        // Get all bytes at once
        // This avoids the nested runtime issue with SyncIoBridge
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("Failed to read response body from: {url}"))?;

        tracing::debug!("Fetched {} bytes from: {}", bytes.len(), url);

        // Convert to a Read trait object using Cursor
        let reader = std::io::Cursor::new(bytes);

        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    // HTTP tests would require a mock server, skipping for now
    // The implementation is tested via integration tests in csv/jsonl crates
}
