//! Local filesystem file reader implementation

use anyhow::Result;
use std::path::PathBuf;

/// Reads a local file with configurable buffering
pub struct LocalFileReader;

impl LocalFileReader {
    /// Open a local file and return a buffered, sync-compatible reader
    ///
    /// # Arguments
    /// * `path` - Path to the file
    /// * `buffer_size` - Size of the buffer in bytes (e.g., 1MB = 1024 * 1024)
    ///
    /// # Example
    /// ```ignore
    /// let reader = LocalFileReader::open(
    ///     PathBuf::from("data.csv"),
    ///     1024 * 1024, // 1MB buffer
    /// ).await?;
    /// let csv_reader = csv::Reader::from_reader(reader);
    /// ```
    pub async fn open(path: PathBuf, buffer_size: usize) -> Result<Box<dyn std::io::Read + Send>> {
        let file = tokio::fs::File::open(&path).await?;
        let buffered = tokio::io::BufReader::with_capacity(buffer_size, file);
        let reader = tokio_util::io::SyncIoBridge::new(buffered);
        Ok(Box::new(reader))
    }
}
