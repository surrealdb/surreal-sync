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
    ///     PathBuf::from("data.jsonl"),
    ///     1024 * 1024, // 1MB buffer
    /// ).await?;
    /// // Use reader with BufReader for line-by-line processing
    /// ```
    pub async fn open(path: PathBuf, _buffer_size: usize) -> Result<Box<dyn std::io::Read + Send>> {
        // Read the entire file into memory to avoid SyncIoBridge runtime issues
        let contents = tokio::fs::read(&path).await?;
        let reader = std::io::Cursor::new(contents);
        Ok(Box::new(reader))
    }
}
