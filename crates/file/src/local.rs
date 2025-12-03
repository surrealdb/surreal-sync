//! Local filesystem file reader implementation

use crate::ResolvedSource;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Reads a local file with configurable buffering
pub struct LocalFileReader;

impl LocalFileReader {
    /// Open a local file and return a buffered, sync-compatible reader
    ///
    /// # Arguments
    /// * `path` - Path to the file
    /// * `buffer_size` - Size of the buffer in bytes (currently unused, reads entire file)
    ///
    /// # Example
    /// ```ignore
    /// let reader = LocalFileReader::open(
    ///     PathBuf::from("data.csv"),
    ///     1024 * 1024, // 1MB buffer
    /// ).await?;
    /// // Use reader with BufReader for line-by-line processing
    /// ```
    pub async fn open(path: PathBuf, _buffer_size: usize) -> Result<Box<dyn std::io::Read + Send>> {
        // Read the entire file into memory to avoid SyncIoBridge runtime issues
        let contents = tokio::fs::read(&path)
            .await
            .with_context(|| format!("Failed to read file: {}", path.display()))?;
        let reader = std::io::Cursor::new(contents);
        Ok(Box::new(reader))
    }
}

/// List all files in a directory (non-recursive, immediate children only)
///
/// Returns only files, not subdirectories.
pub async fn list_directory(path: &Path) -> Result<Vec<ResolvedSource>> {
    let mut results = Vec::new();

    let mut entries = tokio::fs::read_dir(path)
        .await
        .with_context(|| format!("Failed to read directory: {}", path.display()))?;

    while let Some(entry) = entries.next_entry().await? {
        let entry_path = entry.path();
        let metadata = entry
            .metadata()
            .await
            .with_context(|| format!("Failed to get metadata for: {}", entry_path.display()))?;

        // Only include files, skip directories
        if metadata.is_file() {
            results.push(ResolvedSource::Local(entry_path));
        }
    }

    // Sort for consistent ordering
    results.sort_by_key(|a| a.display_name());

    tracing::debug!(
        "Listed {} files in directory: {}",
        results.len(),
        path.display()
    );

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_open_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let mut reader = LocalFileReader::open(file_path, 1024).await.unwrap();
        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();

        assert_eq!(contents, "hello world");
    }

    #[tokio::test]
    async fn test_list_directory() {
        let temp_dir = TempDir::new().unwrap();

        // Create some test files
        std::fs::write(temp_dir.path().join("file1.csv"), "data1").unwrap();
        std::fs::write(temp_dir.path().join("file2.jsonl"), "data2").unwrap();
        std::fs::write(temp_dir.path().join("file3.txt"), "data3").unwrap();

        // Create a subdirectory (should be skipped)
        std::fs::create_dir(temp_dir.path().join("subdir")).unwrap();
        std::fs::write(temp_dir.path().join("subdir/nested.csv"), "nested").unwrap();

        let results = list_directory(temp_dir.path()).await.unwrap();

        // Should have 3 files, not the subdirectory or nested file
        assert_eq!(results.len(), 3);

        // Check extensions
        let extensions: Vec<_> = results.iter().map(|r| r.extension()).collect();
        assert!(extensions.contains(&Some("csv")));
        assert!(extensions.contains(&Some("jsonl")));
        assert!(extensions.contains(&Some("txt")));
    }

    #[tokio::test]
    async fn test_list_directory_empty() {
        let temp_dir = TempDir::new().unwrap();
        let results = list_directory(temp_dir.path()).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_list_directory_not_found() {
        let result = list_directory(Path::new("/nonexistent/path")).await;
        assert!(result.is_err());
    }
}
