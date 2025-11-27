//! Metrics collection for CSV import operations

use anyhow::Result;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

/// Metrics collector for CSV import operations
#[derive(Clone)]
pub struct MetricsCollector {
    rows_processed: Arc<AtomicU64>,
    bytes_read: Arc<AtomicU64>,
    start_time: Instant,
    output_path: PathBuf,
}

/// Single metrics entry (one JSON line)
#[derive(Debug, Serialize, serde::Deserialize)]
pub struct MetricsEntry {
    pub timestamp: String,
    pub rows_processed: u64,
    pub bytes_read: u64,
    pub throughput_rows_per_sec: f64,
    pub throughput_mb_per_sec: f64,
    pub memory_mb: u64,
    pub cpu_percent: f64,
    pub elapsed_secs: f64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(output_path: PathBuf) -> Self {
        Self {
            rows_processed: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
            output_path,
        }
    }

    /// Increment rows processed counter
    pub fn add_rows(&self, count: u64) {
        self.rows_processed.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment bytes read counter
    #[allow(dead_code)]
    pub fn add_bytes(&self, count: u64) {
        self.bytes_read.fetch_add(count, Ordering::Relaxed);
    }

    /// Get current metrics snapshot
    fn snapshot(&self) -> MetricsEntry {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let rows = self.rows_processed.load(Ordering::Relaxed);
        let bytes = self.bytes_read.load(Ordering::Relaxed);

        let throughput_rows = if elapsed > 0.0 {
            rows as f64 / elapsed
        } else {
            0.0
        };

        let throughput_mb = if elapsed > 0.0 {
            (bytes as f64 / 1_000_000.0) / elapsed
        } else {
            0.0
        };

        // Get system metrics
        let (memory_mb, cpu_percent) = get_process_stats();

        MetricsEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            rows_processed: rows,
            bytes_read: bytes,
            throughput_rows_per_sec: throughput_rows,
            throughput_mb_per_sec: throughput_mb,
            memory_mb,
            cpu_percent,
            elapsed_secs: elapsed,
        }
    }

    /// Start background metrics emission task
    pub fn start_emission_task(&self, interval: Duration) -> tokio::task::JoinHandle<Result<()>> {
        let collector = self.clone();

        tokio::spawn(async move {
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&collector.output_path)
                .await?;

            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                let metrics = collector.snapshot();
                let json_line = serde_json::to_string(&metrics)?;
                file.write_all(json_line.as_bytes()).await?;
                file.write_all(b"\n").await?;
                file.flush().await?;
            }
        })
    }
}

/// Get current process memory and CPU usage
fn get_process_stats() -> (u64, f64) {
    // Simple implementation using /proc/self/status on Linux
    // For more robust cross-platform support, we'd use sysinfo crate

    // Read memory from /proc/self/status
    let memory_mb = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|kb| kb.parse::<u64>().ok())
                        .map(|kb| kb / 1024) // Convert KB to MB
                })
        })
        .unwrap_or(0);

    // For CPU, we'd need more complex tracking or use sysinfo crate
    // For now, return placeholder
    let cpu_percent = 0.0;

    (memory_mb, cpu_percent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[test]
    fn test_metrics_collector_new() {
        let path = PathBuf::from("/tmp/test_metrics.json");
        let collector = MetricsCollector::new(path.clone());

        assert_eq!(collector.rows_processed.load(Ordering::Relaxed), 0);
        assert_eq!(collector.bytes_read.load(Ordering::Relaxed), 0);
        assert_eq!(collector.output_path, path);
    }

    #[test]
    fn test_add_rows() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));

        collector.add_rows(100);
        assert_eq!(collector.rows_processed.load(Ordering::Relaxed), 100);

        collector.add_rows(50);
        assert_eq!(collector.rows_processed.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_add_bytes() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));

        collector.add_bytes(1024);
        assert_eq!(collector.bytes_read.load(Ordering::Relaxed), 1024);

        collector.add_bytes(2048);
        assert_eq!(collector.bytes_read.load(Ordering::Relaxed), 3072);
    }

    #[test]
    fn test_snapshot_initial_state() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));

        let snapshot = collector.snapshot();

        assert_eq!(snapshot.rows_processed, 0);
        assert_eq!(snapshot.bytes_read, 0);
        assert!(snapshot.elapsed_secs >= 0.0);
        assert_eq!(snapshot.throughput_rows_per_sec, 0.0);
        assert_eq!(snapshot.throughput_mb_per_sec, 0.0);
    }

    #[test]
    fn test_snapshot_with_data() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));

        collector.add_rows(1000);
        collector.add_bytes(1_000_000);

        // Small delay to ensure elapsed time > 0
        std::thread::sleep(Duration::from_millis(10));

        let snapshot = collector.snapshot();

        assert_eq!(snapshot.rows_processed, 1000);
        assert_eq!(snapshot.bytes_read, 1_000_000);
        assert!(snapshot.elapsed_secs > 0.0);
        assert!(snapshot.throughput_rows_per_sec > 0.0);
        assert!(snapshot.throughput_mb_per_sec > 0.0);
    }

    #[test]
    fn test_snapshot_timestamp_format() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));
        let snapshot = collector.snapshot();

        // Verify timestamp is in RFC3339 format
        chrono::DateTime::parse_from_rfc3339(&snapshot.timestamp)
            .expect("Timestamp should be valid RFC3339");
    }

    #[test]
    fn test_metrics_entry_serialization() {
        let entry = MetricsEntry {
            timestamp: "2024-01-15T10:30:00Z".to_string(),
            rows_processed: 1000,
            bytes_read: 2048,
            throughput_rows_per_sec: 100.5,
            throughput_mb_per_sec: 1.5,
            memory_mb: 256,
            cpu_percent: 25.0,
            elapsed_secs: 10.0,
        };

        let json = serde_json::to_string(&entry).expect("Should serialize");

        assert!(json.contains("\"rows_processed\":1000"));
        assert!(json.contains("\"bytes_read\":2048"));
        assert!(json.contains("\"memory_mb\":256"));
    }

    #[test]
    fn test_collector_clone() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));
        collector.add_rows(100);

        let cloned = collector.clone();
        cloned.add_rows(50);

        // Both should see the same counter value (Arc shared)
        assert_eq!(collector.rows_processed.load(Ordering::Relaxed), 150);
        assert_eq!(cloned.rows_processed.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_concurrent_updates() {
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));
        let collector_clone = collector.clone();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let c = collector_clone.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        c.add_rows(1);
                        c.add_bytes(10);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(collector.rows_processed.load(Ordering::Relaxed), 1000);
        assert_eq!(collector.bytes_read.load(Ordering::Relaxed), 10000);
    }

    #[tokio::test]
    async fn test_emission_task_creates_file() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        let collector = MetricsCollector::new(path.clone());
        collector.add_rows(100);
        collector.add_bytes(1024);

        let handle = collector.start_emission_task(Duration::from_millis(50));

        // Wait for at least one emission
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the task
        handle.abort();

        // Verify file has content
        let content = tokio::fs::read_to_string(&path)
            .await
            .expect("Should read file");
        assert!(!content.is_empty(), "File should have metrics data");

        // Parse the first line as JSON
        let first_line = content
            .lines()
            .next()
            .expect("Should have at least one line");
        let entry: MetricsEntry =
            serde_json::from_str(first_line).expect("Should parse as MetricsEntry");

        assert_eq!(entry.rows_processed, 100);
        assert_eq!(entry.bytes_read, 1024);
    }

    #[tokio::test]
    async fn test_emission_task_multiple_entries() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        let collector = MetricsCollector::new(path.clone());

        let handle = collector.start_emission_task(Duration::from_millis(30));

        // Wait for multiple emissions
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the task
        handle.abort();

        // Verify file has multiple lines
        let content = tokio::fs::read_to_string(&path)
            .await
            .expect("Should read file");
        let line_count = content.lines().count();

        assert!(
            line_count >= 2,
            "Should have at least 2 metrics entries, got {line_count}"
        );
    }

    #[test]
    fn test_get_process_stats() {
        let (memory_mb, cpu_percent) = get_process_stats();

        // Memory should be non-negative (may be 0 on non-Linux systems)
        assert!(memory_mb < u64::MAX);

        // CPU is currently a placeholder returning 0.0
        assert_eq!(cpu_percent, 0.0);
    }

    #[test]
    fn test_throughput_calculation_zero_elapsed() {
        // Create collector and immediately take snapshot
        let collector = MetricsCollector::new(PathBuf::from("/tmp/test.json"));
        collector.add_rows(1000);
        collector.add_bytes(1_000_000);

        let snapshot = collector.snapshot();

        // Throughput should be valid even with very small elapsed time
        assert!(snapshot.throughput_rows_per_sec.is_finite());
        assert!(snapshot.throughput_mb_per_sec.is_finite());
    }
}
