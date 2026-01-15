//! Metrics types for container output and aggregation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Operation type performed by a container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Populate,
    Verify,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Populate => write!(f, "populate"),
            Operation::Verify => write!(f, "verify"),
        }
    }
}

/// Runtime environment information captured at container startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    /// Number of CPU cores visible to the container
    pub cpu_cores: usize,
    /// Total memory in MB
    pub memory_mb: u64,
    /// Available memory in MB
    pub available_memory_mb: u64,
    /// Whether tmpfs is enabled for /data
    pub tmpfs_enabled: bool,
    /// tmpfs size in MB if enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmpfs_size_mb: Option<u64>,
    /// cgroup memory limit if in container
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroup_memory_limit: Option<u64>,
    /// cgroup CPU quota if in container (as string like "200000 100000")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroup_cpu_quota: Option<String>,
    /// Filesystem type for /data mount
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_fs_type: Option<String>,
}

/// Populate operation metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PopulateMetrics {
    /// Total rows processed
    pub rows_processed: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Number of batches
    pub batch_count: u64,
    /// Rows per second
    pub rows_per_second: f64,
    /// Bytes written (estimated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
}

/// Verification result for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Table name
    pub table_name: String,
    /// Expected row count
    pub expected: u64,
    /// Found row count
    pub found: u64,
    /// Missing rows
    pub missing: u64,
    /// Mismatched rows
    pub mismatched: u64,
    /// Matched rows
    pub matched: u64,
}

/// Complete verification report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    /// Results per table
    pub tables: Vec<VerificationResult>,
    /// Total expected
    pub total_expected: u64,
    /// Total found
    pub total_found: u64,
    /// Total missing
    pub total_missing: u64,
    /// Total mismatched
    pub total_mismatched: u64,
    /// Total matched
    pub total_matched: u64,
}

/// Complete container metrics output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerMetrics {
    /// Container identifier
    pub container_id: String,
    /// Container hostname
    pub hostname: String,
    /// Start time
    pub started_at: DateTime<Utc>,
    /// Completion time
    pub completed_at: DateTime<Utc>,
    /// Runtime environment info
    pub environment: EnvironmentInfo,
    /// Tables processed
    pub tables_processed: Vec<String>,
    /// Operation type
    pub operation: Operation,
    /// Populate metrics (if operation is Populate)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<PopulateMetrics>,
    /// Verification report (if operation is Verify)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_report: Option<VerificationReport>,
    /// Errors encountered
    #[serde(default)]
    pub errors: Vec<String>,
    /// Whether the operation succeeded
    pub success: bool,
}

impl ContainerMetrics {
    /// Get duration in seconds.
    pub fn duration_secs(&self) -> f64 {
        (self.completed_at - self.started_at).num_milliseconds() as f64 / 1000.0
    }

    /// Get rows per second if metrics available.
    pub fn rows_per_second(&self) -> Option<f64> {
        self.metrics.as_ref().map(|m| m.rows_per_second)
    }

    /// Get total rows processed.
    pub fn total_rows(&self) -> u64 {
        self.metrics
            .as_ref()
            .map(|m| m.rows_processed)
            .or_else(|| self.verification_report.as_ref().map(|v| v.total_found))
            .unwrap_or(0)
    }
}

/// Aggregated results from all containers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedReport {
    /// Total number of containers
    pub total_containers: usize,
    /// Successfully completed containers
    pub completed_containers: usize,
    /// Failed containers
    pub failed_containers: usize,
    /// Total rows populated
    pub total_rows_populated: u64,
    /// Total rows verified
    pub total_rows_verified: u64,
    /// Wall clock duration (max container duration)
    pub wall_clock_duration_secs: f64,
    /// Aggregate throughput (sum of all containers)
    pub aggregate_rows_per_second: f64,
    /// Verification summary if any containers did verification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<VerificationSummary>,
    /// Individual container reports
    pub containers: Vec<ContainerMetrics>,
    /// Aggregation timestamp
    pub aggregated_at: DateTime<Utc>,
}

/// Summary of verification across all containers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationSummary {
    pub total_expected: u64,
    pub total_found: u64,
    pub total_missing: u64,
    pub total_mismatched: u64,
    pub total_matched: u64,
    pub pass_rate: f64,
}

/// Builder for ContainerMetrics to capture start/stop state.
pub struct ContainerMetricsBuilder {
    container_id: String,
    hostname: String,
    started_at: DateTime<Utc>,
    environment: EnvironmentInfo,
    tables_processed: Vec<String>,
    operation: Operation,
}

impl ContainerMetricsBuilder {
    /// Create a new builder and capture the container start state.
    ///
    /// This captures:
    /// - Current timestamp (system time within the container)
    /// - Environment info (CPU, memory, tmpfs, cgroup limits)
    /// - Container ID and hostname
    /// - Tables being processed
    /// - Operation type (Populate or Verify)
    pub fn start(operation: Operation, tables: Vec<String>) -> anyhow::Result<Self> {
        let container_id = std::env::var("CONTAINER_ID")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| format!("{}-{}", operation, uuid::Uuid::new_v4()));

        let hostname = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());

        let started_at = Utc::now();
        let environment = collect_environment_info()?;

        Ok(Self {
            container_id,
            hostname,
            started_at,
            environment,
            tables_processed: tables,
            operation,
        })
    }

    /// Complete the metrics with populate results.
    pub fn finish_populate(
        self,
        metrics: Option<PopulateMetrics>,
        errors: Vec<String>,
        success: bool,
    ) -> ContainerMetrics {
        ContainerMetrics {
            container_id: self.container_id,
            hostname: self.hostname,
            started_at: self.started_at,
            completed_at: Utc::now(),
            environment: self.environment,
            tables_processed: self.tables_processed,
            operation: self.operation,
            metrics,
            verification_report: None,
            errors,
            success,
        }
    }

    /// Complete the metrics with verification results.
    pub fn finish_verify(
        self,
        verification_report: Option<VerificationReport>,
        errors: Vec<String>,
        success: bool,
    ) -> ContainerMetrics {
        ContainerMetrics {
            container_id: self.container_id,
            hostname: self.hostname,
            started_at: self.started_at,
            completed_at: Utc::now(),
            environment: self.environment,
            tables_processed: self.tables_processed,
            operation: self.operation,
            metrics: None,
            verification_report,
            errors,
            success,
        }
    }

    /// Create dummy metrics for dry-run mode.
    pub fn finish_dry_run(self) -> ContainerMetrics {
        let metrics = match self.operation {
            Operation::Populate => Some(PopulateMetrics {
                rows_processed: 0,
                duration_ms: 0,
                batch_count: 0,
                rows_per_second: 0.0,
                bytes_written: None,
            }),
            Operation::Verify => None,
        };

        let verification_report = match self.operation {
            Operation::Verify => Some(VerificationReport {
                tables: vec![],
                total_expected: 0,
                total_found: 0,
                total_missing: 0,
                total_mismatched: 0,
                total_matched: 0,
            }),
            Operation::Populate => None,
        };

        ContainerMetrics {
            container_id: self.container_id,
            hostname: self.hostname,
            started_at: self.started_at,
            completed_at: Utc::now(),
            environment: self.environment,
            tables_processed: self.tables_processed,
            operation: self.operation,
            metrics,
            verification_report,
            errors: vec![],
            success: true,
        }
    }
}

/// Collect runtime environment information.
///
/// This captures:
/// - CPU cores visible to the container
/// - Memory (total and available)
/// - tmpfs configuration for /data mount
/// - cgroup limits (memory and CPU quota)
/// - Filesystem type for /data
fn collect_environment_info() -> anyhow::Result<EnvironmentInfo> {
    use anyhow::Context;
    use std::fs;

    // CPU cores
    let cpu_cores = num_cpus::get();

    // Memory info
    let meminfo = fs::read_to_string("/proc/meminfo").context("Failed to read /proc/meminfo")?;
    let (memory_mb, available_memory_mb) = parse_meminfo(&meminfo)?;

    // Check tmpfs for /data
    let mounts = fs::read_to_string("/proc/mounts").context("Failed to read /proc/mounts")?;
    let (tmpfs_enabled, tmpfs_size_mb, data_fs_type) = parse_mounts(&mounts);

    // cgroup memory limit
    let cgroup_memory_limit = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&limit| limit < (1u64 << 60)); // Filter out "unlimited" values

    // cgroup CPU quota
    let cgroup_cpu_quota = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()
        .and_then(|quota_str| {
            fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
                .ok()
                .map(|period_str| format!("{} {}", quota_str.trim(), period_str.trim()))
        });

    Ok(EnvironmentInfo {
        cpu_cores,
        memory_mb,
        available_memory_mb,
        tmpfs_enabled,
        tmpfs_size_mb,
        cgroup_memory_limit,
        cgroup_cpu_quota,
        data_fs_type,
    })
}

/// Parse /proc/meminfo to extract total and available memory in MB.
fn parse_meminfo(meminfo: &str) -> anyhow::Result<(u64, u64)> {
    let mut total_kb = None;
    let mut available_kb = None;

    for line in meminfo.lines() {
        if line.starts_with("MemTotal:") {
            total_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u64>().ok());
        } else if line.starts_with("MemAvailable:") {
            available_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u64>().ok());
        }
    }

    let total_mb =
        total_kb.ok_or_else(|| anyhow::anyhow!("MemTotal not found in /proc/meminfo"))? / 1024;
    let available_mb = available_kb
        .ok_or_else(|| anyhow::anyhow!("MemAvailable not found in /proc/meminfo"))?
        / 1024;

    Ok((total_mb, available_mb))
}

/// Parse /proc/mounts to detect tmpfs configuration and filesystem type for /data.
fn parse_mounts(mounts: &str) -> (bool, Option<u64>, Option<String>) {
    for line in mounts.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3 && parts[1] == "/data" {
            let fs_type = parts[2].to_string();
            if fs_type == "tmpfs" {
                // Extract size from options (e.g., "rw,nosuid,nodev,size=1048576k")
                let size_mb = parts.get(3).and_then(|opts| {
                    opts.split(',')
                        .find(|opt| opt.starts_with("size="))
                        .and_then(|size_opt| {
                            let size_str = size_opt.trim_start_matches("size=");
                            parse_size_to_mb(size_str)
                        })
                });
                return (true, size_mb, Some(fs_type));
            } else {
                return (false, None, Some(fs_type));
            }
        }
    }
    (false, None, None)
}

/// Parse size string (e.g., "1048576k", "1024M", "1G") to MB.
fn parse_size_to_mb(size_str: &str) -> Option<u64> {
    let size_str = size_str.trim();
    if size_str.is_empty() {
        return None;
    }

    let (num_str, unit) = if size_str.ends_with('k') || size_str.ends_with('K') {
        (&size_str[..size_str.len() - 1], "k")
    } else if size_str.ends_with('m') || size_str.ends_with('M') {
        (&size_str[..size_str.len() - 1], "M")
    } else if size_str.ends_with('g') || size_str.ends_with('G') {
        (&size_str[..size_str.len() - 1], "G")
    } else {
        (size_str, "b")
    };

    let num = num_str.parse::<u64>().ok()?;
    let mb = match unit {
        "k" => num / 1024,
        "M" => num,
        "G" => num * 1024,
        "b" => num / (1024 * 1024),
        _ => return None,
    };

    Some(mb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_metrics_builder_populate() {
        let builder = ContainerMetricsBuilder::start(
            Operation::Populate,
            vec!["users".to_string(), "posts".to_string()],
        )
        .expect("Failed to create builder");

        let metrics = PopulateMetrics {
            rows_processed: 1000,
            duration_ms: 5000,
            batch_count: 10,
            rows_per_second: 200.0,
            bytes_written: Some(1024000),
        };

        let container_metrics = builder.finish_populate(Some(metrics), vec![], true);

        assert_eq!(container_metrics.operation, Operation::Populate);
        assert_eq!(container_metrics.tables_processed.len(), 2);
        assert_eq!(container_metrics.tables_processed[0], "users");
        assert_eq!(container_metrics.tables_processed[1], "posts");
        assert!(container_metrics.metrics.is_some());
        assert!(container_metrics.verification_report.is_none());
        assert!(container_metrics.success);
        assert!(container_metrics.errors.is_empty());
        assert!(container_metrics.completed_at >= container_metrics.started_at);
    }

    #[test]
    fn test_container_metrics_builder_verify() {
        let builder = ContainerMetricsBuilder::start(Operation::Verify, vec!["users".to_string()])
            .expect("Failed to create builder");

        let report = VerificationReport {
            tables: vec![],
            total_expected: 1000,
            total_found: 1000,
            total_missing: 0,
            total_mismatched: 0,
            total_matched: 1000,
        };

        let container_metrics = builder.finish_verify(Some(report), vec![], true);

        assert_eq!(container_metrics.operation, Operation::Verify);
        assert_eq!(container_metrics.tables_processed.len(), 1);
        assert!(container_metrics.verification_report.is_some());
        assert!(container_metrics.metrics.is_none());
        assert!(container_metrics.success);
    }

    #[test]
    fn test_container_metrics_builder_dry_run_populate() {
        let builder =
            ContainerMetricsBuilder::start(Operation::Populate, vec!["test_table".to_string()])
                .expect("Failed to create builder");

        let container_metrics = builder.finish_dry_run();

        assert_eq!(container_metrics.operation, Operation::Populate);
        assert!(container_metrics.metrics.is_some());
        assert!(container_metrics.verification_report.is_none());
        assert!(container_metrics.success);
        assert_eq!(
            container_metrics.metrics.as_ref().unwrap().rows_processed,
            0
        );
    }

    #[test]
    fn test_container_metrics_builder_dry_run_verify() {
        let builder =
            ContainerMetricsBuilder::start(Operation::Verify, vec!["test_table".to_string()])
                .expect("Failed to create builder");

        let container_metrics = builder.finish_dry_run();

        assert_eq!(container_metrics.operation, Operation::Verify);
        assert!(container_metrics.verification_report.is_some());
        assert!(container_metrics.metrics.is_none());
        assert!(container_metrics.success);
        assert_eq!(
            container_metrics
                .verification_report
                .as_ref()
                .unwrap()
                .total_matched,
            0
        );
    }

    #[test]
    fn test_parse_size_to_mb() {
        assert_eq!(parse_size_to_mb("1024k"), Some(1));
        assert_eq!(parse_size_to_mb("1024K"), Some(1));
        assert_eq!(parse_size_to_mb("100M"), Some(100));
        assert_eq!(parse_size_to_mb("100m"), Some(100));
        assert_eq!(parse_size_to_mb("2G"), Some(2048));
        assert_eq!(parse_size_to_mb("2g"), Some(2048));
        assert_eq!(parse_size_to_mb("1048576"), Some(1)); // bytes
        assert_eq!(parse_size_to_mb(""), None);
    }

    #[test]
    fn test_parse_meminfo() {
        let meminfo = "MemTotal:       16384000 kB\n\
                       MemFree:         8192000 kB\n\
                       MemAvailable:    10240000 kB\n";

        let (total_mb, available_mb) = parse_meminfo(meminfo).expect("Failed to parse meminfo");
        assert_eq!(total_mb, 16000); // 16384000 / 1024
        assert_eq!(available_mb, 10000); // 10240000 / 1024
    }

    #[test]
    fn test_parse_mounts_tmpfs() {
        let mounts = "tmpfs /data tmpfs rw,nosuid,nodev,size=1048576k 0 0\n\
                      /dev/sda1 / ext4 rw,relatime 0 0\n";

        let (tmpfs_enabled, tmpfs_size_mb, data_fs_type) = parse_mounts(mounts);
        assert!(tmpfs_enabled);
        assert_eq!(tmpfs_size_mb, Some(1024)); // 1048576k = 1024M
        assert_eq!(data_fs_type, Some("tmpfs".to_string()));
    }

    #[test]
    fn test_parse_mounts_ext4() {
        let mounts = "/dev/sda1 /data ext4 rw,relatime 0 0\n\
                      tmpfs /tmp tmpfs rw,nosuid,nodev 0 0\n";

        let (tmpfs_enabled, tmpfs_size_mb, data_fs_type) = parse_mounts(mounts);
        assert!(!tmpfs_enabled);
        assert_eq!(tmpfs_size_mb, None);
        assert_eq!(data_fs_type, Some("ext4".to_string()));
    }
}
