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
