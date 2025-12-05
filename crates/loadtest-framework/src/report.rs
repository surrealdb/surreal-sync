//! Load test report types.

use crate::metrics::{PipelineMetrics, TableMetrics};

/// Result of a load test run.
#[derive(Debug, Clone)]
pub struct LoadTestReport {
    /// Test configuration summary.
    pub config_summary: ConfigSummary,
    /// Per-table metrics.
    pub table_metrics: Vec<TableMetrics>,
    /// Overall pipeline metrics.
    pub pipeline_metrics: PipelineMetrics,
    /// Overall test status.
    pub status: TestStatus,
    /// Error messages (if any).
    pub errors: Vec<String>,
}

impl LoadTestReport {
    /// Create a new report.
    pub fn new(config_summary: ConfigSummary) -> Self {
        Self {
            config_summary,
            table_metrics: Vec::new(),
            pipeline_metrics: PipelineMetrics::default(),
            status: TestStatus::Pending,
            errors: Vec::new(),
        }
    }

    /// Check if the test passed.
    pub fn passed(&self) -> bool {
        matches!(self.status, TestStatus::Passed)
    }

    /// Get the total number of rows tested.
    pub fn total_rows(&self) -> u64 {
        self.table_metrics.iter().map(|t| t.rows_verified).sum()
    }

    /// Get the total number of rows matched.
    pub fn total_matched(&self) -> u64 {
        self.table_metrics.iter().map(|t| t.rows_matched).sum()
    }

    /// Get the total number of rows mismatched.
    pub fn total_mismatched(&self) -> u64 {
        self.table_metrics.iter().map(|t| t.rows_mismatched).sum()
    }

    /// Get the total number of rows missing.
    pub fn total_missing(&self) -> u64 {
        self.table_metrics.iter().map(|t| t.rows_missing).sum()
    }

    /// Generate a summary string.
    pub fn summary(&self) -> String {
        let status_str = match self.status {
            TestStatus::Pending => "PENDING",
            TestStatus::Running => "RUNNING",
            TestStatus::Passed => "PASSED",
            TestStatus::Failed => "FAILED",
            TestStatus::Error => "ERROR",
        };

        let mut summary = format!(
            "Load Test Report: {}\n\
             ================\n\
             Source: {}\n\
             Seed: {}\n\
             Row Count: {}\n\
             Tables: {}\n\n",
            status_str,
            self.config_summary.source_type,
            self.config_summary.seed,
            self.config_summary.row_count,
            self.config_summary.tables.join(", ")
        );

        summary.push_str(&format!(
            "Results:\n\
             - Total Rows: {}\n\
             - Matched: {}\n\
             - Mismatched: {}\n\
             - Missing: {}\n\n",
            self.total_rows(),
            self.total_matched(),
            self.total_mismatched(),
            self.total_missing()
        ));

        summary.push_str(&format!(
            "Timing:\n\
             - Total Duration: {:?}\n\
             - Population: {:?}\n\
             - Sync: {:?}\n\
             - Verification: {:?}\n",
            self.pipeline_metrics.total_duration,
            self.pipeline_metrics.population.duration,
            self.pipeline_metrics.sync.duration,
            self.pipeline_metrics.verification.duration
        ));

        if !self.errors.is_empty() {
            summary.push_str("\nErrors:\n");
            for error in &self.errors {
                summary.push_str(&format!("- {error}\n"));
            }
        }

        summary
    }
}

/// Summary of test configuration.
#[derive(Debug, Clone)]
pub struct ConfigSummary {
    /// Source type name.
    pub source_type: String,
    /// Random seed.
    pub seed: u64,
    /// Row count per table.
    pub row_count: u64,
    /// Tables being tested.
    pub tables: Vec<String>,
    /// Whether verification is enabled.
    pub verify: bool,
}

/// Overall test status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStatus {
    /// Test has not started.
    Pending,
    /// Test is running.
    Running,
    /// Test completed successfully.
    Passed,
    /// Test completed with verification failures.
    Failed,
    /// Test encountered an error.
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::TableMetrics;
    use std::time::Duration;

    fn test_config_summary() -> ConfigSummary {
        ConfigSummary {
            source_type: "postgresql".to_string(),
            seed: 42,
            row_count: 1000,
            tables: vec!["users".to_string(), "orders".to_string()],
            verify: true,
        }
    }

    #[test]
    fn test_report_passed() {
        let mut report = LoadTestReport::new(test_config_summary());
        report.status = TestStatus::Passed;

        assert!(report.passed());
    }

    #[test]
    fn test_report_failed() {
        let mut report = LoadTestReport::new(test_config_summary());
        report.status = TestStatus::Failed;

        assert!(!report.passed());
    }

    #[test]
    fn test_report_totals() {
        let mut report = LoadTestReport::new(test_config_summary());

        let mut users = TableMetrics::new("users");
        users.rows_verified = 100;
        users.rows_matched = 95;
        users.rows_mismatched = 3;
        users.rows_missing = 2;

        let mut orders = TableMetrics::new("orders");
        orders.rows_verified = 200;
        orders.rows_matched = 198;
        orders.rows_mismatched = 1;
        orders.rows_missing = 1;

        report.table_metrics = vec![users, orders];

        assert_eq!(report.total_rows(), 300);
        assert_eq!(report.total_matched(), 293);
        assert_eq!(report.total_mismatched(), 4);
        assert_eq!(report.total_missing(), 3);
    }

    #[test]
    fn test_report_summary() {
        let mut report = LoadTestReport::new(test_config_summary());
        report.status = TestStatus::Passed;
        report.pipeline_metrics.total_duration = Duration::from_secs(10);

        let summary = report.summary();
        assert!(summary.contains("PASSED"));
        assert!(summary.contains("postgresql"));
        assert!(summary.contains("42"));
    }
}
