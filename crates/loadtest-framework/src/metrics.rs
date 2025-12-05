//! Metrics collection for load testing.

use std::time::Duration;

/// Metrics collected during a pipeline stage.
#[derive(Debug, Clone, Default)]
pub struct StageMetrics {
    /// Duration of this stage.
    pub duration: Duration,
    /// Number of rows processed.
    pub rows_processed: u64,
    /// Number of bytes processed.
    pub bytes_processed: u64,
}

impl StageMetrics {
    /// Calculate rows per second.
    pub fn rows_per_second(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.rows_processed as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate bytes per second.
    pub fn bytes_per_second(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.bytes_processed as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

/// Metrics collected during the entire pipeline.
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    /// Population stage metrics.
    pub population: StageMetrics,
    /// Sync stage metrics.
    pub sync: StageMetrics,
    /// Verification stage metrics.
    pub verification: StageMetrics,
    /// Cleanup stage metrics.
    pub cleanup: StageMetrics,
    /// Total pipeline duration.
    pub total_duration: Duration,
}

impl PipelineMetrics {
    /// Calculate total rows processed across all stages.
    pub fn total_rows(&self) -> u64 {
        self.population.rows_processed
    }

    /// Calculate overall rows per second.
    pub fn overall_rows_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.population.rows_processed as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Get a summary of the pipeline metrics.
    pub fn summary(&self) -> String {
        format!(
            "Pipeline completed in {:?}\n\
             Population: {} rows in {:?} ({:.2} rows/sec)\n\
             Sync: {:?}\n\
             Verification: {:?}\n\
             Cleanup: {:?}",
            self.total_duration,
            self.population.rows_processed,
            self.population.duration,
            self.population.rows_per_second(),
            self.sync.duration,
            self.verification.duration,
            self.cleanup.duration,
        )
    }
}

/// Metrics for a single table.
#[derive(Debug, Clone, Default)]
pub struct TableMetrics {
    /// Table name.
    pub table_name: String,
    /// Rows populated.
    pub rows_populated: u64,
    /// Population duration.
    pub population_duration: Duration,
    /// Rows verified.
    pub rows_verified: u64,
    /// Verification duration.
    pub verification_duration: Duration,
    /// Rows matched.
    pub rows_matched: u64,
    /// Rows mismatched.
    pub rows_mismatched: u64,
    /// Rows missing.
    pub rows_missing: u64,
}

impl TableMetrics {
    /// Create new table metrics.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            ..Default::default()
        }
    }

    /// Check if verification passed for this table.
    pub fn verification_passed(&self) -> bool {
        self.rows_mismatched == 0 && self.rows_missing == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_metrics_rows_per_second() {
        let metrics = StageMetrics {
            duration: Duration::from_secs(10),
            rows_processed: 1000,
            bytes_processed: 0,
        };

        assert_eq!(metrics.rows_per_second(), 100.0);
    }

    #[test]
    fn test_stage_metrics_zero_duration() {
        let metrics = StageMetrics {
            duration: Duration::ZERO,
            rows_processed: 1000,
            bytes_processed: 0,
        };

        assert_eq!(metrics.rows_per_second(), 0.0);
    }

    #[test]
    fn test_table_metrics_verification_passed() {
        let mut metrics = TableMetrics::new("users");
        metrics.rows_verified = 100;
        metrics.rows_matched = 100;
        metrics.rows_mismatched = 0;
        metrics.rows_missing = 0;

        assert!(metrics.verification_passed());
    }

    #[test]
    fn test_table_metrics_verification_failed() {
        let mut metrics = TableMetrics::new("users");
        metrics.rows_verified = 100;
        metrics.rows_matched = 95;
        metrics.rows_mismatched = 3;
        metrics.rows_missing = 2;

        assert!(!metrics.verification_passed());
    }
}
