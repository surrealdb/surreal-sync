//! Verification report types.

use std::time::Duration;

/// Information about a field mismatch.
#[derive(Debug, Clone)]
pub struct FieldMismatch {
    /// Field name.
    pub field: String,
    /// Expected value.
    pub expected: String,
    /// Actual value.
    pub actual: String,
}

/// Information about a mismatched row.
#[derive(Debug, Clone)]
pub struct MismatchInfo {
    /// Record ID in SurrealDB.
    pub record_id: String,
    /// Row index in the generated data.
    pub index: u64,
    /// Field mismatches.
    pub field_mismatches: Vec<FieldMismatch>,
}

/// Information about a missing row.
#[derive(Debug, Clone)]
pub struct MissingInfo {
    /// Expected record ID.
    pub expected_id: String,
    /// Row index in the generated data.
    pub index: u64,
}

/// Verification report.
#[derive(Debug, Clone, Default)]
pub struct VerificationReport {
    /// Total number of rows expected.
    pub expected: u64,
    /// Number of rows found.
    pub found: u64,
    /// Number of rows missing.
    pub missing: u64,
    /// Number of rows with mismatched data.
    pub mismatched: u64,
    /// Number of rows that matched exactly.
    pub matched: u64,
    /// Details of missing rows.
    pub missing_rows: Vec<MissingInfo>,
    /// Details of mismatched rows.
    pub mismatched_rows: Vec<MismatchInfo>,
    /// Total verification time.
    pub total_duration: Duration,
    /// Time spent generating expected data.
    pub generation_duration: Duration,
    /// Time spent querying SurrealDB.
    pub query_duration: Duration,
    /// Time spent comparing data.
    pub compare_duration: Duration,
}

impl VerificationReport {
    /// Check if verification passed.
    pub fn is_success(&self) -> bool {
        self.missing == 0 && self.mismatched == 0
    }

    /// Calculate verification rate (rows per second).
    pub fn rows_per_second(&self) -> f64 {
        if self.total_duration.as_secs_f64() > 0.0 {
            self.expected as f64 / self.total_duration.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Get a summary string.
    pub fn summary(&self) -> String {
        if self.is_success() {
            format!(
                "Verification PASSED: {}/{} rows matched in {:?}",
                self.matched, self.expected, self.total_duration
            )
        } else {
            format!(
                "Verification FAILED: {} missing, {} mismatched out of {} expected",
                self.missing, self.mismatched, self.expected
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_success() {
        let report = VerificationReport {
            expected: 100,
            found: 100,
            missing: 0,
            mismatched: 0,
            matched: 100,
            ..Default::default()
        };

        assert!(report.is_success());
    }

    #[test]
    fn test_report_failure_missing() {
        let report = VerificationReport {
            expected: 100,
            found: 95,
            missing: 5,
            mismatched: 0,
            matched: 95,
            ..Default::default()
        };

        assert!(!report.is_success());
    }

    #[test]
    fn test_report_failure_mismatched() {
        let report = VerificationReport {
            expected: 100,
            found: 100,
            missing: 0,
            mismatched: 3,
            matched: 97,
            ..Default::default()
        };

        assert!(!report.is_success());
    }

    #[test]
    fn test_report_summary() {
        let report = VerificationReport {
            expected: 100,
            found: 100,
            missing: 0,
            mismatched: 0,
            matched: 100,
            total_duration: Duration::from_secs(2),
            ..Default::default()
        };

        let summary = report.summary();
        assert!(summary.contains("PASSED"));
        assert!(summary.contains("100/100"));
    }

    #[test]
    fn test_rows_per_second() {
        let report = VerificationReport {
            expected: 1000,
            total_duration: Duration::from_secs(10),
            ..Default::default()
        };

        assert_eq!(report.rows_per_second(), 100.0);
    }
}
