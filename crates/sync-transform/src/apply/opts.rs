//! Apply options: batching, in-flight window, timeout, failure policy.

use std::time::Duration;

/// What to do when a batch fails transform or sink apply.
///
/// - [`Fail`](Self::Fail) (default): stop; do not commit the failed batch or any
///   later position. Operator restarts resume from the last successful commit.
/// - [`Skip`](Self::Skip): log, do **not** write the failed batch, but still
///   commit past it. This can lose data by explicit operator choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailurePolicy {
    /// Stop on failure; leave checkpoint unchanged for the failed batch.
    #[default]
    Fail,
    /// Drop the failed batch (no sink write) and commit past it.
    Skip,
}

/// Options for [`crate::run_change_feed`], [`crate::write_rows`], and
/// [`crate::ApplyContext`].
///
/// `max_in_flight` is only the **window size** (default 1). W=1 and W=16 share
/// the same apply runtime: concurrent transforms, ordered sink + contiguous
/// commit watermark.
#[derive(Debug, Clone)]
pub struct ApplyOpts {
    /// Maximum number of batches transforming concurrently (window size).
    /// Default: 1.
    pub max_in_flight: usize,
    /// Accumulate this many changes before starting a transform batch.
    /// Default: 1000.
    pub batch_size: usize,
    /// Flush a partial batch after this idle wait.
    /// Default: 500ms.
    pub batch_max_wait: Duration,
    /// Per-batch transform timeout.
    /// Default: 60s.
    pub timeout: Duration,
    /// Transform / sink failure handling.
    /// Default: [`FailurePolicy::Fail`].
    pub failure_policy: FailurePolicy,
}

impl Default for ApplyOpts {
    fn default() -> Self {
        Self {
            max_in_flight: 1,
            batch_size: 1000,
            batch_max_wait: Duration::from_millis(500),
            timeout: Duration::from_secs(60),
            failure_policy: FailurePolicy::Fail,
        }
    }
}

impl ApplyOpts {
    /// Builder: set in-flight window size (clamped to at least 1).
    pub fn with_max_in_flight(mut self, n: usize) -> Self {
        self.max_in_flight = n.max(1);
        self
    }

    /// Builder: set batch size (clamped to at least 1).
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    /// Builder: set max wait before flushing a partial batch.
    pub fn with_batch_max_wait(mut self, d: Duration) -> Self {
        self.batch_max_wait = d;
        self
    }

    /// Builder: set per-batch transform timeout.
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// Builder: set failure policy.
    pub fn with_failure_policy(mut self, policy: FailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }
}
