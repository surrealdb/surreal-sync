//! External transform boundary: NDJSON wire protocol + multiplexed child-stdio.

mod transport;
mod wire;

pub use transport::{
    ChildStdioMode, ExternalTransport, PersistentChildStdio, TransientChildStdio, WireResponse,
};
pub use wire::{RequestHeader, ResponseHeader, WireItemKind};

use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow};
use tokio::sync::Mutex;

use crate::framer::FramerKind;

/// Per-stage retry/backoff for a command transform exchange.
///
/// `max_attempts = 1` (default) means no retry. Backoff grows exponentially
/// from `initial_backoff` up to `max_backoff`. When `jitter` is true, each
/// sleep is scaled by a deterministic factor in `[0.5, 1.5)` derived from the
/// attempt number (no extra RNG dependency).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first try (`>= 1`).
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(30),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Delay before attempt `attempt` (1-based attempt that just failed).
    pub fn backoff_after(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let base = self
            .initial_backoff
            .saturating_mul(1u32 << shift)
            .min(self.max_backoff);
        if !self.jitter {
            return base;
        }
        // Deterministic-ish jitter without pulling in a RNG crate: mix attempt
        // into a simple LCG over the duration nanos.
        let nanos = base.as_nanos().max(1);
        let mixed = nanos
            .wrapping_mul(1103515245)
            .wrapping_add(attempt as u128 * 12345);
        let scale_permille = 500 + (mixed % 1000); // 500..=1499 → 0.5..=1.499
        let jittered = nanos.saturating_mul(scale_permille) / 1000;
        Duration::from_nanos(jittered.min(u128::from(u64::MAX)) as u64).min(self.max_backoff)
    }
}

/// High bit set on the apply `batch_id` when an External stage issues a
/// **relation** wire exchange that shares an apply batch with row changes.
///
/// Mixed change+relation batches perform two sequential NDJSON exchanges.
/// Reusing the same wire `batch_id` for both is a footgun for outstanding-id
/// tracking and worker scripts keyed only on `batch_id`. Relation exchanges
/// in that path use [`relation_wire_batch_id`] instead; homogeneous relation
/// batches keep the plain apply `batch_id`.
pub const RELATION_WIRE_BATCH_ID_BIT: u64 = 1u64 << 63;

/// Wire `batch_id` for the relation half of a mixed External exchange.
#[inline]
pub fn relation_wire_batch_id(batch_id: u64) -> u64 {
    batch_id | RELATION_WIRE_BATCH_ID_BIT
}

/// External (child-stdio) transform stage.
///
/// surreal-sync spawns the configured worker and talks on **the worker's**
/// stdin/stdout pipes — not the surreal-sync CLI's stdin.
///
/// Exchanges are multiplexed by `batch_id`: [`ExternalTransport::write_request`]
/// / [`ExternalTransport::try_read_response`] support overlapping in-flight
/// batches (`max_in_flight` > 1). Each waiter reads **only** its own request's
/// response (request-keyed); payloads are never rebound onto another
/// outstanding id. A mismatched echo fails the exchange (no sink / watermark advance).
///
/// # Relations
///
/// Relation batches use the same NDJSON framing with
/// [`WireItemKind::RelationChange`] / [`WireItemKind::Relation`]. There is **no**
/// silent pass-through of relation events past External stages.
#[derive(Clone)]
pub struct ExternalTransform {
    inner: Arc<ExternalInner>,
}

struct ExternalInner {
    transport: Arc<dyn ExternalTransport>,
    /// batch_ids with a write outstanding / waiter in exchange_raw.
    outstanding: Mutex<HashSet<u64>>,
    /// Optional per-exchange timeout (None = rely on apply-layer timeout only).
    timeout: Option<Duration>,
    /// Per-stage retry/backoff (default: single attempt).
    retry: RetryPolicy,
}

impl std::fmt::Debug for ExternalTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalTransform").finish_non_exhaustive()
    }
}

impl ExternalTransform {
    /// Build an external stage over an arbitrary multiplexed transport.
    pub fn with_transport(transport: Arc<dyn ExternalTransport>) -> Self {
        Self {
            inner: Arc::new(ExternalInner {
                transport,
                outstanding: Mutex::new(HashSet::new()),
                timeout: None,
                retry: RetryPolicy::default(),
            }),
        }
    }

    /// Set per-exchange timeout for this stage (None clears it).
    ///
    /// Intended as a builder step at construction time (before the stage is
    /// shared across in-flight batches).
    pub fn with_timeout(self, timeout: Option<Duration>) -> Self {
        Self {
            inner: Arc::new(ExternalInner {
                transport: Arc::clone(&self.inner.transport),
                outstanding: Mutex::new(HashSet::new()),
                timeout,
                retry: self.inner.retry.clone(),
            }),
        }
    }

    /// Set per-stage retry/backoff policy.
    ///
    /// Intended as a builder step at construction time.
    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self {
            inner: Arc::new(ExternalInner {
                transport: Arc::clone(&self.inner.transport),
                outstanding: Mutex::new(HashSet::new()),
                timeout: self.inner.timeout,
                retry,
            }),
        }
    }

    /// Spawn a persistent child worker (default mode): one process, many batches.
    pub fn persistent_child(command: Vec<String>, framer: FramerKind) -> Result<Self> {
        let transport = PersistentChildStdio::spawn(command, framer)?;
        Ok(Self::with_transport(Arc::new(transport)))
    }

    /// Transient child worker: spawn → one exchange → exit, per batch.
    pub fn transient_child(command: Vec<String>, framer: FramerKind) -> Result<Self> {
        let transport = TransientChildStdio::new(command, framer);
        Ok(Self::with_transport(Arc::new(transport)))
    }

    /// Convenience from mode + argv + framer.
    pub fn child_stdio(
        mode: ChildStdioMode,
        command: Vec<String>,
        framer: FramerKind,
    ) -> Result<Self> {
        match mode {
            ChildStdioMode::Persistent => Self::persistent_child(command, framer),
            ChildStdioMode::Transient => Self::transient_child(command, framer),
        }
    }

    /// Path helper for tests locating a fixture binary.
    pub fn command_from_bin(bin: impl Into<PathBuf>, args: &[&str]) -> Vec<String> {
        let mut cmd = vec![bin.into().to_string_lossy().into_owned()];
        cmd.extend(args.iter().map(|s| (*s).to_string()));
        cmd
    }

    /// Exchange a change batch with the worker (serialize → I/O → deserialize).
    pub async fn exchange_changes(
        &self,
        batch_id: u64,
        changes: Vec<UniversalChange>,
    ) -> Result<Vec<UniversalChange>> {
        let items: Vec<Vec<u8>> = changes
            .iter()
            .map(|c| serde_json::to_vec(c).context("serialize UniversalChange"))
            .collect::<Result<Vec<_>>>()?;
        let resp = self
            .exchange_raw(batch_id, &items, WireItemKind::Change)
            .await?;
        resp.items
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice(&bytes).context("deserialize UniversalChange from worker")
            })
            .collect()
    }

    /// Exchange a row batch with the worker.
    pub async fn exchange_rows(
        &self,
        batch_id: u64,
        rows: Vec<UniversalRow>,
    ) -> Result<Vec<UniversalRow>> {
        let items: Vec<Vec<u8>> = rows
            .iter()
            .map(|r| serde_json::to_vec(r).context("serialize UniversalRow"))
            .collect::<Result<Vec<_>>>()?;
        let resp = self
            .exchange_raw(batch_id, &items, WireItemKind::Row)
            .await?;
        resp.items
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice(&bytes).context("deserialize UniversalRow from worker")
            })
            .collect()
    }

    /// Exchange a relation-change batch with the worker (NDJSON wire).
    pub async fn exchange_relation_changes(
        &self,
        batch_id: u64,
        changes: Vec<UniversalRelationChange>,
    ) -> Result<Vec<UniversalRelationChange>> {
        let items: Vec<Vec<u8>> = changes
            .iter()
            .map(|c| serde_json::to_vec(c).context("serialize UniversalRelationChange"))
            .collect::<Result<Vec<_>>>()?;
        let resp = self
            .exchange_raw(batch_id, &items, WireItemKind::RelationChange)
            .await?;
        resp.items
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .context("deserialize UniversalRelationChange from worker")
            })
            .collect()
    }

    /// Exchange a full-sync relation batch with the worker.
    pub async fn exchange_relations(
        &self,
        batch_id: u64,
        relations: Vec<UniversalRelation>,
    ) -> Result<Vec<UniversalRelation>> {
        let items: Vec<Vec<u8>> = relations
            .iter()
            .map(|r| serde_json::to_vec(r).context("serialize UniversalRelation"))
            .collect::<Result<Vec<_>>>()?;
        let resp = self
            .exchange_raw(batch_id, &items, WireItemKind::Relation)
            .await?;
        resp.items
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice(&bytes).context("deserialize UniversalRelation from worker")
            })
            .collect()
    }

    async fn exchange_raw(
        &self,
        batch_id: u64,
        items: &[Vec<u8>],
        kind: WireItemKind,
    ) -> Result<WireResponse> {
        {
            let mut outstanding = self.inner.outstanding.lock().await;
            if !outstanding.insert(batch_id) {
                bail!("duplicate in-flight external batch_id={batch_id}");
            }
        }

        let result = self.exchange_raw_inner(batch_id, items, kind).await;

        self.inner.outstanding.lock().await.remove(&batch_id);
        result
    }

    async fn exchange_raw_inner(
        &self,
        batch_id: u64,
        items: &[Vec<u8>],
        kind: WireItemKind,
    ) -> Result<WireResponse> {
        let max_attempts = self.inner.retry.max_attempts.max(1);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let result = self.exchange_once(batch_id, items, kind).await;
            match result {
                Ok(resp) => return Ok(resp),
                Err(err) if attempt < max_attempts => {
                    let delay = self.inner.retry.backoff_after(attempt);
                    tracing::warn!(
                        batch_id,
                        attempt,
                        max_attempts,
                        ?delay,
                        error = %err,
                        "command transform exchange failed; retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "command transform failed after {attempt} attempt(s) \
                             for batch_id={batch_id}"
                        )
                    });
                }
            }
        }
    }

    async fn exchange_once(
        &self,
        batch_id: u64,
        items: &[Vec<u8>],
        kind: WireItemKind,
    ) -> Result<WireResponse> {
        let fut = async {
            self.inner
                .transport
                .write_request(batch_id, items, kind)
                .await
                .with_context(|| format!("write_request batch_id={batch_id}"))?;

            let resp = self
                .inner
                .transport
                .try_read_response(batch_id)
                .await
                .context("try_read_response")?
                .ok_or_else(|| {
                    anyhow!("external transport returned no response for batch_id={batch_id}")
                })?;
            finish_response(batch_id, resp)
        };

        if let Some(timeout) = self.inner.timeout {
            tokio::time::timeout(timeout, fut)
                .await
                .map_err(|_| {
                    anyhow!(
                        "command transform timeout after {timeout:?} for batch_id={batch_id}"
                    )
                })?
        } else {
            fut.await
        }
    }
}

fn finish_response(expected_batch_id: u64, resp: WireResponse) -> Result<WireResponse> {
    if resp.batch_id != expected_batch_id {
        bail!(
            "external transform batch_id mismatch: expected {expected_batch_id}, got {} \
             (mismatched batch_id; no sink / watermark advance)",
            resp.batch_id
        );
    }
    if let Some(err) = resp.error {
        return Err(anyhow!(
            "external worker error for batch_id={expected_batch_id}: {err}"
        ));
    }
    Ok(resp)
}

#[cfg(test)]
mod retry_policy_tests {
    use super::*;

    #[test]
    fn backoff_after_no_jitter_doubles_until_cap() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(800),
            jitter: false,
        };
        assert_eq!(policy.backoff_after(1), Duration::from_millis(100));
        assert_eq!(policy.backoff_after(2), Duration::from_millis(200));
        assert_eq!(policy.backoff_after(3), Duration::from_millis(400));
        assert_eq!(policy.backoff_after(4), Duration::from_millis(800));
        assert_eq!(policy.backoff_after(5), Duration::from_millis(800));
        assert_eq!(policy.backoff_after(20), Duration::from_millis(800));
    }

    #[test]
    fn backoff_after_jitter_stays_within_half_to_cap() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(1),
            jitter: true,
        };
        for attempt in 1..=8 {
            let base = {
                let shift = (attempt - 1).min(16);
                policy
                    .initial_backoff
                    .saturating_mul(1u32 << shift)
                    .min(policy.max_backoff)
            };
            let delay = policy.backoff_after(attempt);
            // Jitter scales by [0.5, 1.5) then caps at max_backoff.
            let min_expected = base / 2;
            assert!(
                delay >= min_expected,
                "attempt {attempt}: delay {delay:?} < half of base {base:?}"
            );
            assert!(
                delay <= policy.max_backoff,
                "attempt {attempt}: delay {delay:?} exceeds max_backoff"
            );
        }
        // Same attempt is deterministic (no RNG).
        assert_eq!(policy.backoff_after(3), policy.backoff_after(3));
    }

    #[test]
    fn backoff_after_jitter_differs_from_plain_base() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(1000),
            max_backoff: Duration::from_secs(60),
            jitter: true,
        };
        let no_jitter = RetryPolicy {
            jitter: false,
            ..policy.clone()
        };
        // At least one of the early attempts should differ once jitter mixes.
        let differs = (1..=4).any(|a| policy.backoff_after(a) != no_jitter.backoff_after(a));
        assert!(differs, "jitter should change at least one backoff delay");
    }
}
