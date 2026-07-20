//! External transform boundary: NDJSON wire protocol + multiplexed child-stdio.

mod transport;
mod wire;

pub use transport::{
    ChildStdioMode, ExternalTransport, PersistentChildStdio, TransientChildStdio, WireResponse,
};
pub use wire::{RequestHeader, ResponseHeader};

use anyhow::{anyhow, bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use sync_core::{UniversalChange, UniversalRow};
use tokio::sync::{Mutex, Notify};

/// External (child-stdio) transform stage.
///
/// surreal-sync spawns the configured worker and talks on **the worker's**
/// stdin/stdout pipes — not the surreal-sync CLI's stdin.
///
/// Exchanges are multiplexed by `batch_id`: [`ExternalTransport::write_request`]
/// / [`ExternalTransport::try_read_response`] support overlapping in-flight
/// batches (`max_in_flight` > 1). Responses may complete out of order; this
/// type demuxes them so each [`Self::exchange_changes`] / [`Self::exchange_rows`]
/// call receives its own `batch_id`.
#[derive(Clone)]
pub struct ExternalTransform {
    inner: Arc<ExternalInner>,
}

struct ExternalInner {
    transport: Arc<dyn ExternalTransport>,
    /// Stash for out-of-order responses belonging to other waiters.
    stash: Mutex<HashMap<u64, WireResponse>>,
    /// batch_ids with a write outstanding / waiter in exchange_raw.
    outstanding: Mutex<HashSet<u64>>,
    /// Serialize the reader so only one task drives `try_read_response`.
    read_gate: Mutex<()>,
    /// Wakes waiters when a stashed response may match.
    notify: Notify,
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
                stash: Mutex::new(HashMap::new()),
                outstanding: Mutex::new(HashSet::new()),
                read_gate: Mutex::new(()),
                notify: Notify::new(),
            }),
        }
    }

    /// Spawn a persistent child worker (default mode): one process, many batches.
    pub fn persistent_child(command: Vec<String>) -> Result<Self> {
        let transport =
            PersistentChildStdio::spawn(command, crate::framer::NdjsonFramer)?;
        Ok(Self::with_transport(Arc::new(transport)))
    }

    /// Transient child worker: spawn → one exchange → exit, per batch.
    pub fn transient_child(command: Vec<String>) -> Result<Self> {
        let transport =
            TransientChildStdio::new(command, crate::framer::NdjsonFramer);
        Ok(Self::with_transport(Arc::new(transport)))
    }

    /// Convenience from mode + argv.
    pub fn child_stdio(mode: ChildStdioMode, command: Vec<String>) -> Result<Self> {
        match mode {
            ChildStdioMode::Persistent => Self::persistent_child(command),
            ChildStdioMode::Transient => Self::transient_child(command),
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
        let resp = self.exchange_raw(batch_id, &items).await?;
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
        let resp = self.exchange_raw(batch_id, &items).await?;
        resp.items
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice(&bytes).context("deserialize UniversalRow from worker")
            })
            .collect()
    }

    async fn exchange_raw(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<WireResponse> {
        {
            let mut outstanding = self.inner.outstanding.lock().await;
            if !outstanding.insert(batch_id) {
                bail!("duplicate in-flight external batch_id={batch_id}");
            }
        }

        let result = self.exchange_raw_inner(batch_id, items).await;

        self.inner.outstanding.lock().await.remove(&batch_id);
        self.inner.notify.notify_waiters();
        result
    }

    async fn exchange_raw_inner(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<WireResponse> {
        self.inner
            .transport
            .write_request(batch_id, items)
            .await
            .with_context(|| format!("write_request batch_id={batch_id}"))?;

        loop {
            if let Some(resp) = self.inner.stash.lock().await.remove(&batch_id) {
                return finish_response(batch_id, resp);
            }

            let _gate = self.inner.read_gate.lock().await;
            if let Some(resp) = self.inner.stash.lock().await.remove(&batch_id) {
                return finish_response(batch_id, resp);
            }

            match self
                .inner
                .transport
                .try_read_response()
                .await
                .context("try_read_response")?
            {
                None => {
                    drop(_gate);
                    self.inner.notify.notified().await;
                }
                Some(resp) => {
                    if resp.batch_id == batch_id {
                        self.inner.notify.notify_waiters();
                        return finish_response(batch_id, resp);
                    }
                    let known = self.inner.outstanding.lock().await.contains(&resp.batch_id)
                        || self.inner.stash.lock().await.contains_key(&resp.batch_id);
                    if known {
                        self.inner.stash.lock().await.insert(resp.batch_id, resp);
                        self.inner.notify.notify_waiters();
                    } else {
                        self.inner.notify.notify_waiters();
                        bail!(
                            "external transform batch_id mismatch: got response for {}, \
                             expected {batch_id} (unknown/mismatched batch_id; no sink/commit)",
                            resp.batch_id
                        );
                    }
                }
            }
        }
    }
}

fn finish_response(expected_batch_id: u64, resp: WireResponse) -> Result<WireResponse> {
    if resp.batch_id != expected_batch_id {
        bail!(
            "external transform batch_id mismatch: expected {expected_batch_id}, got {}",
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
