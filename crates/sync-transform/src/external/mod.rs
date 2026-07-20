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
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow};
use tokio::sync::Mutex;

/// External (child-stdio) transform stage.
///
/// surreal-sync spawns the configured worker and talks on **the worker's**
/// stdin/stdout pipes — not the surreal-sync CLI's stdin.
///
/// Exchanges are multiplexed by `batch_id`: [`ExternalTransport::write_request`]
/// / [`ExternalTransport::try_read_response`] support overlapping in-flight
/// batches (`max_in_flight` > 1). Each waiter reads **only** its own request's
/// response (request-keyed); payloads are never rebound onto another
/// outstanding id. A mismatched echo fails the exchange (no sink/commit).
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
    }
}

fn finish_response(expected_batch_id: u64, resp: WireResponse) -> Result<WireResponse> {
    if resp.batch_id != expected_batch_id {
        bail!(
            "external transform batch_id mismatch: expected {expected_batch_id}, got {} \
             (mismatched batch_id; no sink/commit)",
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
