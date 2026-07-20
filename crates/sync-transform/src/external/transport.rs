//! Multiplexed external transports: persistent/transient child-stdio.

use crate::external::wire::{RequestHeader, ResponseHeader};
use crate::framer::{Framer, NdjsonFramer};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, Notify};

/// Whether the worker process lives across batches or is respawned each time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChildStdioMode {
    /// Spawn once; many exchanges on the same pipes (default).
    #[default]
    Persistent,
    /// Spawn → one batch exchange → wait for exit; repeat per batch.
    Transient,
}

/// One complete worker response (header + item payloads).
#[derive(Debug, Clone)]
pub struct WireResponse {
    pub batch_id: u64,
    pub error: Option<String>,
    pub items: Vec<Vec<u8>>,
}

/// Multiplexed byte-oriented external transport.
///
/// Even when `max_in_flight = 1`, apply uses this API (not a “last request
/// wins” single-slot exchange). Responses are correlated by the **request**
/// `batch_id` passed to [`Self::try_read_response`] — never by rebinding a
/// payload read for one waiter onto another outstanding id.
#[async_trait]
pub trait ExternalTransport: Send + Sync {
    /// Write one framed request (header + item lines) for `batch_id`.
    async fn write_request(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<()>;

    /// Await the response for a specific request `batch_id`.
    ///
    /// Returns `Ok(None)` only when there is nothing to wait on yet (rare;
    /// most implementations block until this request’s response is ready).
    /// The returned [`WireResponse::batch_id`] must still echo the request id
    /// (checked by [`crate::ExternalTransform`]).
    async fn try_read_response(&self, batch_id: u64) -> Result<Option<WireResponse>>;
}

pub(crate) fn encode_request(framer: &dyn Framer, batch_id: u64, items: &[Vec<u8>]) -> Vec<u8> {
    let header = RequestHeader {
        batch_id,
        count: items.len(),
    };
    let header_bytes = serde_json::to_vec(&header).expect("RequestHeader serializes");
    let mut out = Vec::with_capacity(
        header_bytes.len() + 1 + items.iter().map(|i| i.len() + 1).sum::<usize>(),
    );
    framer.write_message(&header_bytes, &mut out);
    for item in items {
        framer.write_message(item, &mut out);
    }
    out
}

async fn write_all_locked(stdin: &Mutex<ChildStdin>, bytes: &[u8]) -> Result<()> {
    let mut guard = stdin.lock().await;
    guard.write_all(bytes).await.context("write worker stdin")?;
    guard.flush().await.context("flush worker stdin")?;
    Ok(())
}

/// Pending header when item lines are not yet complete.
struct PendingHeader {
    batch_id: u64,
    count: usize,
    items: Vec<Vec<u8>>,
    /// When set, items are drained then an error [`WireResponse`] is returned.
    error: Option<String>,
}

struct ReadState {
    stdout: ChildStdout,
    buf: BytesMut,
    pending: Option<PendingHeader>,
}

impl ReadState {
    fn new(stdout: ChildStdout) -> Self {
        Self {
            stdout,
            buf: BytesMut::with_capacity(8 * 1024),
            pending: None,
        }
    }

    /// Block until one complete [`WireResponse`] is available.
    async fn read_response(&mut self, framer: &dyn Framer) -> Result<WireResponse> {
        loop {
            if let Some(resp) = self.try_parse(framer)? {
                return Ok(resp);
            }
            let n = self
                .stdout
                .read_buf(&mut self.buf)
                .await
                .context("read worker stdout")?;
            if n == 0 {
                if self.buf.is_empty() && self.pending.is_none() {
                    bail!("external worker stdout EOF while waiting for response");
                }
                bail!(
                    "external worker stdout EOF with incomplete NDJSON ({} bytes buffered)",
                    self.buf.len()
                );
            }
        }
    }

    fn try_parse(&mut self, framer: &dyn Framer) -> Result<Option<WireResponse>> {
        try_parse_buf(&mut self.buf, &mut self.pending, framer)
    }
}

/// Shared NDJSON response parse (used by child-stdio readers and unit tests).
fn try_parse_buf(
    buf: &mut BytesMut,
    pending: &mut Option<PendingHeader>,
    framer: &dyn Framer,
) -> Result<Option<WireResponse>> {
    loop {
        if let Some(mut p) = pending.take() {
            while p.items.len() < p.count {
                match framer.next_message(buf)? {
                    Some(item) => p.items.push(item.to_vec()),
                    None => {
                        *pending = Some(p);
                        return Ok(None);
                    }
                }
            }
            if let Some(error) = p.error {
                // Trailing item lines after an error header are drained so
                // the next header stays framed; payload is discarded.
                return Ok(Some(WireResponse {
                    batch_id: p.batch_id,
                    error: Some(error),
                    items: Vec::new(),
                }));
            }
            return Ok(Some(WireResponse {
                batch_id: p.batch_id,
                error: None,
                items: p.items,
            }));
        }

        let Some(header_bytes) = framer.next_message(buf)? else {
            return Ok(None);
        };

        let header: ResponseHeader = serde_json::from_slice(&header_bytes).with_context(|| {
            format!(
                "invalid external response header JSON: {}",
                String::from_utf8_lossy(&header_bytes)
            )
        })?;

        if header.error.is_some() {
            // Prefer draining declared trailing items to avoid desync; if
            // count is absent, assume a compliant worker sent no items.
            let drain = header.count.unwrap_or(0);
            if drain > 0 {
                *pending = Some(PendingHeader {
                    batch_id: header.batch_id,
                    count: drain,
                    items: Vec::with_capacity(drain),
                    error: header.error,
                });
                continue;
            }
            return Ok(Some(WireResponse {
                batch_id: header.batch_id,
                error: header.error,
                items: Vec::new(),
            }));
        }

        let count = header.count.ok_or_else(|| {
            anyhow!(
                "external response for batch_id={} missing both count and error",
                header.batch_id
            )
        })?;

        *pending = Some(PendingHeader {
            batch_id: header.batch_id,
            count,
            items: Vec::with_capacity(count),
            error: None,
        });
    }
}

/// Persistent child: spawn once, keep alive for many exchanges.
///
/// # Pairing / reliability
///
/// Responses are paired to requests by **write order** on the shared pipe
/// (fail closed if the echoed `batch_id` does not match the dequeued write).
/// That prevents rebinding one request’s waiter onto another outstanding id
/// when a confused worker reorders or mis-labels responses.
///
/// Only one task holds [`Self`]'s read gate while advancing stdout; after a
/// framed response is stored (or returned), the gate is released so peer
/// waiters can drive the next read — the gate is **not** held across peer
/// notify waits.
///
/// # Threat model (accepted)
///
/// A **malicious** worker that answers in write order while echoing the
/// correct `batch_id` for each dequeued write, but attaching another batch’s
/// transformed payload, cannot be detected on a single stdio pipe (transforms
/// change bytes, so request/response content hashing is not a viable check).
/// surreal-sync trusts the worker’s integrity for payload↔id binding; the
/// fail-closed path targets buggy / accidental identity confusion, not a
/// hostile worker. Prefer honest workers; see integration tests for FIFO
/// multiplex and colliding mismatch fail-closed.
pub struct PersistentChildStdio {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    write_order: Mutex<VecDeque<u64>>,
    completed: Mutex<HashMap<u64, Result<WireResponse>>>,
    read: Mutex<ReadState>,
    /// Wakes waiters when a request-keyed slot may be ready.
    notify: Notify,
    /// Only one task advances the shared stdout stream.
    read_gate: Mutex<()>,
    framer: NdjsonFramer,
}

impl PersistentChildStdio {
    /// Spawn `command[0]` with `command[1..]` as args; pipe stdin/stdout.
    pub fn spawn(command: Vec<String>, framer: NdjsonFramer) -> Result<Self> {
        if command.is_empty() {
            bail!("persistent child command must not be empty");
        }
        let program = &command[0];
        let mut cmd = Command::new(program);
        if command.len() > 1 {
            cmd.args(&command[1..]);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn persistent worker: {command:?}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("worker stdin pipe missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("worker stdout pipe missing"))?;

        Ok(Self {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            write_order: Mutex::new(VecDeque::new()),
            completed: Mutex::new(HashMap::new()),
            read: Mutex::new(ReadState::new(stdout)),
            notify: Notify::new(),
            read_gate: Mutex::new(()),
            framer,
        })
    }
}

#[async_trait]
impl ExternalTransport for PersistentChildStdio {
    async fn write_request(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<()> {
        let bytes = encode_request(&self.framer, batch_id, items);
        self.write_order.lock().await.push_back(batch_id);
        write_all_locked(&self.stdin, &bytes).await?;
        self.notify.notify_waiters();
        Ok(())
    }

    async fn try_read_response(&self, batch_id: u64) -> Result<Option<WireResponse>> {
        loop {
            // Register before checking to avoid lost wakeups.
            let notified = self.notify.notified();
            {
                let mut completed = self.completed.lock().await;
                if let Some(resp) = completed.remove(&batch_id) {
                    return resp.map(Some);
                }
            }

            let gate = self.read_gate.try_lock();
            let Ok(_gate) = gate else {
                notified.await;
                continue;
            };

            // Recheck under gate — another reader may have filled our slot.
            {
                let mut completed = self.completed.lock().await;
                if let Some(resp) = completed.remove(&batch_id) {
                    return resp.map(Some);
                }
            }

            let req_id = {
                let mut q = self.write_order.lock().await;
                if q.is_empty() {
                    drop(_gate);
                    notified.await;
                    continue;
                }
                q.pop_front().expect("non-empty")
            };

            // Blocking read without holding write_order / completed.
            let framed = {
                let mut read = self.read.lock().await;
                read.read_response(&self.framer).await
            };

            let result = match framed {
                Ok(resp) if resp.batch_id == req_id => Ok(resp),
                Ok(resp) => Err(anyhow!(
                    "external transform batch_id mismatch: expected request {req_id}, \
                     got response for {} (fail closed; no cross-batch rebind)",
                    resp.batch_id
                )),
                Err(e) => Err(e),
            };

            if req_id == batch_id {
                self.notify.notify_waiters();
                return result.map(Some);
            }
            self.completed.lock().await.insert(req_id, result);
            self.notify.notify_waiters();
            // Another waiter's response — check whether ours was filled as a
            // side effect, then drop the read gate so peers can drive stdout.
            {
                let mut completed = self.completed.lock().await;
                if let Some(resp) = completed.remove(&batch_id) {
                    return resp.map(Some);
                }
            }
            drop(_gate);
        }
    }
}

impl Drop for PersistentChildStdio {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.try_lock() {
            let _ = child.start_kill();
        }
    }
}

/// Transient child: one process per batch exchange.
///
/// `write_request` spawns and writes; `try_read_response` reads that child's
/// response and waits for exit. Overlapping batches each get their own child
/// (limited usefulness vs persistent when `max_in_flight` is large).
pub struct TransientChildStdio {
    command: Vec<String>,
    framer: NdjsonFramer,
    /// In-flight children keyed by request batch_id.
    inflight: Mutex<HashMap<u64, TransientChild>>,
    notify: Arc<Notify>,
}

struct TransientChild {
    child: Child,
    read: ReadState,
}

impl TransientChildStdio {
    pub fn new(command: Vec<String>, framer: NdjsonFramer) -> Self {
        Self {
            command,
            framer,
            inflight: Mutex::new(HashMap::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    fn spawn_one(&self) -> Result<(ChildStdin, TransientChild)> {
        if self.command.is_empty() {
            bail!("transient child command must not be empty");
        }
        let program = &self.command[0];
        let mut cmd = Command::new(program);
        if self.command.len() > 1 {
            cmd.args(&self.command[1..]);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn transient worker: {:?}", self.command))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("worker stdin pipe missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("worker stdout pipe missing"))?;
        Ok((
            stdin,
            TransientChild {
                child,
                read: ReadState::new(stdout),
            },
        ))
    }
}

#[async_trait]
impl ExternalTransport for TransientChildStdio {
    async fn write_request(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<()> {
        let (mut stdin, tc) = self.spawn_one()?;
        let bytes = encode_request(&self.framer, batch_id, items);
        stdin
            .write_all(&bytes)
            .await
            .context("write transient worker stdin")?;
        stdin
            .flush()
            .await
            .context("flush transient worker stdin")?;
        // Close stdin so workers that read until EOF still complete.
        drop(stdin);
        self.inflight.lock().await.insert(batch_id, tc);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn try_read_response(&self, batch_id: u64) -> Result<Option<WireResponse>> {
        loop {
            // Register before checking to avoid lost wakeups when write races.
            let notified = self.notify.notified();
            let tc = {
                let mut inflight = self.inflight.lock().await;
                inflight.remove(&batch_id)
            };
            let Some(mut tc) = tc else {
                notified.await;
                continue;
            };

            // Read/wait without holding the inflight map (other batches can write).
            let resp = tc.read.read_response(&self.framer).await?;
            let status = tc
                .child
                .wait()
                .await
                .context("wait transient worker exit")?;
            if !status.success() {
                bail!("transient worker exited with {status} for batch_id={batch_id}");
            }
            return Ok(Some(resp));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framer::NdjsonFramer;

    #[test]
    fn error_header_drains_trailing_item_lines() {
        let framer = NdjsonFramer;
        let mut out = Vec::new();
        // error + count=2 with two trailing item lines, then a normal success header.
        framer.write_message(br#"{"batch_id":1,"error":"boom","count":2}"#, &mut out);
        framer.write_message(br#"{"junk":1}"#, &mut out);
        framer.write_message(br#"{"junk":2}"#, &mut out);
        framer.write_message(br#"{"batch_id":2,"count":0}"#, &mut out);
        let mut buf = BytesMut::from(out.as_slice());
        let mut pending = None;

        let err_resp = try_parse_buf(&mut buf, &mut pending, &framer)
            .unwrap()
            .expect("error response");
        assert_eq!(err_resp.batch_id, 1);
        assert_eq!(err_resp.error.as_deref(), Some("boom"));
        assert!(err_resp.items.is_empty());
        assert!(pending.is_none());

        let ok_resp = try_parse_buf(&mut buf, &mut pending, &framer)
            .unwrap()
            .expect("follow-up response");
        assert_eq!(ok_resp.batch_id, 2);
        assert!(ok_resp.error.is_none());
        assert!(ok_resp.items.is_empty());
    }
}
