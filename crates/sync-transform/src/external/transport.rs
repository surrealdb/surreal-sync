//! Multiplexed external transports: persistent/transient child-stdio.

use crate::external::wire::{RequestHeader, ResponseHeader};
use crate::framer::{Framer, NdjsonFramer};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

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
/// wins” single-slot exchange). Responses are correlated by `batch_id`.
#[async_trait]
pub trait ExternalTransport: Send + Sync {
    /// Write one framed request (header + item lines) for `batch_id`.
    async fn write_request(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<()>;

    /// Await the next complete response from the stream.
    ///
    /// Returns `Ok(None)` only when there is nothing to read yet (e.g. transient
    /// mode with no in-flight children). Returns `Ok(Some(_))` for any completed
    /// response — not necessarily FIFO with writes (multiplexed / out-of-order).
    async fn try_read_response(&self) -> Result<Option<WireResponse>>;
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
        loop {
            if let Some(mut pending) = self.pending.take() {
                while pending.items.len() < pending.count {
                    match framer.next_message(&mut self.buf)? {
                        Some(item) => pending.items.push(item.to_vec()),
                        None => {
                            self.pending = Some(pending);
                            return Ok(None);
                        }
                    }
                }
                return Ok(Some(WireResponse {
                    batch_id: pending.batch_id,
                    error: None,
                    items: pending.items,
                }));
            }

            let Some(header_bytes) = framer.next_message(&mut self.buf)? else {
                return Ok(None);
            };

            let header: ResponseHeader =
                serde_json::from_slice(&header_bytes).with_context(|| {
                    format!(
                        "invalid external response header JSON: {}",
                        String::from_utf8_lossy(&header_bytes)
                    )
                })?;

            if header.error.is_some() {
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

            self.pending = Some(PendingHeader {
                batch_id: header.batch_id,
                count,
                items: Vec::with_capacity(count),
            });
        }
    }
}

/// Persistent child: spawn once, keep alive for many exchanges.
pub struct PersistentChildStdio {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    read: Mutex<ReadState>,
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
            read: Mutex::new(ReadState::new(stdout)),
            framer,
        })
    }
}

#[async_trait]
impl ExternalTransport for PersistentChildStdio {
    async fn write_request(&self, batch_id: u64, items: &[Vec<u8>]) -> Result<()> {
        let bytes = encode_request(&self.framer, batch_id, items);
        write_all_locked(&self.stdin, &bytes).await
    }

    async fn try_read_response(&self) -> Result<Option<WireResponse>> {
        let mut read = self.read.lock().await;
        let resp = read.read_response(&self.framer).await?;
        Ok(Some(resp))
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
    /// In-flight children keyed by batch_id.
    inflight: Mutex<HashMap<u64, TransientChild>>,
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
        Ok(())
    }

    async fn try_read_response(&self) -> Result<Option<WireResponse>> {
        let mut inflight = self.inflight.lock().await;
        if inflight.is_empty() {
            return Ok(None);
        }

        // Prefer any child that already has a complete response buffered;
        // otherwise block on an arbitrary in-flight child.
        let keys: Vec<u64> = inflight.keys().copied().collect();
        for batch_id in keys {
            let Some(tc) = inflight.get_mut(&batch_id) else {
                continue;
            };
            if let Some(resp) = tc.read.try_parse(&self.framer)? {
                let mut tc = inflight.remove(&batch_id).expect("just got");
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

        // Block on the first in-flight child.
        let batch_id = *inflight.keys().next().expect("non-empty");
        let resp = {
            let tc = inflight.get_mut(&batch_id).expect("just keyed");
            tc.read.read_response(&self.framer).await?
        };
        let mut tc = inflight.remove(&batch_id).expect("just got");
        let status = tc
            .child
            .wait()
            .await
            .context("wait transient worker exit")?;
        if !status.success() {
            bail!("transient worker exited with {status} for batch_id={batch_id}");
        }
        Ok(Some(resp))
    }
}
