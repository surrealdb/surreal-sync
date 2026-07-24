//! Test doubles for the shared apply loop (feature `test-support` or `cfg(test)`).
//!
//! Shared by `sync-transform` reliability tests and (later) source crates that
//! need the same harness without duplicating window semantics.

use crate::{
    ApplyEvent, BatchTransformer, ChangeFeed, ExternalTransport, Pipeline, PositionedChange,
    PositionedEvent, SourceDriver, WireResponse,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use surreal_sink::SurrealSink;
use sync_core::{Change, Relation, RelationChange, Row};
use tokio::sync::Notify;

/// Per-batch script for [`ScriptedTransformer`].
#[derive(Debug, Clone)]
pub struct BatchScript {
    /// Artificial delay before the inner pipeline runs (enables out-of-order completion).
    pub delay: Duration,
    /// If set, fail after the delay (transform not acked).
    pub fail_with: Option<String>,
}

impl Default for BatchScript {
    fn default() -> Self {
        Self {
            delay: Duration::ZERO,
            fail_with: None,
        }
    }
}

impl BatchScript {
    /// Succeed after `delay`.
    pub fn succeed_after(delay: Duration) -> Self {
        Self {
            delay,
            fail_with: None,
        }
    }

    /// Fail after `delay` with the given message.
    pub fn fail_after(delay: Duration, msg: impl Into<String>) -> Self {
        Self {
            delay,
            fail_with: Some(msg.into()),
        }
    }
}

#[derive(Default)]
struct ScriptedTransformerState {
    /// Scripts keyed by `batch_id`. Missing ids use [`BatchScript::default`].
    by_batch_id: HashMap<u64, BatchScript>,
    /// Order in which transforms finished (for assertions).
    completed_order: Vec<u64>,
    /// Order in which transforms started.
    started_order: Vec<u64>,
    /// How many times `transform_*` was invoked (identity-dispatch checks).
    transform_calls: u64,
}

/// [`BatchTransformer`] that wraps a [`Pipeline`] with per-`batch_id` delay/fail.
///
/// Used to simulate out-of-order transform completion without External child-stdio.
#[derive(Clone)]
pub struct ScriptedTransformer {
    inner: Pipeline,
    state: Arc<Mutex<ScriptedTransformerState>>,
}

impl ScriptedTransformer {
    /// Wrap an inner pipeline (often identity / empty).
    pub fn new(inner: Pipeline) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(ScriptedTransformerState::default())),
        }
    }

    /// Configure behavior for a specific `batch_id`.
    pub fn on_batch(self, batch_id: u64, script: BatchScript) -> Self {
        self.state
            .lock()
            .expect("scripted transformer lock")
            .by_batch_id
            .insert(batch_id, script);
        self
    }

    /// Batch ids in the order transforms started.
    pub fn started_order(&self) -> Vec<u64> {
        self.state
            .lock()
            .expect("scripted transformer lock")
            .started_order
            .clone()
    }

    /// Batch ids in the order transforms completed.
    pub fn completed_order(&self) -> Vec<u64> {
        self.state
            .lock()
            .expect("scripted transformer lock")
            .completed_order
            .clone()
    }

    /// Number of `transform_changes` / `transform_rows` invocations.
    pub fn transform_calls(&self) -> u64 {
        self.state
            .lock()
            .expect("scripted transformer lock")
            .transform_calls
    }
}

#[async_trait]
impl BatchTransformer for ScriptedTransformer {
    fn is_identity(&self) -> bool {
        // Even when the inner pipeline is empty, scripts may delay/fail — not identity.
        false
    }

    async fn transform_changes(&self, batch_id: u64, changes: Vec<Change>) -> Result<Vec<Change>> {
        let script = {
            let mut st = self.state.lock().expect("scripted transformer lock");
            st.transform_calls += 1;
            st.started_order.push(batch_id);
            st.by_batch_id.get(&batch_id).cloned().unwrap_or_default()
        };
        if !script.delay.is_zero() {
            tokio::time::sleep(script.delay).await;
        }
        if let Some(msg) = script.fail_with {
            self.state
                .lock()
                .expect("scripted transformer lock")
                .completed_order
                .push(batch_id);
            return Err(anyhow!(msg));
        }
        let out = self.inner.apply_changes(changes)?;
        self.state
            .lock()
            .expect("scripted transformer lock")
            .completed_order
            .push(batch_id);
        Ok(out)
    }

    async fn transform_rows(&self, batch_id: u64, rows: Vec<Row>) -> Result<Vec<Row>> {
        let script = {
            let mut st = self.state.lock().expect("scripted transformer lock");
            st.transform_calls += 1;
            st.started_order.push(batch_id);
            st.by_batch_id.get(&batch_id).cloned().unwrap_or_default()
        };
        if !script.delay.is_zero() {
            tokio::time::sleep(script.delay).await;
        }
        if let Some(msg) = script.fail_with {
            self.state
                .lock()
                .expect("scripted transformer lock")
                .completed_order
                .push(batch_id);
            return Err(anyhow!(msg));
        }
        let out = self.inner.apply_rows(rows)?;
        self.state
            .lock()
            .expect("scripted transformer lock")
            .completed_order
            .push(batch_id);
        Ok(out)
    }

    async fn transform_relation_changes(
        &self,
        batch_id: u64,
        changes: Vec<RelationChange>,
    ) -> Result<Vec<RelationChange>> {
        // Prefer transform_events for scripted delay/fail; this path is for
        // homogeneous relation-only helpers.
        let script = {
            let mut st = self.state.lock().expect("scripted transformer lock");
            st.transform_calls += 1;
            st.started_order.push(batch_id);
            st.by_batch_id.get(&batch_id).cloned().unwrap_or_default()
        };
        if !script.delay.is_zero() {
            tokio::time::sleep(script.delay).await;
        }
        if let Some(msg) = script.fail_with {
            self.state
                .lock()
                .expect("scripted transformer lock")
                .completed_order
                .push(batch_id);
            return Err(anyhow!(msg));
        }
        let out = self.inner.apply_relation_changes(changes)?;
        self.state
            .lock()
            .expect("scripted transformer lock")
            .completed_order
            .push(batch_id);
        Ok(out)
    }

    async fn transform_relations(
        &self,
        batch_id: u64,
        relations: Vec<Relation>,
    ) -> Result<Vec<Relation>> {
        let script = {
            let mut st = self.state.lock().expect("scripted transformer lock");
            st.transform_calls += 1;
            st.started_order.push(batch_id);
            st.by_batch_id.get(&batch_id).cloned().unwrap_or_default()
        };
        if !script.delay.is_zero() {
            tokio::time::sleep(script.delay).await;
        }
        if let Some(msg) = script.fail_with {
            self.state
                .lock()
                .expect("scripted transformer lock")
                .completed_order
                .push(batch_id);
            return Err(anyhow!(msg));
        }
        let out = self.inner.apply_relations(relations)?;
        self.state
            .lock()
            .expect("scripted transformer lock")
            .completed_order
            .push(batch_id);
        Ok(out)
    }

    async fn transform_events(
        &self,
        batch_id: u64,
        events: Vec<ApplyEvent>,
    ) -> Result<Vec<ApplyEvent>> {
        let script = {
            let mut st = self.state.lock().expect("scripted transformer lock");
            st.transform_calls += 1;
            st.started_order.push(batch_id);
            st.by_batch_id.get(&batch_id).cloned().unwrap_or_default()
        };
        if !script.delay.is_zero() {
            tokio::time::sleep(script.delay).await;
        }
        if let Some(msg) = script.fail_with {
            self.state
                .lock()
                .expect("scripted transformer lock")
                .completed_order
                .push(batch_id);
            return Err(anyhow!(msg));
        }
        let out = self.inner.apply_events_async(batch_id, events).await?;
        self.state
            .lock()
            .expect("scripted transformer lock")
            .completed_order
            .push(batch_id);
        Ok(out)
    }
}

/// Per-batch script for [`ScriptedExternalTransport`].
#[derive(Debug, Clone)]
pub struct ExternalBatchScript {
    /// Delay before the response becomes readable.
    pub delay: Duration,
    /// If set, respond with this error header (no items).
    pub error: Option<String>,
    /// If set, echo this batch_id instead of the request's (mismatch tests).
    pub echo_batch_id: Option<u64>,
    /// If true, omit batch_id from the logical response (surfaced as error).
    pub missing_batch_id: bool,
    /// Optional item rewrite: when true, append `"-x"` to UTF-8 payloads.
    pub mutate_payload: bool,
}

impl Default for ExternalBatchScript {
    fn default() -> Self {
        Self {
            delay: Duration::ZERO,
            error: None,
            echo_batch_id: None,
            missing_batch_id: false,
            mutate_payload: false,
        }
    }
}

impl ExternalBatchScript {
    /// Echo items after `delay`.
    pub fn echo_after(delay: Duration) -> Self {
        Self {
            delay,
            ..Default::default()
        }
    }

    /// Respond with a mismatched batch_id after `delay`.
    pub fn bad_batch_id_after(delay: Duration, echo: u64) -> Self {
        Self {
            delay,
            echo_batch_id: Some(echo),
            ..Default::default()
        }
    }
}

struct ScriptedExternalState {
    by_batch_id: HashMap<u64, ExternalBatchScript>,
    /// Completed responses keyed by **request** batch_id (not echoed id).
    /// Out-of-order completion is fine; waiters never steal another request's slot.
    ready: HashMap<u64, Result<WireResponse>>,
    writes: Vec<u64>,
    /// Recorded `(batch_id, kind)` for each `write_request` (wire kind proofs).
    write_kinds: Vec<(u64, crate::WireItemKind)>,
    notify: Arc<Notify>,
}

/// In-memory multiplexed [`ExternalTransport`] for apply-loop tests.
///
/// Supports out-of-order completion, delays, and scripted batch_id errors.
#[derive(Clone)]
pub struct ScriptedExternalTransport {
    state: Arc<Mutex<ScriptedExternalState>>,
}

impl ScriptedExternalTransport {
    /// Create an empty scripted transport (default: echo with no delay).
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ScriptedExternalState {
                by_batch_id: HashMap::new(),
                ready: HashMap::new(),
                writes: Vec::new(),
                write_kinds: Vec::new(),
                notify: Arc::new(Notify::new()),
            })),
        }
    }

    /// Configure behavior for a specific `batch_id`.
    pub fn on_batch(self, batch_id: u64, script: ExternalBatchScript) -> Self {
        self.state
            .lock()
            .expect("scripted external lock")
            .by_batch_id
            .insert(batch_id, script);
        self
    }

    /// Recorded write order.
    pub fn write_order(&self) -> Vec<u64> {
        self.state
            .lock()
            .expect("scripted external lock")
            .writes
            .clone()
    }

    /// Recorded `(batch_id, kind)` for each write (proves RelationChange kind, etc.).
    pub fn write_kinds(&self) -> Vec<(u64, crate::WireItemKind)> {
        self.state
            .lock()
            .expect("scripted external lock")
            .write_kinds
            .clone()
    }
}

impl Default for ScriptedExternalTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExternalTransport for ScriptedExternalTransport {
    async fn write_request(
        &self,
        batch_id: u64,
        items: &[Vec<u8>],
        kind: crate::WireItemKind,
    ) -> Result<()> {
        let (script, notify) = {
            let mut st = self.state.lock().expect("scripted external lock");
            st.writes.push(batch_id);
            st.write_kinds.push((batch_id, kind));
            let script = st.by_batch_id.get(&batch_id).cloned().unwrap_or_default();
            (script, Arc::clone(&st.notify))
        };

        let items = items.to_vec();
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            if !script.delay.is_zero() {
                tokio::time::sleep(script.delay).await;
            }
            let resp = if script.missing_batch_id {
                Err(anyhow!(
                    "invalid external response header JSON: missing field `batch_id`"
                ))
            } else if let Some(err) = script.error {
                Ok(WireResponse {
                    batch_id: script.echo_batch_id.unwrap_or(batch_id),
                    error: Some(err),
                    items: Vec::new(),
                })
            } else {
                let out_id = script.echo_batch_id.unwrap_or(batch_id);
                let items = if script.mutate_payload {
                    items
                        .into_iter()
                        .map(|mut b| {
                            b.extend_from_slice(b"-x");
                            b
                        })
                        .collect()
                } else {
                    items
                };
                Ok(WireResponse {
                    batch_id: out_id,
                    error: None,
                    items,
                })
            };
            {
                let mut st = state.lock().expect("scripted external lock");
                // Key by request id so a colliding wrong echo cannot be
                // delivered to another outstanding waiter.
                st.ready.insert(batch_id, resp);
            }
            notify.notify_waiters();
        });
        Ok(())
    }

    async fn try_read_response(&self, batch_id: u64) -> Result<Option<WireResponse>> {
        loop {
            let notify = {
                let st = self.state.lock().expect("scripted external lock");
                Arc::clone(&st.notify)
            };
            // Register waiter before checking ready to avoid lost wakeups.
            let notified = notify.notified();
            {
                let mut st = self.state.lock().expect("scripted external lock");
                if let Some(resp) = st.ready.remove(&batch_id) {
                    return resp.map(Some);
                }
            }
            notified.await;
        }
    }
}

/// In-place transform that succeeds/fails/delays by call index (sync path).
///
/// Prefer [`ScriptedTransformer`] for W≥2 out-of-order async scenarios.
#[derive(Debug, Clone)]
pub struct ScriptedInPlace {
    /// If the call index (0-based) is in this set, fail.
    pub fail_on_calls: Vec<usize>,
    calls: Arc<Mutex<usize>>,
}

impl ScriptedInPlace {
    /// Create a scripted in-place transform.
    pub fn new(fail_on_calls: Vec<usize>) -> Self {
        Self {
            fail_on_calls,
            calls: Arc::new(Mutex::new(0)),
        }
    }

    /// Number of transform invocations so far.
    pub fn call_count(&self) -> usize {
        *self.calls.lock().expect("scripted inplace lock")
    }
}

impl crate::InPlaceTransform for ScriptedInPlace {
    fn transform(
        &self,
        _table: &str,
        _id: &mut sync_core::Value,
        _fields: Option<&mut std::collections::HashMap<String, sync_core::Value>>,
    ) -> Result<()> {
        let mut c = self.calls.lock().expect("scripted inplace lock");
        let idx = *c;
        *c += 1;
        if self.fail_on_calls.contains(&idx) {
            return Err(anyhow!("ScriptedInPlace fail on call {idx}"));
        }
        Ok(())
    }
}

/// Predetermined change feed with recorded advances.
#[derive(Debug)]
pub struct ScriptedChangeFeed<P> {
    remaining: VecDeque<PositionedChange<P>>,
    /// Recorded `advance_watermark` positions in order.
    pub advances: Vec<P>,
    /// When true, [`ChangeFeed::is_finished`] is true once `remaining` is empty.
    finished_when_empty: bool,
}

impl<P> ScriptedChangeFeed<P> {
    /// Create a feed from an ordered list of positioned changes.
    pub fn new(changes: Vec<PositionedChange<P>>) -> Self {
        Self {
            remaining: changes.into(),
            advances: Vec::new(),
            finished_when_empty: true,
        }
    }

    /// Resume a feed from not-yet-advanced changes (crash/replay helper).
    pub fn from_remaining(changes: Vec<PositionedChange<P>>) -> Self {
        Self::new(changes)
    }

    /// Number of events still to yield.
    pub fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    /// Last advanced watermark position, if any.
    pub fn last_advance(&self) -> Option<&P> {
        self.advances.last()
    }
}

#[async_trait]
impl<P> ChangeFeed for ScriptedChangeFeed<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Position = P;

    async fn poll_changes(&mut self) -> Result<Vec<PositionedChange<P>>> {
        match self.remaining.pop_front() {
            Some(pc) => Ok(vec![pc]),
            None => Ok(vec![]),
        }
    }

    async fn advance_watermark(&mut self, position: P) -> Result<()> {
        self.advances.push(position);
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished_when_empty && self.remaining.is_empty()
    }
}

/// Sink failure script.
#[derive(Debug, Clone)]
pub enum SinkFailWhen {
    /// Fail on the Nth `apply_change` call (0-based).
    ApplyIndex(usize),
    /// Fail when the change id (as display) matches.
    ChangeId(String),
}

#[derive(Default)]
struct RecordingSinkState {
    applied: Vec<Change>,
    relations_applied: Vec<RelationChange>,
    rows_written: Vec<Vec<Row>>,
    relations_written: Vec<Vec<Relation>>,
    /// Sink apply order tags: `change:{id}` or `relation:{id}`.
    event_order: Vec<String>,
    apply_count: usize,
    relation_apply_count: usize,
    fail_when: Vec<SinkFailWhen>,
    /// If true, fail only once per scripted condition then succeed on retry.
    fail_once: bool,
    fired: Vec<bool>,
    /// Artificial delay before each successful `apply_change`.
    apply_delay: Duration,
    /// Notified when an apply begins (before delay/gate).
    apply_started: Option<Arc<Notify>>,
    /// If set, each apply waits for this notify before finishing (slow sink).
    apply_gate: Option<Arc<Notify>>,
}

/// Recording [`SurrealSink`] with optional scripted failures.
#[derive(Clone, Default)]
pub struct RecordingSink {
    state: Arc<Mutex<RecordingSinkState>>,
}

impl RecordingSink {
    /// Empty recording sink (always succeeds).
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail on the given conditions (checked in order against each apply).
    pub fn fail_when(self, conditions: Vec<SinkFailWhen>) -> Self {
        let n = conditions.len();
        let mut st = self.state.lock().expect("recording sink lock");
        st.fail_when = conditions;
        st.fired = vec![false; n];
        drop(st);
        self
    }

    /// Each fail condition fires at most once (supports retry/replay tests).
    pub fn fail_once(self) -> Self {
        self.state.lock().expect("recording sink lock").fail_once = true;
        self
    }

    /// Delay each successful change apply (simulates a slow sink).
    pub fn with_apply_delay(self, delay: Duration) -> Self {
        self.state.lock().expect("recording sink lock").apply_delay = delay;
        self
    }

    /// Notify when an apply starts; optionally gate completion on `gate.notified()`.
    pub fn with_apply_hold(self, started: Arc<Notify>, gate: Arc<Notify>) -> Self {
        let mut st = self.state.lock().expect("recording sink lock");
        st.apply_started = Some(started);
        st.apply_gate = Some(gate);
        drop(st);
        self
    }

    /// Applied changes in sink order.
    pub fn applied(&self) -> Vec<Change> {
        self.state
            .lock()
            .expect("recording sink lock")
            .applied
            .clone()
    }

    /// Ids of applied changes (Debug of `Value`).
    pub fn applied_ids(&self) -> Vec<String> {
        self.applied()
            .iter()
            .map(|c| format!("{:?}", c.id))
            .collect()
    }

    /// Row batches passed to `write_rows`.
    pub fn rows_written(&self) -> Vec<Vec<Row>> {
        self.state
            .lock()
            .expect("recording sink lock")
            .rows_written
            .clone()
    }

    /// Number of `apply_change` attempts (including failures).
    pub fn apply_attempts(&self) -> usize {
        self.state.lock().expect("recording sink lock").apply_count
    }

    /// Applied relation changes in sink order.
    pub fn relations_applied(&self) -> Vec<RelationChange> {
        self.state
            .lock()
            .expect("recording sink lock")
            .relations_applied
            .clone()
    }

    /// Relation batches passed to `write_relations`.
    pub fn relations_written(&self) -> Vec<Vec<Relation>> {
        self.state
            .lock()
            .expect("recording sink lock")
            .relations_written
            .clone()
    }

    /// Combined apply order as `"change:{id}"` / `"relation:{id}"` strings.
    pub fn apply_order_tags(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("recording sink lock")
            .event_order
            .clone()
    }
}

fn id_matches(change: &Change, want: &str) -> bool {
    format!("{:?}", change.id) == want || change_id_display(change) == want
}

fn change_id_display(change: &Change) -> String {
    match &change.id {
        sync_core::Value::Int64(n) => n.to_string(),
        sync_core::Value::Int32(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

#[async_trait]
impl SurrealSink for RecordingSink {
    async fn write_rows(&self, rows: &[Row]) -> Result<()> {
        self.state
            .lock()
            .expect("recording sink lock")
            .rows_written
            .push(rows.to_vec());
        Ok(())
    }

    async fn write_relations(&self, relations: &[Relation]) -> Result<()> {
        self.state
            .lock()
            .expect("recording sink lock")
            .relations_written
            .push(relations.to_vec());
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> Result<()> {
        let (delay, started, gate, should_fail) = {
            let mut st = self.state.lock().expect("recording sink lock");
            let idx = st.apply_count;
            st.apply_count += 1;

            let mut should_fail = false;
            for (i, cond) in st.fail_when.iter().enumerate() {
                let matches = match cond {
                    SinkFailWhen::ApplyIndex(n) => idx == *n,
                    SinkFailWhen::ChangeId(id) => id_matches(change, id),
                };
                if matches {
                    if st.fail_once && st.fired[i] {
                        continue;
                    }
                    st.fired[i] = true;
                    should_fail = true;
                    break;
                }
            }

            (
                st.apply_delay,
                st.apply_started.clone(),
                st.apply_gate.clone(),
                should_fail,
            )
        };

        if let Some(started) = started {
            started.notify_one();
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if let Some(gate) = gate {
            gate.notified().await;
        }

        if should_fail {
            return Err(anyhow!(
                "RecordingSink scripted fail on apply index {}",
                self.state.lock().expect("recording sink lock").apply_count - 1
            ));
        }

        let mut st = self.state.lock().expect("recording sink lock");
        st.event_order
            .push(format!("change:{}", change_id_display(change)));
        st.applied.push(change.clone());
        Ok(())
    }

    async fn apply_relation_change(&self, change: &RelationChange) -> Result<()> {
        let mut st = self.state.lock().expect("recording sink lock");
        st.relation_apply_count += 1;
        st.event_order.push(format!(
            "relation:{}",
            relation_id_display(&change.relation.id)
        ));
        st.relations_applied.push(change.clone());
        Ok(())
    }
}

fn relation_id_display(id: &sync_core::Value) -> String {
    match id {
        sync_core::Value::Int64(n) => n.to_string(),
        sync_core::Value::Int32(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

/// Scripted [`SourceDriver`] for runtime tests (work + control + stop + checkpoint).
pub struct ScriptedSourceDriver<P> {
    /// Remaining work items (drained by poll_work).
    pub remaining: Vec<PositionedEvent<P>>,
    /// Recorded advance_watermark positions.
    pub advances: Vec<P>,
    /// Recorded persist_checkpoint positions (sink-safe only).
    pub persisted: Vec<P>,
    /// Signals returned once from the next `between_events` call, then cleared.
    pub pending_signals: Vec<crate::ControlSignal>,
    /// Count of `on_schema_refresh` invocations.
    pub schema_refresh_count: u64,
    /// Tables passed to `on_adhoc_snapshot`.
    pub adhoc_snapshots: Vec<Vec<String>>,
    /// Optional stop reason (checked each loop).
    pub stop: Option<crate::StopReason>,
    /// Checkpoint policy.
    pub policy: crate::CheckpointPolicy,
    /// Optional position returned from `read_progress_for_persist`.
    pub read_progress: Option<P>,
    /// When true, `is_finished` once remaining is empty.
    pub finished_when_empty: bool,
    /// How many times `poll_work` was called.
    pub poll_count: u64,
    /// After this many polls, set `stop` to Cancelled (0 = never).
    pub cancel_after_polls: u64,
    /// Sum of counts passed to `note_sunk_events`.
    pub sunk_events: u64,
}

impl<P> ScriptedSourceDriver<P> {
    /// Finite driver that finishes when remaining is empty.
    pub fn new(items: Vec<PositionedEvent<P>>) -> Self {
        Self {
            remaining: items,
            advances: Vec::new(),
            persisted: Vec::new(),
            pending_signals: Vec::new(),
            schema_refresh_count: 0,
            adhoc_snapshots: Vec::new(),
            stop: None,
            policy: crate::CheckpointPolicy::PersistAfterAdvance,
            read_progress: None,
            finished_when_empty: true,
            poll_count: 0,
            cancel_after_polls: 0,
            sunk_events: 0,
        }
    }

    /// Cancel after `n` poll_work calls.
    pub fn cancel_after_polls(mut self, n: u64) -> Self {
        self.cancel_after_polls = n;
        self
    }

    /// Queue control signals for the next between_events.
    pub fn with_signals(mut self, signals: Vec<crate::ControlSignal>) -> Self {
        self.pending_signals = signals;
        self
    }

    /// Use AdvanceOnly policy (no persist_checkpoint).
    pub fn advance_only(mut self) -> Self {
        self.policy = crate::CheckpointPolicy::AdvanceOnly;
        self
    }

    /// Use IntervalWhenDrained checkpoint policy.
    pub fn interval_when_drained(mut self, interval: std::time::Duration) -> Self {
        self.policy = crate::CheckpointPolicy::IntervalWhenDrained { interval };
        self
    }

    /// Provide a filtered/idle read-progress position for IntervalWhenDrained.
    pub fn with_read_progress(mut self, position: P) -> Self {
        self.read_progress = Some(position);
        self
    }
}

#[async_trait]
impl<P> SourceDriver for ScriptedSourceDriver<P>
where
    P: Clone + Send + Sync + 'static,
{
    type Position = P;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        self.poll_count += 1;
        if self.cancel_after_polls > 0 && self.poll_count >= self.cancel_after_polls {
            self.stop = Some(crate::StopReason::Cancelled);
        }
        if self.remaining.is_empty() {
            return Ok(Vec::new());
        }
        // Yield one item per poll for predictable cancel tests.
        let item = self.remaining.remove(0);
        Ok(vec![item])
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()> {
        self.advances.push(position);
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished_when_empty && self.remaining.is_empty()
    }

    async fn between_events(&mut self) -> Result<Vec<crate::ControlSignal>> {
        Ok(std::mem::take(&mut self.pending_signals))
    }

    async fn on_schema_refresh(&mut self) -> Result<()> {
        self.schema_refresh_count += 1;
        Ok(())
    }

    async fn on_adhoc_snapshot(
        &mut self,
        tables: &[String],
        apply: &dyn crate::AdhocApply,
    ) -> Result<()> {
        // Record that the runtime passed apply helpers (sink+pipeline path).
        let _ = apply.apply_opts();
        self.adhoc_snapshots.push(tables.to_vec());
        Ok(())
    }

    fn stop_reason(&self) -> Option<crate::StopReason> {
        self.stop.clone()
    }

    fn checkpoint_policy(&self) -> crate::CheckpointPolicy {
        self.policy
    }

    async fn persist_checkpoint(&mut self, position: Self::Position) -> Result<()> {
        self.persisted.push(position);
        Ok(())
    }

    async fn read_progress_for_persist(&mut self) -> Result<Option<Self::Position>> {
        Ok(self.read_progress.clone())
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.sunk_events = self.sunk_events.saturating_add(count);
    }
}
