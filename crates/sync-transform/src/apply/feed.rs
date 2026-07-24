//! Change feed trait: thin adapter over [`crate::SourceDriver`] for tests and simple row feeds.
//!
//! Prefer [`crate::SourceDriver`] + [`crate::run_source_runtime`] for production ports.
//! [`ChangeFeed`] / [`crate::run_change_feed`] remain for tests and simple row-CDC sources
//! that do not need schema refresh, ad-hoc snapshot, or other control hooks.

use crate::apply::event::{ApplyEvent, PositionedEvent};
use anyhow::Result;
use sync_core::Change;

/// A CDC / incremental row event plus the source position to advance after sink success.
///
/// Prefer [`PositionedEvent`] when the feed may also emit relation changes.
#[derive(Debug, Clone)]
pub struct PositionedChange<P> {
    /// Universal change to transform and apply.
    pub change: Change,
    /// Source position associated with this change (checkpoint candidate).
    pub position: P,
}

impl<P> PositionedChange<P> {
    /// Construct a positioned change.
    pub fn new(change: Change, position: P) -> Self {
        Self { change, position }
    }

    /// Convert to the unified [`PositionedEvent`] form.
    pub fn into_event(self) -> PositionedEvent<P> {
        PositionedEvent::new(ApplyEvent::Change(self.change), self.position)
    }
}

impl<P> From<PositionedChange<P>> for PositionedEvent<P> {
    fn from(pc: PositionedChange<P>) -> Self {
        pc.into_event()
    }
}

/// Source-facing incremental **row** feed: poll events, advance watermark after durable sink apply.
///
/// Sources do **not** batch for transforms — the shared apply loop owns batching,
/// the in-flight window, ordered sink apply, and contiguous sink-safe watermark.
///
/// For sources that also emit relation edges or need DDL / cancel / checkpoint
/// hooks, implement [`crate::SourceDriver`] instead (or wrap this feed with
/// [`crate::ChangeFeedDriver`]).
#[async_trait::async_trait]
pub trait ChangeFeed: Send {
    /// Checkpoint / resume position type.
    type Position: Clone + Send + Sync + 'static;

    /// Next source events (may be empty on idle).
    async fn poll_changes(&mut self) -> Result<Vec<PositionedChange<Self::Position>>>;

    /// Mark `position` sink-safe. May be in-memory only, broker offset commit, or
    /// both — durable store writes belong in
    /// [`SourceDriver::persist_checkpoint`](crate::SourceDriver::persist_checkpoint)
    /// unless policy is [`CheckpointPolicy::AdvanceOnly`](crate::CheckpointPolicy::AdvanceOnly).
    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()>;

    /// Whether the feed will produce no more changes (EOF).
    ///
    /// Default: `false` (endless sources). Scripted / finite feeds override so
    /// [`crate::run_change_feed`] can exit after draining in-flight work.
    fn is_finished(&self) -> bool {
        false
    }
}
