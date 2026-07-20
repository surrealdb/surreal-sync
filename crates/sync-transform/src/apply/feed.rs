//! Change feed trait: source events + checkpoint commit.

use anyhow::Result;
use sync_core::UniversalChange;

/// A CDC / incremental event plus the source position to commit after sink success.
#[derive(Debug, Clone)]
pub struct PositionedChange<P> {
    /// Universal change to transform and apply.
    pub change: UniversalChange,
    /// Source position associated with this change (checkpoint candidate).
    pub position: P,
}

impl<P> PositionedChange<P> {
    /// Construct a positioned change.
    pub fn new(change: UniversalChange, position: P) -> Self {
        Self { change, position }
    }
}

/// Source-facing incremental feed: poll events, commit after durable sink apply.
///
/// Sources do **not** batch for transforms — the apply framework owns batching,
/// the in-flight window, ordered sink apply, and contiguous commit watermark.
#[async_trait::async_trait]
pub trait ChangeFeed: Send {
    /// Checkpoint / resume position type.
    type Position: Clone + Send + Sync + 'static;

    /// Next source events (may be empty on idle).
    async fn poll_changes(&mut self) -> Result<Vec<PositionedChange<Self::Position>>>;

    /// Advance the source checkpoint after docs for this position were sunk.
    async fn commit(&mut self, position: Self::Position) -> Result<()>;

    /// Whether the feed will produce no more changes (EOF).
    ///
    /// Default: `false` (endless sources). Scripted / finite feeds override so
    /// [`crate::run_change_feed`] can exit after draining in-flight work.
    fn is_finished(&self) -> bool {
        false
    }
}
