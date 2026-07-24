//! Traits for connecting a concrete Surreal sink (and its matching checkpoint store)
//! from [`SurrealConfig`].

use super::{SurrealConfig, SurrealSink};
use crate::CheckpointStore;
use async_trait::async_trait;

/// Connect a single Surreal sink implementation from plain config.
///
/// In your app, pick one sink type, e.g. `run::<Surreal3Sink>(…)`.
/// The stock CLI may still auto-detect and branch across both SDKs.
#[async_trait]
pub trait SinkConnect: SurrealSink + Sized {
    /// Connect using endpoint / credentials / ns / db from [`SurrealConfig`].
    async fn connect(config: &SurrealConfig) -> anyhow::Result<Self>;
}

/// Sink that can open a SurrealDB-table [`CheckpointStore`] for the same SDK major.
///
/// Embedders pick a sink crate (`surreal-sync-surreal` with `v2` / `v3`);
/// that type carries the correct checkpoint backend. Origin `run` / `run_sync` APIs
/// call this when `--checkpoints-surreal-table` is set — embedders never construct
/// stores or clients by hand.
///
/// Filesystem checkpoints (`--checkpoint-dir`) are handled by the source crate, not this trait.
pub trait SinkWithCheckpoints: SinkConnect {
    /// Checkpoint store that matches this sink's SurrealDB SDK major.
    type CheckpointStore: CheckpointStore;

    /// Build a table-backed checkpoint store reusing this connected sink's client.
    fn table_checkpoints(&self, table: String) -> Self::CheckpointStore;
}
