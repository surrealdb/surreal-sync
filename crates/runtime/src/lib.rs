//! Shared runtime for surreal-sync: process setup, SurrealDB connection settings,
//! transform loading, and the apply pipeline.
//!
//! Origin `from-*` crates re-export the pieces embedders need. The stock CLI
//! may still auto-detect SurrealDB major version (linking both SDKs); the
//! documented embed path uses a pre-built sink or `run::<OneSinkType>`.
//!
//! Sink crates use the default features (no CLI argument parsing). Enable `cli`
//! only if you need [`SurrealCliOpts`].
//!
//! Enable `checkpoint_fs` (default) for [`checkpoint_fs::FilesystemStore`].
//!
//! # Sink + checkpoint typing
//!
//! - [`SinkConnect`] / [`SinkWithCheckpoints`] / [`SurrealConfig`] — defined in
//!   [`surreal_sync_core`], re-exported here for existing imports
//! - [`SinkWithCheckpoints`] opens a matching Surreal-table
//!   [`CheckpointStore`](surreal_sync_core::CheckpointStore) for
//!   `--checkpoints-surreal-table` (implemented on `Surreal2Sink` / `Surreal3Sink`)

mod config;
mod init;
mod sink_connect;
mod transforms;

/// Shared apply loop, transform pipeline, and interleaved snapshot engine.
pub mod pipeline;

/// Store checkpoints on the filesystem.
#[cfg(feature = "checkpoint_fs")]
pub mod checkpoint_fs;

#[cfg(feature = "cli")]
mod cli_opts;

#[cfg(feature = "cli")]
pub use cli_opts::SurrealCliOpts;
pub use config::SurrealConfig;
pub use init::init;
pub use sink_connect::{SinkConnect, SinkWithCheckpoints};
pub use transforms::{load_transforms_from_args, merge_inplace_boxed, merge_inplace_transforms};

// Re-exports commonly used pipeline types at the crate root.
pub use pipeline::{
    apply_changes, apply_changes_with, apply_relation_changes, apply_relation_changes_with,
    ensure_command_resolvable, load_pipeline_and_opts, load_transforms_config, parse_humantime,
    parse_transforms_toml, relation_wire_batch_id, run_adhoc_snapshot_tables,
    run_adhoc_snapshot_tables_with_transforms, run_change_feed, run_change_feed_with,
    run_interleaved_snapshot, run_interleaved_snapshot_with_resume,
    run_interleaved_snapshot_with_resume_and_transforms, run_interleaved_snapshot_with_transforms,
    run_source_runtime, run_source_runtime_with, write_relations, write_relations_with, write_rows,
    write_rows_with, AdhocApply, ApplyContext, ApplyEvent, ApplyOpts, BatchTransformer, ChangeFeed,
    ChangeFeedDriver, ChangeFeedRef, CheckpointPolicy, ChildStdioMode, CommandStageConfig,
    ConfiguredStage, ControlSignal, CowBatch, ExternalTransform, ExternalTransport, FailurePolicy,
    FlattenId, FlattenIdStageConfig, Framer, FramerKind, InPlaceTransform,
    InterleavedSnapshotCheckpoint, InterleavedSnapshotConfig, InterleavedSnapshotResult,
    ManagerCheckpointer, NdjsonFramer, NoopCheckpointer, Passthrough, PersistentChildStdio,
    Pipeline, PipelineSection, PkTuple, PositionedChange, PositionedEvent, ReconciliationEvent,
    ReconciliationPos, RelationChunkDriver, RelationChunkSource, RequestHeader, ResponseHeader,
    RetryPolicy, RowChunkDriver, RowChunkSource, RuntimeExit, SnapshotCheckpointer, SnapshotSignal,
    SnapshotTableProgress, SnapshotTransforms, SourceDriver, SourceRuntimeOpts, Stage, StdioConfig,
    StopReason, TableSpec, TransformsConfig, TransientChildStdio, WatermarkKind, WatermarkSource,
    WireItemKind, WireResponse, DEFAULT_CHUNK_SIZE, DEFAULT_FLATTEN_ID_SEPARATOR,
    RELATION_WIRE_BATCH_ID_BIT,
};

#[cfg(any(test, feature = "test-support"))]
pub use pipeline::test_support;

/// Parse a duration string like "1h", "30m", "300s", "300" into seconds.
pub fn parse_duration_to_secs(s: &str) -> anyhow::Result<i64> {
    use anyhow::Context;

    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("Empty duration string");
    }

    if let Some(num_str) = s.strip_suffix('h') {
        let hours: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid hours value: {num_str}"))?;
        return Ok(hours * 3600);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let minutes: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid minutes value: {num_str}"))?;
        return Ok(minutes * 60);
    }
    if let Some(num_str) = s.strip_suffix('s') {
        let secs: i64 = num_str
            .parse()
            .with_context(|| format!("Invalid seconds value: {num_str}"))?;
        return Ok(secs);
    }

    s.parse::<i64>()
        .with_context(|| format!("Invalid duration value: {s}"))
}
