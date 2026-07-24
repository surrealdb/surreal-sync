//! Shared apply loop for surreal-sync: transform → ordered SurrealDB write → advance watermark.
//!
//! # API hierarchy (source authors / embedders)
//!
//! 1. **[`SourceDriver`] / [`run_source_runtime`]** — production incremental loop
//!    (schema refresh, ad-hoc snapshot, cancel/deadline, sink-safe `persist_checkpoint`)
//! 2. **[`ApplyContext`]** — push/flush row **and** relation events; shared
//!    max_in_flight window, ordered sink, sink-safe positions, poison/discard
//! 3. **[`ChangeFeed`] / [`run_change_feed`]** — thin adapter for tests and simple
//!    row-only feeds (no-op defaults for the rest)
//!
//! Layering:
//!
//! ```text
//! InPlaceTransform          ← core (rows / changes / relations)
//!         ↑
//! CowBatch::apply_inplace   ← adapter (Arc COW sharing via make_mut)
//!         ↑
//! Pipeline                  ← InPlace | External (child-stdio)
//!         ↑
//! ApplyContext / write_rows / write_relations
//!         ↑
//! SourceDriver / run_source_runtime   ← production
//!         ↑
//! ChangeFeed / run_change_feed        ← thin adapter (tests / simple feeds)
//! ```
//!
//! An empty [`Pipeline`] is identity: docs pass through with **no** transform
//! stage dispatch. The apply path gates on
//! [`BatchTransformer::is_identity`] (for [`Pipeline`], equivalent to
//! [`Pipeline::is_identity`] — only an empty stage list). A lone
//! [`Passthrough`] stage is *not* identity.
//!
//! # FK / schema-aware transforms
//!
//! Construct an [`InPlaceTransform`] with schema (FK → record links, etc.) and
//! [`Pipeline::push_inplace`]. Relation edges are first-class in the apply
//! engine; full join-table→relation conversion may remain in a source crate.

mod apply;
mod config;
mod cow;
mod external;
mod flatten_id;
mod framer;
mod inplace;
pub mod interleaved;
// Nested under `runtime::pipeline` after absorbing the former pipeline crate.
#[allow(clippy::module_inception)]
mod pipeline;

pub use apply::{
    apply_changes, apply_changes_with, apply_relation_changes, apply_relation_changes_with,
    run_change_feed, run_change_feed_with, run_source_runtime, run_source_runtime_with,
    write_relations, write_relations_with, write_rows, write_rows_with, AdhocApply, ApplyContext,
    ApplyEvent, ApplyOpts, BatchTransformer, ChangeFeed, ChangeFeedDriver, ChangeFeedRef,
    CheckpointPolicy, ControlSignal, FailurePolicy, PositionedChange, PositionedEvent,
    RelationChunkDriver, RelationChunkSource, RowChunkDriver, RowChunkSource, RuntimeExit,
    SourceDriver, SourceRuntimeOpts, StopReason,
};
pub use config::{
    ensure_command_resolvable, load_pipeline_and_opts, load_transforms_config, parse_humantime,
    parse_transforms_toml, CommandStageConfig, ConfiguredStage, FlattenIdStageConfig,
    PipelineSection, StdioConfig, TransformsConfig,
};
pub use cow::CowBatch;
pub use external::{
    relation_wire_batch_id, ChildStdioMode, ExternalTransform, ExternalTransport,
    PersistentChildStdio, RequestHeader, ResponseHeader, RetryPolicy, TransientChildStdio,
    WireItemKind, WireResponse, RELATION_WIRE_BATCH_ID_BIT,
};
pub use flatten_id::{FlattenId, DEFAULT_FLATTEN_ID_SEPARATOR};
pub use framer::{Framer, FramerKind, NdjsonFramer};
pub use inplace::{InPlaceTransform, Passthrough};
pub use interleaved::{
    run_adhoc_snapshot_tables, run_adhoc_snapshot_tables_with_transforms, run_interleaved_snapshot,
    run_interleaved_snapshot_with_resume, run_interleaved_snapshot_with_resume_and_transforms,
    run_interleaved_snapshot_with_transforms, InterleavedSnapshotCheckpoint,
    InterleavedSnapshotConfig, InterleavedSnapshotResult, ManagerCheckpointer, NoopCheckpointer,
    PkTuple, ReconciliationEvent, ReconciliationPos, SnapshotCheckpointer, SnapshotSignal,
    SnapshotTableProgress, SnapshotTransforms, TableSpec, WatermarkKind, WatermarkSource,
    DEFAULT_CHUNK_SIZE,
};
pub use pipeline::{Pipeline, Stage};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod apply_tests;

#[cfg(test)]
mod source_driver_tests;

#[cfg(test)]
mod external_tests;

// Compile-time Send + Sync assertions for types used across async apply paths.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}

    fn check() {
        assert_send_sync::<Pipeline>();
        assert_send_sync::<CowBatch<surreal_sync_core::Row>>();
        assert_send_sync::<CowBatch<surreal_sync_core::Change>>();
        assert_send_sync::<CowBatch<surreal_sync_core::Relation>>();
        assert_send_sync::<ApplyOpts>();
        assert_send_sync::<ExternalTransform>();
        assert_send_sync::<NdjsonFramer>();
    }

    let _ = check;
};
