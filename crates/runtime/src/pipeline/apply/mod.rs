//! Shared apply loop: windowed transform, ordered sink, contiguous watermark advance.
//!
//! # API hierarchy
//!
//! 1. **[`SourceDriver`] / [`run_source_runtime`]** — production incremental loop
//! 2. **[`ApplyContext`]** — library / custom loops; rows + changes + **relations**
//! 3. **[`ChangeFeed`] / [`run_change_feed`]** — thin adapter for tests / simple row feeds

mod event;
mod feed;
mod opts;
mod row_chunk;
mod runtime;
mod source_driver;
mod transform;

pub use event::{ApplyEvent, PositionedEvent};
pub use feed::{ChangeFeed, PositionedChange};
pub use opts::{ApplyOpts, FailurePolicy};
pub use row_chunk::{RelationChunkDriver, RelationChunkSource, RowChunkDriver, RowChunkSource};
pub use runtime::{
    apply_changes, apply_changes_with, apply_relation_changes, apply_relation_changes_with,
    run_change_feed, run_change_feed_with, write_relations, write_relations_with, write_rows,
    write_rows_with, ApplyContext,
};
pub use source_driver::{
    run_source_runtime, run_source_runtime_with, AdhocApply, ChangeFeedDriver, ChangeFeedRef,
    CheckpointPolicy, ControlSignal, RuntimeExit, SourceDriver, SourceRuntimeOpts, StopReason,
};
pub use transform::BatchTransformer;
