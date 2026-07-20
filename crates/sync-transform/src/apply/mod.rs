//! Change-feed apply framework: windowed transform, ordered sink, contiguous commit.

mod feed;
mod opts;
mod runtime;
mod transform;

pub use feed::{ChangeFeed, PositionedChange};
pub use opts::{ApplyOpts, FailurePolicy};
pub use runtime::{
    apply_changes, apply_changes_with, run_change_feed, run_change_feed_with, write_rows,
    write_rows_with, ApplyContext,
};
pub use transform::BatchTransformer;
