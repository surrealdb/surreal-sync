//! Transform pipeline framework for surreal-sync.
//!
//! Layering:
//!
//! ```text
//! InPlaceTransform          ← core (per-item + slice helpers)
//!         ↑
//! CowBatch::apply_inplace   ← adapter (Arc COW sharing via make_mut)
//!         ↑
//! Pipeline                  ← InPlace | External (stub)
//!         ↑
//! ApplyContext / run_change_feed / write_rows   ← ChangeFeed framework
//! ```
//!
//! An empty [`Pipeline`] is identity: docs pass through with **zero** transform
//! dispatch overhead (`is_identity()` short-circuits before any stage call).
//! The apply framework gates on [`Pipeline::is_identity`] for that hot path —
//! a lone [`Passthrough`] stage is *not* identity.

mod apply;
mod cow;
mod inplace;
mod pipeline;

pub use apply::{
    run_change_feed, run_change_feed_with, write_rows, write_rows_with, ApplyContext, ApplyOpts,
    BatchTransformer, ChangeFeed, FailurePolicy, PositionedChange,
};
pub use cow::CowBatch;
pub use inplace::{InPlaceTransform, Passthrough};
pub use pipeline::{ExternalTransform, Pipeline, Stage};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod apply_tests;

// Compile-time Send + Sync assertions for types used across async apply paths.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}

    fn check() {
        assert_send_sync::<Pipeline>();
        assert_send_sync::<CowBatch<sync_core::UniversalRow>>();
        assert_send_sync::<CowBatch<sync_core::UniversalChange>>();
        assert_send_sync::<ApplyOpts>();
    }

    let _ = check;
};
