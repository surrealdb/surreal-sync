//! Transform pipeline framework for surreal-sync.
//!
//! Layering (Phase 1):
//!
//! ```text
//! InPlaceTransform          ← core (per-item + slice helpers)
//!         ↑
//! CowBatch::apply_inplace   ← adapter (Arc COW sharing via make_mut)
//!         ↑
//! Pipeline                  ← InPlace | External (stub)
//! ```
//!
//! An empty [`Pipeline`] is identity: docs pass through with **zero** transform
//! dispatch overhead (`is_identity()` short-circuits before any stage call).
//! Phase 2's apply framework must gate on [`Pipeline::is_identity`] for that
//! hot path — a lone [`Passthrough`] stage is *not* identity.

mod cow;
mod inplace;
mod pipeline;

pub use cow::CowBatch;
pub use inplace::{InPlaceTransform, Passthrough};
pub use pipeline::{ExternalTransform, Pipeline, Stage};

#[cfg(test)]
mod tests;

// Compile-time Send + Sync assertions for types used across async apply paths.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}

    fn check() {
        assert_send_sync::<Pipeline>();
        assert_send_sync::<CowBatch<sync_core::UniversalRow>>();
        assert_send_sync::<CowBatch<sync_core::UniversalChange>>();
    }

    let _ = check;
};
