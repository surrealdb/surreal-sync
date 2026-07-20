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

mod cow;
mod inplace;
mod pipeline;

pub use cow::CowBatch;
pub use inplace::{InPlaceTransform, Passthrough};
pub use pipeline::{ExternalTransform, Pipeline, Stage};

#[cfg(test)]
mod tests;
