//! In-place transform trait re-exports and passthrough.
//!
//! Trait definitions live in [`surreal_sync_core`]. Concrete pipeline stages
//! (`FlattenId`, `Pipeline`, …) remain in this crate.

pub use surreal_sync_core::{InPlaceTransform, Passthrough};
