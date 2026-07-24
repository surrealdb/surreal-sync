//! Connect a SurrealDB sink (and matching checkpoint store) from [`SurrealConfig`].
//!
//! Definitions live in [`surreal_sync_core`]; this module re-exports them for
//! existing `surreal_sync_runtime::{SinkConnect, SinkWithCheckpoints}` imports.

pub use surreal_sync_core::{SinkConnect, SinkWithCheckpoints};
