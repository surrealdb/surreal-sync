//! SurrealDB sink trait abstraction.
//!
//! Defines [`SurrealSink`] so origin crates stay SDK-version independent.
//! Implementations live in `surreal-sync-surreal` (`v2` / `v3` features).

mod config;
mod connect;
mod traits;
mod version;

pub use config::SurrealConfig;
pub use connect::{SinkConnect, SinkWithCheckpoints};
pub use traits::SurrealSink;
pub use version::SurrealSdkVersion;
