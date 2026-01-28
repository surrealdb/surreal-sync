//! SurrealDB sink trait abstraction.
//!
//! This crate defines the `SurrealSink` trait that abstracts over SurrealDB SDK
//! version differences. Both `surreal2-sink` and `surreal3-sink` implement this
//! trait, allowing source crates to write version-independent code.
//!
//! The trait uses sync-core universal types (UniversalRow, UniversalRelation,
//! UniversalChange) to avoid coupling to specific SurrealDB SDK types.

mod traits;
mod version;

pub use traits::SurrealSink;
pub use version::SurrealSdkVersion;
