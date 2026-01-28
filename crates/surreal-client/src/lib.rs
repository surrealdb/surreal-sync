//! SurrealDB test client trait abstraction.
//!
//! Provides the `SurrealTestClient` trait for version-agnostic testing.

mod traits;
mod version;

pub use traits::SurrealTestClient;
pub use version::SurrealSdkVersion;
