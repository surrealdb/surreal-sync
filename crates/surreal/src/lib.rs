//! SurrealDB helpers for surreal-sync.
//!
//! # Features
//!
//! | Feature | Default | What it enables |
//! |---------|---------|-----------------|
//! | `v3` | yes | SurrealDB 3 sink (`Surreal3Sink`), types, checkpoint |
//! | `v2` | no | SurrealDB 2 sink (`Surreal2Sink`), types, checkpoint |
//! | `reqwest` | no | HTTP version detection, test containers, agnostic test client |
//!
//! Default is SurrealDB 3 only. For SurrealDB 2: `default-features = false,
//! features = ["v2"]`. The CLI enables both so it can detect the server
//! version; apps should enable exactly one.
//!
//! ```ignore
//! use surreal_sync_surreal::Surreal3Sink; // feature = "v3" (default)
//! ```

#[cfg(feature = "reqwest")]
pub mod client;

#[cfg(feature = "reqwest")]
pub mod version;

#[cfg(feature = "v2")]
pub mod v2;

#[cfg(feature = "v3")]
pub mod v3;

#[cfg(feature = "v2")]
pub use v2::{Surreal2Sink, Surreal2Store};

#[cfg(feature = "v2")]
pub use v2::client::Surreal2Client;

#[cfg(feature = "v3")]
pub use v3::{Surreal3Sink, Surreal3Store};

#[cfg(feature = "v3")]
pub use v3::client::Surreal3Client;
