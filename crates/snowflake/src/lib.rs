//! Snowflake type conversions and from_snowflake origin for surreal-sync.
//!
//! # Embed surface
//!
//! With the `from_snowflake` feature, embedders use only:
//!
//! ```ignore
//! use surreal_sync_snowflake::{run, FlattenId, InPlaceTransform, Value};
//! // or: use surreal_sync_snowflake::from_snowflake::{run, FlattenId, InPlaceTransform, Value};
//! ```

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_snowflake")]
pub mod from_snowflake;

/// Crate-root sugar for the public embed surface (same four items as
/// [`from_snowflake`]).
#[cfg(feature = "from_snowflake")]
pub use from_snowflake::{run, FlattenId, InPlaceTransform, Value};
