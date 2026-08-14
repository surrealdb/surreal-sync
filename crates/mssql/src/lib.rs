//! SQL Server type mapping and `from mssql` origin for surreal-sync.
//!
//! # Embed
//!
//! With the `from_mssql` feature, embedders use only:
//!
//! ```ignore
//! use surreal_sync_mssql::{run, FlattenId, InPlaceTransform, Value};
//! // or: use surreal_sync_mssql::from_mssql::{run, FlattenId, InPlaceTransform, Value};
//! ```

#[cfg(feature = "types")]
pub mod types;

#[cfg(feature = "from_mssql")]
pub mod from_mssql;

/// Crate-root sugar for the public embed surface (same four items as
/// [`from_mssql`]).
#[cfg(feature = "from_mssql")]
pub use from_mssql::{run, FlattenId, InPlaceTransform, Value};
