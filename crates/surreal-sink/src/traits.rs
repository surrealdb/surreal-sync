//! SurrealSink trait definition.
//!
//! This trait abstracts over SurrealDB SDK version differences, allowing
//! source crates to be compiled against a single interface that works
//! with both v2 and v3 servers.

use anyhow::Result;
use sync_core::{UniversalChange, UniversalRelation, UniversalRow};

/// Trait for writing data to SurrealDB.
///
/// This trait provides version-independent operations for writing data
/// to SurrealDB. Implementations handle the SDK-specific details of
/// converting universal types to SurrealDB types.
///
/// # Usage Pattern
///
/// Source crates use generics for zero-cost dispatch:
///
/// ```ignore
/// pub async fn run_full_sync<S: SurrealSink>(
///     surreal: &S,
///     opts: &SourceOpts,
/// ) -> Result<()> {
///     // All calls here are statically dispatched after monomorphization
///     surreal.write_universal_rows(&batch).await?;
/// }
/// ```
///
/// The CLI entry point branches once based on SDK version, and after
/// that all code is monomorphized for the specific implementation.
#[async_trait::async_trait]
pub trait SurrealSink: Send + Sync {
    /// Write a batch of universal rows to SurrealDB.
    ///
    /// Converts each `UniversalRow` to the appropriate SurrealDB record
    /// format and writes it using UPSERT semantics.
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> Result<()>;

    /// Write a batch of universal relations to SurrealDB.
    ///
    /// Converts each `UniversalRelation` to a SurrealDB relation (graph edge)
    /// and writes it using RELATE semantics.
    async fn write_universal_relations(&self, relations: &[UniversalRelation]) -> Result<()>;

    /// Apply a single universal change (create/update/delete) to SurrealDB.
    ///
    /// Converts the `UniversalChange` to the appropriate SurrealDB operation:
    /// - Create/Update: UPSERT the record
    /// - Delete: DELETE the record
    async fn apply_universal_change(&self, change: &UniversalChange) -> Result<()>;
}
