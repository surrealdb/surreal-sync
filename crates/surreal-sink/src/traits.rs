//! SurrealSink trait definition.
//!
//! This trait abstracts over SurrealDB SDK version differences, allowing
//! source crates to be compiled against a single interface that works
//! with both v2 and v3 servers.

use anyhow::Result;
use sync_core::{Change, Relation, RelationChange, Row};

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
///     surreal.write_rows(&batch).await?;
/// }
/// ```
///
/// The CLI entry point branches once based on SDK version, and after
/// that all code is monomorphized for the specific implementation.
#[async_trait::async_trait]
pub trait SurrealSink: Send + Sync {
    /// Write a batch of universal rows to SurrealDB.
    ///
    /// Converts each `Row` to the appropriate SurrealDB record
    /// format and writes it using UPSERT semantics.
    async fn write_rows(&self, rows: &[Row]) -> Result<()>;

    /// Write a batch of universal relations to SurrealDB.
    ///
    /// Converts each `Relation` to a SurrealDB relation (graph edge)
    /// and writes it using RELATE semantics.
    async fn write_relations(&self, relations: &[Relation]) -> Result<()>;

    /// Apply a single universal change (create/update/delete) to SurrealDB.
    ///
    /// Converts the `Change` to the appropriate SurrealDB operation:
    /// - Create/Update: UPSERT the record
    /// - Delete: DELETE the record
    async fn apply_change(&self, change: &Change) -> Result<()>;

    /// Apply a single relation change (create/update/delete) to SurrealDB.
    ///
    /// Converts the `RelationChange` to the appropriate SurrealDB operation:
    /// - Create/Update: RELATE the edge
    /// - Delete: DELETE the relation
    async fn apply_relation_change(&self, change: &RelationChange) -> Result<()>;
}
