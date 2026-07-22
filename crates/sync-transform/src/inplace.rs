//! In-place transform trait and passthrough implementation.
//!
//! # Schema-aware transforms (FK → record links)
//!
//! [`InPlaceTransform`] does not take schema itself. Library code may construct
//! a transform with a schema/catalog (e.g. wrap `postgresql::fk_transform`
//! helpers) and [`crate::Pipeline::push_inplace`] it. The apply path runs that
//! stage like any other in-place transform — including over
//! [`sync_core::UniversalRelation`] / [`sync_core::UniversalRelationChange`]
//! via the relation methods below.
//!
//! Full join-table → relation **source** logic may remain in PostgreSQL (or
//! other) source crates; relation **edges are first-class in the apply engine**.

use anyhow::Result;
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow};

/// Mutate-only, same-length transform over universal docs.
///
/// This is the **only** in-process mutation primitive. Filter / fan-out belong
/// in [`crate::ExternalTransform`] or outside this trait.
///
/// # Slice defaults
///
/// [`transform_rows_inplace`](Self::transform_rows_inplace) and
/// [`transform_changes_inplace`](Self::transform_changes_inplace) loop over
/// items by default. Override only when a batched implementation is faster.
///
/// Relation methods default to no-op so row-only transforms stay trivial.
pub trait InPlaceTransform: Send + Sync {
    /// Transform a single row in place.
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()>;

    /// Transform a single change in place.
    fn transform_change(&self, change: &mut UniversalChange) -> Result<()>;

    /// Transform a single relation in place. Default: no-op.
    fn transform_relation(&self, _relation: &mut UniversalRelation) -> Result<()> {
        Ok(())
    }

    /// Transform a single relation change in place. Default: no-op.
    fn transform_relation_change(&self, _change: &mut UniversalRelationChange) -> Result<()> {
        Ok(())
    }

    /// Transform a slice of owned rows in place (true zero-copy path).
    fn transform_rows_inplace(&self, rows: &mut [UniversalRow]) -> Result<()> {
        for row in rows {
            self.transform_row(row)?;
        }
        Ok(())
    }

    /// Transform a slice of owned changes in place (true zero-copy path).
    fn transform_changes_inplace(&self, changes: &mut [UniversalChange]) -> Result<()> {
        for change in changes {
            self.transform_change(change)?;
        }
        Ok(())
    }

    /// Transform a slice of owned relations in place. Default: loop.
    fn transform_relations_inplace(&self, relations: &mut [UniversalRelation]) -> Result<()> {
        for relation in relations {
            self.transform_relation(relation)?;
        }
        Ok(())
    }

    /// Transform a slice of owned relation changes in place. Default: loop.
    fn transform_relation_changes_inplace(
        &self,
        changes: &mut [UniversalRelationChange],
    ) -> Result<()> {
        for change in changes {
            self.transform_relation_change(change)?;
        }
        Ok(())
    }
}

/// No-op [`InPlaceTransform`]. Useful for tests and library completeness.
///
/// Operators configuring surreal-sync via TOML should omit transforms entirely
/// for identity; they do not need an explicit passthrough stage.
///
/// Pushing `Passthrough` into a [`crate::Pipeline`] does **not** make
/// [`crate::Pipeline::is_identity`] true — that requires an empty stage list.
/// Config loading should collapse passthrough-only TOML to empty.
#[derive(Debug, Default, Clone, Copy)]
pub struct Passthrough;

impl InPlaceTransform for Passthrough {
    fn transform_row(&self, _row: &mut UniversalRow) -> Result<()> {
        Ok(())
    }

    fn transform_change(&self, _change: &mut UniversalChange) -> Result<()> {
        Ok(())
    }
}
