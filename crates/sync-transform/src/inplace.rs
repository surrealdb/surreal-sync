//! In-place transform trait and passthrough implementation.
//!
//! # Schema-aware transforms (FK → record links)
//!
//! [`InPlaceTransform`] does not take schema itself. Library code may construct
//! a transform with a schema/catalog (e.g. wrap `postgresql::fk_transform`
//! helpers) and [`crate::Pipeline::push_inplace`] it. The apply path runs that
//! stage like any other in-place transform — including over
//! [`sync_core::Relation`] / [`sync_core::RelationChange`]
//! via the relation methods below.
//!
//! Full join-table → relation **source** logic may remain in PostgreSQL (or
//! other) source crates; relation **edges are first-class in the apply engine**.

use anyhow::Result;
use std::collections::HashMap;
use sync_core::{Change, Relation, RelationChange, Row, Value};

/// Mutate-only, same-length transform over sync docs.
///
/// This is the **only** in-process mutation primitive. Filter / fan-out belong
/// in [`crate::ExternalTransform`] or outside this trait.
///
/// Implement [`transform`](Self::transform) once for the shared document surface
/// (`table` / `id` / optional `fields`). [`transform_row`](Self::transform_row)
/// and [`transform_change`](Self::transform_change) default to calling it.
/// Deletes arrive with `fields == None`.
///
/// # Slice defaults
///
/// [`transform_rows_inplace`](Self::transform_rows_inplace) and
/// [`transform_changes_inplace`](Self::transform_changes_inplace) loop over
/// items by default. Override only when a batched implementation is faster.
///
/// Relation methods default to no-op so row-only transforms stay trivial.
pub trait InPlaceTransform: Send + Sync {
    /// Mutate the shared document surface (id + optional field map).
    ///
    /// `fields` is `None` for delete changes.
    fn transform(
        &self,
        table: &str,
        id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()>;

    /// Transform a single row in place.
    fn transform_row(&self, row: &mut Row) -> Result<()> {
        self.transform(&row.table, &mut row.id, Some(&mut row.fields))
    }

    /// Transform a single change in place (including deletes).
    fn transform_change(&self, change: &mut Change) -> Result<()> {
        self.transform(&change.table, &mut change.id, change.fields.as_mut())
    }

    /// Transform a single relation in place. Default: no-op.
    fn transform_relation(&self, _relation: &mut Relation) -> Result<()> {
        Ok(())
    }

    /// Transform a single relation change in place. Default: no-op.
    fn transform_relation_change(&self, _change: &mut RelationChange) -> Result<()> {
        Ok(())
    }

    /// Transform a slice of owned rows in place (true zero-copy path).
    fn transform_rows_inplace(&self, rows: &mut [Row]) -> Result<()> {
        for row in rows {
            self.transform_row(row)?;
        }
        Ok(())
    }

    /// Transform a slice of owned changes in place (true zero-copy path).
    fn transform_changes_inplace(&self, changes: &mut [Change]) -> Result<()> {
        for change in changes {
            self.transform_change(change)?;
        }
        Ok(())
    }

    /// Transform a slice of owned relations in place. Default: loop.
    fn transform_relations_inplace(&self, relations: &mut [Relation]) -> Result<()> {
        for relation in relations {
            self.transform_relation(relation)?;
        }
        Ok(())
    }

    /// Transform a slice of owned relation changes in place. Default: loop.
    fn transform_relation_changes_inplace(&self, changes: &mut [RelationChange]) -> Result<()> {
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
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        _fields: Option<&mut HashMap<String, Value>>,
    ) -> Result<()> {
        Ok(())
    }
}
