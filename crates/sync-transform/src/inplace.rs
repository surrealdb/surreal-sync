//! In-place transform trait and passthrough implementation.

use anyhow::Result;
use sync_core::{UniversalChange, UniversalRow};

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
pub trait InPlaceTransform: Send + Sync {
    /// Transform a single row in place.
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()>;

    /// Transform a single change in place.
    fn transform_change(&self, change: &mut UniversalChange) -> Result<()>;

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
