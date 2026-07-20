//! Ordered transform pipeline: in-place stages and external boundary.

use crate::external::ExternalTransform;
use crate::inplace::InPlaceTransform;
use anyhow::{bail, Result};
use std::sync::Arc;
use sync_core::{UniversalChange, UniversalRow};

/// A single pipeline stage.
#[derive(Clone)]
pub enum Stage {
    /// In-process mutate-only transform.
    InPlace(Arc<dyn InPlaceTransform>),
    /// External worker boundary (child-stdio NDJSON).
    External(ExternalTransform),
}

impl std::fmt::Debug for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stage::InPlace(_) => f.write_str("InPlace(_)"),
            Stage::External(ext) => f.debug_tuple("External").field(ext).finish(),
        }
    }
}

/// Ordered list of transform stages.
///
/// An empty pipeline is **identity**: [`is_identity`](Self::is_identity) is
/// true and apply helpers return immediately without dispatching any stage.
///
/// # Identity vs passthrough
///
/// Pushing a lone [`crate::Passthrough`] via [`push_inplace`](Self::push_inplace)
/// does **not** make [`is_identity`](Self::is_identity) return `true` — the
/// pipeline still has a stage and will dispatch into it. Config loading (Phase
/// later) must collapse passthrough-only TOML to an empty pipeline so the CLI
/// hot path stays zero-dispatch.
///
/// # Apply framework hot path
///
/// The ChangeFeed / `write_rows` apply path gates on
/// [`crate::BatchTransformer::is_identity`] (implemented for [`Pipeline`] via
/// [`is_identity`](Self::is_identity)). Only an empty stage list is identity —
/// not “stages happen to be no-ops.”
#[derive(Debug, Default, Clone)]
pub struct Pipeline {
    stages: Vec<Stage>,
}

impl Pipeline {
    /// Create an empty (identity) pipeline.
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    /// Whether this pipeline has no stages (identity / zero dispatch overhead).
    ///
    /// Only an empty stage list is identity. A pipeline that contains only
    /// [`crate::Passthrough`] still returns `false` here — see type-level docs.
    pub fn is_identity(&self) -> bool {
        self.stages.is_empty()
    }

    /// Number of stages (0 = identity).
    pub fn len(&self) -> usize {
        self.stages.len()
    }

    /// Whether there are no stages.
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Borrow the stage list.
    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }

    /// Append an in-place transform stage (library / embedder API).
    ///
    /// Note: appending [`crate::Passthrough`] alone does not yield an identity
    /// pipeline ([`is_identity`](Self::is_identity) stays `false`).
    pub fn push_inplace<T>(&mut self, transform: T)
    where
        T: InPlaceTransform + 'static,
    {
        self.stages
            .push(Stage::InPlace(Arc::new(transform)));
    }

    /// Append a pre-boxed in-place stage.
    pub fn push_inplace_arc(&mut self, transform: Arc<dyn InPlaceTransform>) {
        self.stages.push(Stage::InPlace(transform));
    }

    /// Append an external (child-stdio) stage.
    pub fn push_external(&mut self, external: ExternalTransform) {
        self.stages.push(Stage::External(external));
    }

    /// Transform owned rows in place (sync path — **in-place stages only**).
    ///
    /// Empty pipeline: no-op with no stage dispatch. External stages are not
    /// supported here; use [`crate::BatchTransformer::transform_rows`] (async).
    pub fn transform_rows_inplace(&self, rows: &mut [UniversalRow]) -> Result<()> {
        if self.is_identity() {
            return Ok(());
        }
        for stage in &self.stages {
            match stage {
                Stage::InPlace(t) => t.transform_rows_inplace(rows)?,
                Stage::External(_) => {
                    bail!(
                        "External transforms require the async BatchTransformer path \
                         (transform_rows); sync inplace apply is in-place-only"
                    )
                }
            }
        }
        Ok(())
    }

    /// Transform owned changes in place (sync path — **in-place stages only**).
    ///
    /// Empty pipeline: no-op with no stage dispatch. External stages are not
    /// supported here; use [`crate::BatchTransformer::transform_changes`] (async).
    pub fn transform_changes_inplace(&self, changes: &mut [UniversalChange]) -> Result<()> {
        if self.is_identity() {
            return Ok(());
        }
        for stage in &self.stages {
            match stage {
                Stage::InPlace(t) => t.transform_changes_inplace(changes)?,
                Stage::External(_) => {
                    bail!(
                        "External transforms require the async BatchTransformer path \
                         (transform_changes); sync inplace apply is in-place-only"
                    )
                }
            }
        }
        Ok(())
    }

    /// Consume an owned row batch, transform in place, and return it.
    ///
    /// Preferred sync-framework path for **in-place-only** pipelines: empty
    /// pipeline is a pure move with no transform dispatch.
    pub fn apply_rows(&self, mut rows: Vec<UniversalRow>) -> Result<Vec<UniversalRow>> {
        self.transform_rows_inplace(&mut rows)?;
        Ok(rows)
    }

    /// Consume an owned change batch, transform in place, and return it.
    pub fn apply_changes(&self, mut changes: Vec<UniversalChange>) -> Result<Vec<UniversalChange>> {
        self.transform_changes_inplace(&mut changes)?;
        Ok(changes)
    }

    /// Async stage walk used by [`crate::BatchTransformer`]: in-place mutates,
    /// External exchanges over child-stdio (may change batch length).
    pub(crate) async fn apply_changes_async(
        &self,
        batch_id: u64,
        mut changes: Vec<UniversalChange>,
    ) -> Result<Vec<UniversalChange>> {
        if self.is_identity() {
            return Ok(changes);
        }
        for stage in &self.stages {
            match stage {
                Stage::InPlace(t) => t.transform_changes_inplace(&mut changes)?,
                Stage::External(ext) => {
                    changes = ext.exchange_changes(batch_id, changes).await?;
                }
            }
        }
        Ok(changes)
    }

    /// Async stage walk for rows (see [`Self::apply_changes_async`]).
    pub(crate) async fn apply_rows_async(
        &self,
        batch_id: u64,
        mut rows: Vec<UniversalRow>,
    ) -> Result<Vec<UniversalRow>> {
        if self.is_identity() {
            return Ok(rows);
        }
        for stage in &self.stages {
            match stage {
                Stage::InPlace(t) => t.transform_rows_inplace(&mut rows)?,
                Stage::External(ext) => {
                    rows = ext.exchange_rows(batch_id, rows).await?;
                }
            }
        }
        Ok(rows)
    }
}
