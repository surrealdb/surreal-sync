//! Ordered transform pipeline: in-place stages and external boundary stub.

use crate::inplace::InPlaceTransform;
use anyhow::{bail, Result};
use std::sync::Arc;
use sync_core::{UniversalChange, UniversalRow};

/// Stub for Phase 3 external (child-stdio) transforms.
///
/// Not constructible from config yet; present so [`Stage`] and [`Pipeline`] can
/// grow an `External` variant without reshaping the Phase 1 API.
#[derive(Debug, Clone)]
pub struct ExternalTransform {
    _private: (),
}

impl ExternalTransform {
    /// Placeholder constructor for future wiring / tests that need the variant.
    pub fn stub() -> Self {
        Self { _private: () }
    }
}

/// A single pipeline stage.
#[derive(Clone)]
pub enum Stage {
    /// In-process mutate-only transform.
    InPlace(Arc<dyn InPlaceTransform>),
    /// External worker boundary (Phase 3; applying today returns an error).
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
/// # Phase 2 apply framework
///
/// The ChangeFeed / `write_rows` apply path must gate on
/// [`is_identity`](Self::is_identity) (not merely “stages happen to be
/// no-ops”) for the true zero-dispatch hot path.
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

    /// Append an external stage stub (Phase 3 will flesh this out).
    pub fn push_external(&mut self, external: ExternalTransform) {
        self.stages.push(Stage::External(external));
    }

    /// Transform owned rows in place.
    ///
    /// Empty pipeline: no-op with no stage dispatch. Non-empty: each in-place
    /// stage mutates the slice without reallocating the `Vec` buffer (true
    /// zero-copy relative to building a new batch).
    pub fn transform_rows_inplace(&self, rows: &mut [UniversalRow]) -> Result<()> {
        if self.is_identity() {
            return Ok(());
        }
        for stage in &self.stages {
            match stage {
                Stage::InPlace(t) => t.transform_rows_inplace(rows)?,
                Stage::External(_) => {
                    bail!("External transforms are not implemented yet (Phase 3)")
                }
            }
        }
        Ok(())
    }

    /// Transform owned changes in place.
    ///
    /// Empty pipeline: no-op with no stage dispatch. Non-empty: each in-place
    /// stage mutates the slice without reallocating the `Vec` buffer (true
    /// zero-copy relative to building a new batch).
    pub fn transform_changes_inplace(&self, changes: &mut [UniversalChange]) -> Result<()> {
        if self.is_identity() {
            return Ok(());
        }
        for stage in &self.stages {
            match stage {
                Stage::InPlace(t) => t.transform_changes_inplace(changes)?,
                Stage::External(_) => {
                    bail!("External transforms are not implemented yet (Phase 3)")
                }
            }
        }
        Ok(())
    }

    /// Consume an owned row batch, transform in place, and return it.
    ///
    /// Preferred sync-framework path: empty pipeline is a pure move with no
    /// transform dispatch.
    pub fn apply_rows(&self, mut rows: Vec<UniversalRow>) -> Result<Vec<UniversalRow>> {
        self.transform_rows_inplace(&mut rows)?;
        Ok(rows)
    }

    /// Consume an owned change batch, transform in place, and return it.
    pub fn apply_changes(&self, mut changes: Vec<UniversalChange>) -> Result<Vec<UniversalChange>> {
        self.transform_changes_inplace(&mut changes)?;
        Ok(changes)
    }
}
