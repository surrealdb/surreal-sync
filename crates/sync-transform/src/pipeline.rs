//! Ordered transform pipeline: in-place stages and external boundary stub.

use crate::inplace::InPlaceTransform;
use anyhow::{bail, Result};
use std::sync::Arc;
use sync_core::{UniversalChange, UniversalRow};

/// Stub for Phase 3 external (child-stdio) transforms.
///
/// Not constructible from config yet; present so [`Stage`] and [`Pipeline`] can
/// grow an `External` variant without reshaping the Phase 1 API.
#[derive(Debug)]
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
#[derive(Debug, Default)]
pub struct Pipeline {
    stages: Vec<Stage>,
}

impl Pipeline {
    /// Create an empty (identity) pipeline.
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    /// Whether this pipeline has no stages (identity / zero dispatch overhead).
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

    /// Transform owned rows in place (true zero-copy when pipeline is non-empty).
    ///
    /// Empty pipeline: no-op, no stage dispatch.
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

    /// Transform owned changes in place (true zero-copy when pipeline is non-empty).
    ///
    /// Empty pipeline: no-op, no stage dispatch.
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
