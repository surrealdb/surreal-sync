//! Batch transform execution seam (Pipeline + test doubles).

use crate::pipeline::Pipeline;
use anyhow::Result;
use async_trait::async_trait;
use sync_core::{UniversalChange, UniversalRow};

/// Executes a transform for one numbered batch.
///
/// The apply runtime correlates in-flight work by `batch_id`. Responses may
/// complete out of order; sink apply + commit stay strictly ordered.
///
/// [`Pipeline`] implements this with the owned in-place path. Test-support
/// provides scripted delays / failures so W≥2 reliability scenarios are real
/// without External child-stdio (Phase 3).
#[async_trait]
pub trait BatchTransformer: Send + Sync {
    /// Whether transform dispatch can be skipped entirely (empty pipeline).
    fn is_identity(&self) -> bool;

    /// Transform an owned change batch. `batch_id` is monotonic per apply run.
    async fn transform_changes(
        &self,
        batch_id: u64,
        changes: Vec<UniversalChange>,
    ) -> Result<Vec<UniversalChange>>;

    /// Transform an owned row batch.
    async fn transform_rows(
        &self,
        batch_id: u64,
        rows: Vec<UniversalRow>,
    ) -> Result<Vec<UniversalRow>>;
}

#[async_trait]
impl BatchTransformer for Pipeline {
    fn is_identity(&self) -> bool {
        Pipeline::is_identity(self)
    }

    async fn transform_changes(
        &self,
        _batch_id: u64,
        changes: Vec<UniversalChange>,
    ) -> Result<Vec<UniversalChange>> {
        // Identity: pure move, no stage dispatch.
        if self.is_identity() {
            return Ok(changes);
        }
        self.apply_changes(changes)
    }

    async fn transform_rows(
        &self,
        _batch_id: u64,
        rows: Vec<UniversalRow>,
    ) -> Result<Vec<UniversalRow>> {
        if self.is_identity() {
            return Ok(rows);
        }
        self.apply_rows(rows)
    }
}
