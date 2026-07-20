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
/// [`Pipeline`] implements this: empty pipelines short-circuit via
/// [`Self::is_identity`]; non-empty pipelines walk in-place and External stages
/// asynchronously. Test-support provides scripted delays / failures so W≥2
/// reliability scenarios are real without a child process.
#[async_trait]
pub trait BatchTransformer: Send + Sync {
    /// Whether transform dispatch can be skipped entirely (empty pipeline).
    ///
    /// For [`Pipeline`], this is [`Pipeline::is_identity`] — only an empty
    /// stage list, not a lone passthrough stage.
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
        batch_id: u64,
        changes: Vec<UniversalChange>,
    ) -> Result<Vec<UniversalChange>> {
        self.apply_changes_async(batch_id, changes).await
    }

    async fn transform_rows(
        &self,
        batch_id: u64,
        rows: Vec<UniversalRow>,
    ) -> Result<Vec<UniversalRow>> {
        self.apply_rows_async(batch_id, rows).await
    }
}
