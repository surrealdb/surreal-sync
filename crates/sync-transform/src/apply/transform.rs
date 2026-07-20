//! Batch transform execution seam (Pipeline + test doubles).

use crate::apply::ApplyEvent;
use crate::pipeline::Pipeline;
use anyhow::{bail, Result};
use async_trait::async_trait;
use sync_core::{UniversalChange, UniversalRelation, UniversalRelationChange, UniversalRow};

/// Executes a transform for one numbered batch.
///
/// The apply runtime correlates in-flight work by `batch_id`. Responses may
/// complete out of order; sink apply + commit stay strictly ordered.
///
/// [`Pipeline`] implements this: empty pipelines short-circuit via
/// [`Self::is_identity`]; non-empty pipelines walk in-place and External stages
/// asynchronously. Test-support provides scripted delays / failures so W≥2
/// reliability scenarios are real without a child process.
///
/// # Relations
///
/// Batches may contain [`ApplyEvent::RelationChange`] interleaved with row
/// changes. Implement [`transform_relation_changes`](Self::transform_relation_changes)
/// / [`transform_relations`](Self::transform_relations) for relation-aware
/// stages; the default [`transform_events`](Self::transform_events) splits,
/// transforms, and recombines by index (length of each kind must be preserved).
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

    /// Transform an owned relation-change batch.
    ///
    /// Default: return unchanged (no stage dispatch). Override for relation-aware
    /// pipelines / test doubles.
    async fn transform_relation_changes(
        &self,
        _batch_id: u64,
        changes: Vec<UniversalRelationChange>,
    ) -> Result<Vec<UniversalRelationChange>> {
        Ok(changes)
    }

    /// Transform an owned relation (full-sync) batch.
    ///
    /// Default: return unchanged.
    async fn transform_relations(
        &self,
        _batch_id: u64,
        relations: Vec<UniversalRelation>,
    ) -> Result<Vec<UniversalRelation>> {
        Ok(relations)
    }

    /// Transform a mixed apply-event batch, preserving order.
    ///
    /// Default implementation splits into changes / relation changes, calls
    /// [`transform_changes`](Self::transform_changes) /
    /// [`transform_relation_changes`](Self::transform_relation_changes), and
    /// recombines. Length of each kind must be preserved (filter/fan-out of a
    /// single kind in a mixed batch is not supported here — use homogeneous
    /// batches or External on row-only feeds).
    async fn transform_events(
        &self,
        batch_id: u64,
        events: Vec<ApplyEvent>,
    ) -> Result<Vec<ApplyEvent>> {
        let mut change_idxs = Vec::new();
        let mut changes = Vec::new();
        let mut rel_idxs = Vec::new();
        let mut rels = Vec::new();
        let mut out: Vec<Option<ApplyEvent>> = Vec::with_capacity(events.len());
        for (i, event) in events.into_iter().enumerate() {
            out.push(None);
            match event {
                ApplyEvent::Change(c) => {
                    change_idxs.push(i);
                    changes.push(c);
                }
                ApplyEvent::RelationChange(r) => {
                    rel_idxs.push(i);
                    rels.push(*r);
                }
            }
        }

        if !changes.is_empty() {
            let n = changes.len();
            let transformed = self.transform_changes(batch_id, changes).await?;
            if transformed.len() != n {
                bail!(
                    "transform_changes changed length ({n} → {}) in a mixed ApplyEvent batch; \
                     use homogeneous batches for filter/fan-out",
                    transformed.len()
                );
            }
            for (idx, c) in change_idxs.into_iter().zip(transformed) {
                out[idx] = Some(ApplyEvent::Change(c));
            }
        }

        if !rels.is_empty() {
            let n = rels.len();
            let transformed = self.transform_relation_changes(batch_id, rels).await?;
            if transformed.len() != n {
                bail!(
                    "transform_relation_changes changed length ({n} → {}) in a mixed \
                     ApplyEvent batch; use homogeneous batches for filter/fan-out",
                    transformed.len()
                );
            }
            for (idx, r) in rel_idxs.into_iter().zip(transformed) {
                out[idx] = Some(ApplyEvent::relation_change(r));
            }
        }

        Ok(out
            .into_iter()
            .map(|e| e.expect("every slot filled"))
            .collect())
    }
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

    async fn transform_relation_changes(
        &self,
        batch_id: u64,
        changes: Vec<UniversalRelationChange>,
    ) -> Result<Vec<UniversalRelationChange>> {
        self.apply_relation_changes_async(batch_id, changes).await
    }

    async fn transform_relations(
        &self,
        batch_id: u64,
        relations: Vec<UniversalRelation>,
    ) -> Result<Vec<UniversalRelation>> {
        self.apply_relations_async(batch_id, relations).await
    }

    async fn transform_events(
        &self,
        batch_id: u64,
        events: Vec<ApplyEvent>,
    ) -> Result<Vec<ApplyEvent>> {
        self.apply_events_async(batch_id, events).await
    }
}
