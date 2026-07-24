//! Batch transform step (Pipeline + test doubles).

use crate::apply::ApplyEvent;
use crate::pipeline::Pipeline;
use anyhow::{bail, Result};
use async_trait::async_trait;
use sync_core::{Change, Relation, RelationChange, Row};

/// Executes a transform for one numbered batch.
///
/// The apply runtime correlates in-flight work by `batch_id`. Responses may
/// complete out of order; sink apply + watermark advance stay strictly ordered.
///
/// [`Pipeline`] implements this: empty pipelines take the same JoinSet path
/// as non-empty ones via [`Self::is_identity`] (async no-op); non-empty
/// pipelines walk in-place and External stages asynchronously. Test-support
/// provides scripted delays / failures so W≥2 reliability scenarios are real
/// without a child process.
///
/// # Relations
///
/// Batches may contain [`ApplyEvent::RelationChange`] interleaved with row
/// changes. **Fail closed:** the default
/// [`transform_relation_changes`](Self::transform_relation_changes) /
/// [`transform_relations`](Self::transform_relations) return `Err` so a custom
/// impl that only overrides [`transform_changes`](Self::transform_changes)
/// cannot silently pass relation edges through. Override those methods, or use
/// [`Pipeline`] (which implements them). The default
/// [`transform_events`](Self::transform_events) splits, transforms, and
/// recombines by index (length of each kind must be preserved).
#[async_trait]
pub trait BatchTransformer: Send + Sync {
    /// Whether transform dispatch can be skipped entirely (empty pipeline).
    ///
    /// For [`Pipeline`], this is [`Pipeline::is_identity`] — only an empty
    /// stage list, not a lone passthrough stage.
    fn is_identity(&self) -> bool;

    /// Transform an owned change batch. `batch_id` is monotonic per apply run.
    async fn transform_changes(&self, batch_id: u64, changes: Vec<Change>) -> Result<Vec<Change>>;

    /// Transform an owned row batch.
    async fn transform_rows(&self, batch_id: u64, rows: Vec<Row>) -> Result<Vec<Row>>;

    /// Transform an owned relation-change batch.
    ///
    /// **Default: fail closed** — returns an error so relation edges are never
    /// silently skipped when only [`transform_changes`](Self::transform_changes)
    /// is overridden. Implement this method (or use [`Pipeline`]) for
    /// relation-aware transformers. Explicit passthrough is `Ok(changes)`.
    async fn transform_relation_changes(
        &self,
        _batch_id: u64,
        _changes: Vec<RelationChange>,
    ) -> Result<Vec<RelationChange>> {
        bail!(
            "BatchTransformer::transform_relation_changes is not implemented; \
             override it (or return Ok(changes) for explicit passthrough), or use Pipeline"
        )
    }

    /// Transform an owned relation (full-sync) batch.
    ///
    /// **Default: fail closed** — see [`transform_relation_changes`](Self::transform_relation_changes).
    async fn transform_relations(
        &self,
        _batch_id: u64,
        _relations: Vec<Relation>,
    ) -> Result<Vec<Relation>> {
        bail!(
            "BatchTransformer::transform_relations is not implemented; \
             override it (or return Ok(relations) for explicit passthrough), or use Pipeline"
        )
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

    async fn transform_changes(&self, batch_id: u64, changes: Vec<Change>) -> Result<Vec<Change>> {
        self.apply_changes_async(batch_id, changes).await
    }

    async fn transform_rows(&self, batch_id: u64, rows: Vec<Row>) -> Result<Vec<Row>> {
        self.apply_rows_async(batch_id, rows).await
    }

    async fn transform_relation_changes(
        &self,
        batch_id: u64,
        changes: Vec<RelationChange>,
    ) -> Result<Vec<RelationChange>> {
        self.apply_relation_changes_async(batch_id, changes).await
    }

    async fn transform_relations(
        &self,
        batch_id: u64,
        relations: Vec<Relation>,
    ) -> Result<Vec<Relation>> {
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
