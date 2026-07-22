//! Unit tests for Phase 1: InPlaceTransform, CowBatch, Pipeline.

use crate::{CowBatch, ExternalTransform, InPlaceTransform, Passthrough, Pipeline};
use anyhow::{bail, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use sync_core::{UniversalChange, UniversalChangeOp, UniversalRow, UniversalValue};

fn sample_row(name: &str) -> UniversalRow {
    UniversalRow::builder("users", 0, UniversalValue::Int64(1))
        .field(
            "name",
            UniversalValue::VarChar {
                value: name.to_string(),
                length: 64,
            },
        )
        .build()
}

fn sample_change(name: &str) -> UniversalChange {
    let mut data = HashMap::new();
    data.insert(
        "name".to_string(),
        UniversalValue::VarChar {
            value: name.to_string(),
            length: 64,
        },
    );
    UniversalChange::create("users", UniversalValue::Int64(1), data)
}

/// Mutating transform used to prove make_mut / stage dispatch.
struct Rename {
    to: String,
}

impl InPlaceTransform for Rename {
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        row.fields.insert(
            "name".to_string(),
            UniversalValue::VarChar {
                value: self.to.clone(),
                length: 64,
            },
        );
        Ok(())
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        if let Some(data) = change.data.as_mut() {
            data.insert(
                "name".to_string(),
                UniversalValue::VarChar {
                    value: self.to.clone(),
                    length: 64,
                },
            );
        }
        Ok(())
    }
}

/// Counts how many times per-item transforms are invoked.
struct Counting<T> {
    inner: T,
    rows: AtomicUsize,
    changes: AtomicUsize,
}

impl<T: InPlaceTransform> InPlaceTransform for Counting<T> {
    fn transform_row(&self, row: &mut UniversalRow) -> Result<()> {
        self.rows.fetch_add(1, Ordering::SeqCst);
        self.inner.transform_row(row)
    }

    fn transform_change(&self, change: &mut UniversalChange) -> Result<()> {
        self.changes.fetch_add(1, Ordering::SeqCst);
        self.inner.transform_change(change)
    }
}

/// Always fails; used to assert later stages are not reached.
struct AlwaysFail;

impl InPlaceTransform for AlwaysFail {
    fn transform_row(&self, _row: &mut UniversalRow) -> Result<()> {
        bail!("stage failed (rows)")
    }

    fn transform_change(&self, _change: &mut UniversalChange) -> Result<()> {
        bail!("stage failed (changes)")
    }
}

#[test]
fn empty_pipeline_is_identity() {
    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());
    assert!(pipeline.is_empty());
    assert_eq!(pipeline.len(), 0);

    let rows = vec![sample_row("alice")];
    let out = pipeline.apply_rows(rows).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "alice".to_string(),
            length: 64,
        })
    );

    let changes = vec![sample_change("bob")];
    let out = pipeline.apply_changes(changes).unwrap();
    assert_eq!(out[0].operation, UniversalChangeOp::Create);
}

/// Empty pipeline short-circuits before any stage dispatch.
///
/// Phase 1 can only assert emptiness + apply no-op here. Phase 2's apply
/// loop must gate on [`Pipeline::is_identity`] so the sync identity path never
/// enters transform dispatch when there are no stages.
#[test]
fn empty_pipeline_is_identity_short_circuit() {
    let pipeline = Pipeline::new();
    assert!(pipeline.is_identity());
    assert!(pipeline.stages().is_empty());

    let rows = vec![sample_row("x")];
    let out = pipeline.apply_rows(rows).unwrap();
    assert_eq!(
        out[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "x".to_string(),
            length: 64,
        })
    );

    let changes = vec![sample_change("y")];
    let out = pipeline.apply_changes(changes).unwrap();
    assert_eq!(
        out[0].data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "y".to_string(),
            length: 64,
        })
    );

    // Contrast: a non-empty pipeline with a counting stage *does* dispatch.
    let counter = Arc::new(Counting {
        inner: Passthrough,
        rows: AtomicUsize::new(0),
        changes: AtomicUsize::new(0),
    });
    let mut with_stage = Pipeline::new();
    with_stage.push_inplace_arc(counter.clone());
    assert!(!with_stage.is_identity());
    with_stage.apply_rows(vec![sample_row("z")]).unwrap();
    assert_eq!(counter.rows.load(Ordering::SeqCst), 1);
}

#[test]
fn lone_passthrough_is_not_identity() {
    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(Passthrough);
    assert!(!pipeline.is_identity());
    assert_eq!(pipeline.len(), 1);
    // Still a no-op on data, but stages are dispatched (not the zero-dispatch path).
    let out = pipeline.apply_rows(vec![sample_row("alice")]).unwrap();
    assert_eq!(
        out[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "alice".to_string(),
            length: 64,
        })
    );
}

#[test]
fn passthrough_on_owned_vec_mutates_in_place_without_realloc() {
    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(Passthrough);

    let mut rows = vec![sample_row("alice"), sample_row("bob")];
    let ptr_before = rows.as_ptr();
    pipeline.transform_rows_inplace(&mut rows).unwrap();
    assert_eq!(rows.as_ptr(), ptr_before);
    assert_eq!(
        rows[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "alice".to_string(),
            length: 64,
        })
    );
}

#[test]
fn cowbatch_passthrough_unique_arc_no_clone() {
    let row = sample_row("alice");
    let arc = Arc::new(row);
    let ptr_before = Arc::as_ptr(&arc);

    let mut batch = CowBatch::new(vec![arc]);
    batch.apply_inplace(&Passthrough).unwrap();

    let ptr_after = Arc::as_ptr(&batch.items[0]);
    assert_eq!(
        ptr_before, ptr_after,
        "unique Arc must not be cloned by make_mut on passthrough"
    );
    assert_eq!(Arc::strong_count(&batch.items[0]), 1);
}

/// Regression: shared Arc + Passthrough still clones — `make_mut` runs before
/// the no-op transform body.
#[test]
fn cowbatch_shared_passthrough_still_clones() {
    let row = sample_row("alice");
    let shared = Arc::new(row);
    let held = Arc::clone(&shared);
    assert_eq!(Arc::strong_count(&shared), 2);

    let ptr_before = Arc::as_ptr(&shared);
    let mut batch = CowBatch::new(vec![shared]);
    batch.apply_inplace(&Passthrough).unwrap();

    let ptr_after = Arc::as_ptr(&batch.items[0]);
    assert_ne!(
        ptr_before, ptr_after,
        "shared Arc must be cloned by make_mut even for Passthrough"
    );
    assert_eq!(Arc::strong_count(&batch.items[0]), 1);
    assert_eq!(Arc::strong_count(&held), 1);
    // Original holder unchanged; batch item is a distinct clone of the same data.
    assert_eq!(
        held.get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "alice".to_string(),
            length: 64,
        })
    );
    assert_eq!(
        batch.items[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "alice".to_string(),
            length: 64,
        })
    );
}

#[test]
fn cowbatch_make_mut_clones_only_when_shared() {
    let change = sample_change("alice");
    let shared = Arc::new(change);
    let held = Arc::clone(&shared);
    assert_eq!(Arc::strong_count(&shared), 2);

    let ptr_before = Arc::as_ptr(&shared);
    let mut batch = CowBatch::new(vec![shared]);

    // Mutating transform: make_mut must clone because Arc is shared.
    batch
        .apply_inplace(&Rename {
            to: "carol".to_string(),
        })
        .unwrap();

    let ptr_after = Arc::as_ptr(&batch.items[0]);
    assert_ne!(
        ptr_before, ptr_after,
        "shared Arc must be cloned by make_mut before mutation"
    );
    // Original holder still sees the old value (COW).
    assert_eq!(
        held.data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "alice".to_string(),
            length: 64,
        })
    );
    assert_eq!(
        batch.items[0].data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "carol".to_string(),
            length: 64,
        })
    );
    assert_eq!(Arc::strong_count(&batch.items[0]), 1);
    assert_eq!(Arc::strong_count(&held), 1);
}

#[test]
fn cowbatch_unique_arc_mutating_transform_no_clone() {
    let change = sample_change("alice");
    let arc = Arc::new(change);
    let ptr_before = Arc::as_ptr(&arc);

    let mut batch = CowBatch::new(vec![arc]);
    batch
        .apply_inplace(&Rename {
            to: "dave".to_string(),
        })
        .unwrap();

    assert_eq!(
        Arc::as_ptr(&batch.items[0]),
        ptr_before,
        "unique Arc should mutate in place without clone"
    );
    assert_eq!(
        batch.items[0].data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "dave".to_string(),
            length: 64,
        })
    );
}

#[test]
fn pipeline_applies_inplace_stages_in_order() {
    let counter = Arc::new(Counting {
        inner: Rename {
            to: "step1".to_string(),
        },
        rows: AtomicUsize::new(0),
        changes: AtomicUsize::new(0),
    });

    let mut pipeline = Pipeline::new();
    pipeline.push_inplace_arc(counter.clone());
    pipeline.push_inplace(Rename {
        to: "step2".to_string(),
    });

    let out = pipeline.apply_rows(vec![sample_row("alice")]).unwrap();
    assert_eq!(counter.rows.load(Ordering::SeqCst), 1);
    assert_eq!(
        out[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "step2".to_string(),
            length: 64,
        })
    );
}

#[test]
fn pipeline_applies_changes_stages_in_order() {
    let step1 = Arc::new(Counting {
        inner: Rename {
            to: "step1".to_string(),
        },
        rows: AtomicUsize::new(0),
        changes: AtomicUsize::new(0),
    });
    let step2 = Arc::new(Counting {
        inner: Rename {
            to: "step2".to_string(),
        },
        rows: AtomicUsize::new(0),
        changes: AtomicUsize::new(0),
    });

    let mut pipeline = Pipeline::new();
    pipeline.push_inplace_arc(step1.clone());
    pipeline.push_inplace_arc(step2.clone());

    let out = pipeline
        .apply_changes(vec![sample_change("alice")])
        .unwrap();
    assert_eq!(step1.changes.load(Ordering::SeqCst), 1);
    assert_eq!(step2.changes.load(Ordering::SeqCst), 1);
    assert_eq!(
        out[0].data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "step2".to_string(),
            length: 64,
        })
    );

    // Same ordering via transform_changes_inplace.
    let mut changes = vec![sample_change("bob")];
    pipeline.transform_changes_inplace(&mut changes).unwrap();
    assert_eq!(step1.changes.load(Ordering::SeqCst), 2);
    assert_eq!(step2.changes.load(Ordering::SeqCst), 2);
    assert_eq!(
        changes[0].data.as_ref().unwrap().get("name"),
        Some(&UniversalValue::VarChar {
            value: "step2".to_string(),
            length: 64,
        })
    );
}

#[test]
fn failing_stage_stops_later_stages_rows() {
    let later = Arc::new(Counting {
        inner: Rename {
            to: "should-not-run".to_string(),
        },
        rows: AtomicUsize::new(0),
        changes: AtomicUsize::new(0),
    });

    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(AlwaysFail);
    pipeline.push_inplace_arc(later.clone());

    let err = pipeline.apply_rows(vec![sample_row("alice")]).unwrap_err();
    assert!(
        err.to_string().contains("stage failed"),
        "unexpected error: {err}"
    );
    assert_eq!(
        later.rows.load(Ordering::SeqCst),
        0,
        "later stage must not run after earlier failure"
    );
}

#[test]
fn failing_stage_stops_later_stages_changes() {
    let later = Arc::new(Counting {
        inner: Rename {
            to: "should-not-run".to_string(),
        },
        rows: AtomicUsize::new(0),
        changes: AtomicUsize::new(0),
    });

    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(AlwaysFail);
    pipeline.push_inplace_arc(later.clone());

    let err = pipeline
        .apply_changes(vec![sample_change("alice")])
        .unwrap_err();
    assert!(
        err.to_string().contains("stage failed"),
        "unexpected error: {err}"
    );
    assert_eq!(
        later.changes.load(Ordering::SeqCst),
        0,
        "later stage must not run after earlier failure"
    );
}

#[test]
fn pipeline_external_sync_inplace_errors() {
    let transport = crate::test_support::ScriptedExternalTransport::new();
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(std::sync::Arc::new(
        transport,
    )));
    let err = pipeline.apply_rows(vec![sample_row("alice")]).unwrap_err();
    assert!(
        err.to_string().contains("BatchTransformer") || err.to_string().contains("async"),
        "unexpected error: {err}"
    );
}

#[test]
fn pipeline_external_sync_relation_inplace_errors() {
    use sync_core::{UniversalRelation, UniversalRelationChange, UniversalThingRef};

    let transport = crate::test_support::ScriptedExternalTransport::new();
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::with_transport(std::sync::Arc::new(
        transport,
    )));

    let rel = UniversalRelation::new(
        "follows",
        UniversalValue::Int64(1),
        UniversalThingRef::new("users", UniversalValue::Int64(1)),
        UniversalThingRef::new("users", UniversalValue::Int64(2)),
        HashMap::new(),
    );

    let err_changes = pipeline
        .apply_relation_changes(vec![UniversalRelationChange::create(rel.clone())])
        .unwrap_err();
    assert!(
        err_changes.to_string().contains("BatchTransformer")
            || err_changes.to_string().contains("async")
            || err_changes.to_string().contains("transform_relation"),
        "sync apply_relation_changes with External must bail: {err_changes}"
    );

    let err_rels = pipeline.apply_relations(vec![rel]).unwrap_err();
    assert!(
        err_rels.to_string().contains("BatchTransformer")
            || err_rels.to_string().contains("async")
            || err_rels.to_string().contains("transform_relation"),
        "sync apply_relations with External must bail: {err_rels}"
    );
}

/// Custom BatchTransformer that only overrides row/change paths must not
/// silently no-op relation batches (fail closed by default).
#[tokio::test]
async fn batch_transformer_default_relation_methods_fail_closed() {
    use crate::BatchTransformer;
    use async_trait::async_trait;
    use sync_core::{UniversalRelation, UniversalRelationChange, UniversalThingRef};

    struct ChangesOnly;

    #[async_trait]
    impl BatchTransformer for ChangesOnly {
        fn is_identity(&self) -> bool {
            false
        }

        async fn transform_changes(
            &self,
            _batch_id: u64,
            changes: Vec<UniversalChange>,
        ) -> Result<Vec<UniversalChange>> {
            Ok(changes)
        }

        async fn transform_rows(
            &self,
            _batch_id: u64,
            rows: Vec<UniversalRow>,
        ) -> Result<Vec<UniversalRow>> {
            Ok(rows)
        }
    }

    let t = ChangesOnly;
    let rel = UniversalRelation::new(
        "follows",
        UniversalValue::Int64(1),
        UniversalThingRef::new("users", UniversalValue::Int64(1)),
        UniversalThingRef::new("users", UniversalValue::Int64(2)),
        HashMap::new(),
    );

    let err = t
        .transform_relation_changes(1, vec![UniversalRelationChange::create(rel.clone())])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("transform_relation_changes")
            && err.to_string().contains("not implemented"),
        "unexpected: {err}"
    );

    let err = t.transform_relations(1, vec![rel]).await.unwrap_err();
    assert!(
        err.to_string().contains("transform_relations")
            && err.to_string().contains("not implemented"),
        "unexpected: {err}"
    );

    // Mixed events also fail closed via the default transform_events path.
    let err = t
        .transform_events(
            1,
            vec![
                crate::ApplyEvent::Change(sample_change("a")),
                crate::ApplyEvent::relation_change(UniversalRelationChange::create(
                    UniversalRelation::new(
                        "follows",
                        UniversalValue::Int64(2),
                        UniversalThingRef::new("users", UniversalValue::Int64(3)),
                        UniversalThingRef::new("users", UniversalValue::Int64(4)),
                        HashMap::new(),
                    ),
                )),
            ],
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("transform_relation_changes"),
        "mixed events must not silently drop relations: {err}"
    );
}

#[test]
fn cowbatch_from_owned_and_row_apply() {
    let mut batch = CowBatch::from_owned(vec![sample_row("alice")]);
    assert_eq!(batch.len(), 1);
    assert!(!batch.is_empty());
    batch
        .apply_inplace(&Rename {
            to: "zoe".to_string(),
        })
        .unwrap();
    let items = batch.into_items();
    assert_eq!(
        items[0].get_field("name"),
        Some(&UniversalValue::VarChar {
            value: "zoe".to_string(),
            length: 64,
        })
    );
}
