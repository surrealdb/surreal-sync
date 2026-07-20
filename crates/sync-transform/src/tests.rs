//! Unit tests for Phase 1: InPlaceTransform, CowBatch, Pipeline.

use crate::{CowBatch, ExternalTransform, InPlaceTransform, Passthrough, Pipeline};
use anyhow::Result;
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

#[test]
fn empty_pipeline_skips_transform_dispatch() {
    // Identity path must not require any stage object and must short-circuit
    // before looping stages (there are none to call).
    let pipeline = Pipeline::new();
    let mut rows = vec![sample_row("x")];
    let ptr_before = rows.as_ptr();
    pipeline.transform_rows_inplace(&mut rows).unwrap();
    // Vec buffer address unchanged (in-place no-op, no realloc from transforms).
    assert_eq!(rows.as_ptr(), ptr_before);
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
fn pipeline_external_stub_errors_on_apply() {
    let mut pipeline = Pipeline::new();
    pipeline.push_external(ExternalTransform::stub());
    let err = pipeline
        .apply_rows(vec![sample_row("alice")])
        .unwrap_err();
    assert!(
        err.to_string().contains("not implemented"),
        "unexpected error: {err}"
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
