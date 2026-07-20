//! Arc copy-on-write batch adapter over [`InPlaceTransform`].

use crate::inplace::InPlaceTransform;
use anyhow::Result;
use std::sync::Arc;
use sync_core::{UniversalChange, UniversalRow};

/// Batch of `Arc`-shared items with copy-on-write mutation via [`Arc::make_mut`].
///
/// Prefer owned `Vec` + [`InPlaceTransform::transform_*_inplace`] when sharing is
/// not required. Use [`CowBatch`] only when the same items may be shared across
/// holders and mutation must not affect other owners.
#[derive(Debug, Clone)]
pub struct CowBatch<T> {
    /// Shared items; mutated through [`Arc::make_mut`] in [`Self::apply_inplace`].
    pub items: Vec<Arc<T>>,
}

impl<T> CowBatch<T> {
    /// Create a batch from already-shared items.
    pub fn new(items: Vec<Arc<T>>) -> Self {
        Self { items }
    }

    /// Create a batch by wrapping each owned item in a fresh `Arc` (refcount 1).
    pub fn from_owned(items: Vec<T>) -> Self {
        Self {
            items: items.into_iter().map(Arc::new).collect(),
        }
    }

    /// Number of items in the batch.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Consume the batch, returning the underlying `Arc`s.
    pub fn into_items(self) -> Vec<Arc<T>> {
        self.items
    }
}

impl CowBatch<UniversalRow> {
    /// Apply an in-place transform with Arc COW semantics.
    ///
    /// [`Arc::make_mut`] clones an item only when its strong count is greater
    /// than one (shared). Unique arcs are mutated without allocation.
    pub fn apply_inplace(&mut self, t: &impl InPlaceTransform) -> Result<()> {
        for item in &mut self.items {
            t.transform_row(Arc::make_mut(item))?;
        }
        Ok(())
    }
}

impl CowBatch<UniversalChange> {
    /// Apply an in-place transform with Arc COW semantics.
    ///
    /// [`Arc::make_mut`] clones an item only when its strong count is greater
    /// than one (shared). Unique arcs are mutated without allocation.
    pub fn apply_inplace(&mut self, t: &impl InPlaceTransform) -> Result<()> {
        for item in &mut self.items {
            t.transform_change(Arc::make_mut(item))?;
        }
        Ok(())
    }
}
