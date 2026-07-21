//! Finite row-chunk streams for full-sync / keyset table scans.
//!
//! Unlike oneshot [`crate::write_rows`] per chunk, a [`RowChunkDriver`] fed to
//! [`crate::run_source_runtime`] keeps one apply window alive across chunks so
//! the next keyset read can overlap prior-chunk transform/sink when
//! `max_in_flight > 1`.

use anyhow::Result;
use async_trait::async_trait;
use sync_core::{UniversalChange, UniversalRow};

use super::event::PositionedEvent;
use super::source_driver::{CheckpointPolicy, SourceDriver};

/// Produces successive row chunks until the table scan is exhausted.
///
/// Return `Ok(None)` (or an empty `Vec`) when there are no more rows.
#[async_trait]
pub trait RowChunkSource: Send {
    async fn next_chunk(&mut self) -> Result<Option<Vec<UniversalRow>>>;
}

/// Long-lived [`SourceDriver`] over a [`RowChunkSource`] (CSV-like full-sync pattern).
///
/// Each poll loads one chunk, converts rows to upsert changes, and returns them
/// as positioned events. [`run_source_runtime`](crate::run_source_runtime) may
/// poll the next chunk while earlier chunks are still transforming or sinking.
pub struct RowChunkDriver<C> {
    source: C,
    next_index: u64,
    sunk_count: u64,
    finished: bool,
}

impl<C> RowChunkDriver<C> {
    /// Wrap a chunk source. `next_index` seeds [`UniversalRow::index`] / positions.
    pub fn new(source: C) -> Self {
        Self {
            source,
            next_index: 0,
            sunk_count: 0,
            finished: false,
        }
    }

    /// Rows successfully sunk (pre-transform input count via [`SourceDriver::note_sunk_events`]).
    pub fn sunk_count(&self) -> u64 {
        self.sunk_count
    }
}

#[async_trait]
impl<C> SourceDriver for RowChunkDriver<C>
where
    C: RowChunkSource,
{
    type Position = u64;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.finished {
            return Ok(Vec::new());
        }

        match self.source.next_chunk().await? {
            None => {
                self.finished = true;
                Ok(Vec::new())
            }
            Some(rows) if rows.is_empty() => {
                self.finished = true;
                Ok(Vec::new())
            }
            Some(mut rows) => {
                let mut events = Vec::with_capacity(rows.len());
                for row in rows.drain(..) {
                    let mut row = row;
                    row.index = self.next_index;
                    let pos = self.next_index;
                    self.next_index = self.next_index.saturating_add(1);
                    let change = UniversalChange::update(row.table, row.id, row.fields);
                    events.push(PositionedEvent::change(change, pos));
                }
                Ok(events)
            }
        }
    }

    async fn commit(&mut self, _position: Self::Position) -> Result<()> {
        // File / keyset scans have no durable mid-run cursor.
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::CommitOnly
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.sunk_count = self.sunk_count.saturating_add(count);
    }
}
