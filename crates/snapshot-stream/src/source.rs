//! The backend trait each source implements to drive a watermark snapshot.

use anyhow::Result;
use sync_core::UniversalRow;
use uuid::Uuid;

use crate::types::{PkTuple, SnapshotSignal, StreamEvent, StreamPosition, TableSpec, WatermarkKind};

/// A source capable of driving a DBLog-style watermark snapshot.
///
/// The generic loop ([`crate::run_snapshot_stream`]) owns the algorithm; a
/// backend supplies the source-specific pieces: how to enumerate tables, read
/// primary-key-ordered chunks, write watermark rows that surface in the change
/// stream, consume that stream, report the current position, and free already
/// consumed change-log data.
///
/// # Watermark contract
///
/// [`write_watermark`](WatermarkSource::write_watermark) inserts a row into a
/// signal table keyed by the given UUID. That insert must subsequently appear
/// in [`next_stream_events`](WatermarkSource::next_stream_events) as a
/// [`StreamEvent`] whose [`PkTuple`] is the single watermark UUID, so the loop
/// can detect when its low/high watermarks pass by in stream order.
#[async_trait::async_trait]
pub trait WatermarkSource: Send {
    /// The change-stream position type for this source.
    type Position: StreamPosition;

    /// Enumerate the tables to snapshot, each with its ordered primary key
    /// columns.
    async fn snapshot_tables(&self) -> Result<Vec<TableSpec>>;

    /// Read the next chunk of a table using keyset pagination.
    ///
    /// Returns up to `limit` rows where `(pk) > after` (or from the start when
    /// `after` is `None`), ordered ascending by primary key. An empty result
    /// means the table is exhausted.
    async fn read_chunk(
        &self,
        table: &TableSpec,
        after: Option<&PkTuple>,
        limit: usize,
    ) -> Result<Vec<UniversalRow>>;

    /// Write a watermark row (keyed by `id`) into the source's signal table.
    async fn write_watermark(&self, kind: WatermarkKind, id: Uuid) -> Result<()>;

    /// Return the next batch of change-stream events (including watermark
    /// rows). May return an empty batch when no events are currently
    /// available.
    async fn next_stream_events(&mut self) -> Result<Vec<StreamEvent<Self::Position>>>;

    /// Report the current change-stream position.
    async fn current_position(&self) -> Result<Self::Position>;

    /// Mark all change-stream data up to and including `position` as durably
    /// applied, allowing the backend to free it (advance a replication slot,
    /// prune consumed audit rows, etc.). Backends must never free past the
    /// resumable checkpoint position.
    async fn commit_consumed(&mut self, position: Self::Position) -> Result<()>;

    /// Return any pending ad-hoc snapshot signals. Backends without signalling
    /// support may return an empty vector.
    async fn read_signals(&mut self) -> Result<Vec<SnapshotSignal>>;
}
