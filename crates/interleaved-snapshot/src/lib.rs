//! Watermark-based interleaved snapshot full-sync framework.
//!
//! This crate implements a DBLog-style interleaved snapshot: tables are copied
//! in resumable, primary-key-ordered chunks *concurrently with* consuming the
//! source change stream, using low/high watermarks to deduplicate snapshot
//! reads against live changes (the log event always wins).
//!
//! The generic algorithm lives in [`run_interleaved_snapshot`]; each source
//! backend implements [`WatermarkSource`] to supply chunk reads, watermark
//! writes, stream consumption, position reporting, and consumed-log freeing.
//! Results are written through the version-independent
//! [`surreal_sink::SurrealSink`].
//!
//! # Guarantees
//!
//! - **Bounded memory**: the only buffered state is a single chunk (at most
//!   `chunk_size` rows), so memory is `O(chunk_size)` regardless of table size.
//!   [`InterleavedSnapshotResult::peak_buffered_rows`] exposes the exact peak.
//! - **Resumable**: progress is checkpointed per chunk via
//!   [`SnapshotCheckpointer`], so a crash resumes at the last copied primary
//!   key rather than restarting the table.
//! - **Bounded retention**: [`WatermarkSource::commit_reconciled`] is called as
//!   the stream is applied, so the source can free change-log data continuously
//!   instead of pinning it for the whole snapshot.

mod checkpointer;
mod runner;
mod source;
mod types;

#[cfg(test)]
mod tests;

pub use checkpointer::{ManagerCheckpointer, NoopCheckpointer, SnapshotCheckpointer};
pub use runner::{
    run_adhoc_snapshot_tables, run_interleaved_snapshot, run_interleaved_snapshot_with_resume,
    InterleavedSnapshotConfig, InterleavedSnapshotResult, DEFAULT_CHUNK_SIZE,
};
pub use source::WatermarkSource;
pub use types::{
    PkTuple, ReconciliationEvent, ReconciliationPos, SnapshotSignal, TableSpec, WatermarkKind,
};

// Re-export the resumable checkpoint types the framework produces.
pub use checkpoint::{InterleavedSnapshotCheckpoint, SnapshotTableProgress};
