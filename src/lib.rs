//! This crate backs the surreal-sync CLI and shared tests. It is not the
//! library you should depend on to embed sync in your app.
//!
//! The stock `surreal-sync` binary links this crate. Embedders must **not**
//! depend on `surreal-sync` as a library.
//!
//! # Embed here instead
//!
//! | Source | Crate |
//! |--------|-------|
//! | Snowflake | `surreal-sync-snowflake` (`from_snowflake`) |
//! | MySQL/MariaDB binlog | `surreal-sync-mysql` (`from_binlog`) |
//! | SurrealDB sink | `surreal-sync-surreal` with feature `v2` or `v3` |
//! | Checkpoint (CDC) | included in `surreal-sync-surreal`; or `surreal-sync-runtime::checkpoint_fs` |
//!
//! See `docs/sync-pipeline.md` (“Advanced: embedding”) and
//! `examples/from-mysql-binlog` / `examples/from-snowflake`.
//!
//! # What this crate exposes
//!
//! - [`testing`] — shared helpers for workspace integration / load tests
//! - Thin re-exports used by the stock binary’s `from *` handlers and tests
//!   ([`csv`], [`jsonl`], [`orchestrate_snapshot_then_incremental`])
//!
//! The CLI picks SurrealDB v2 vs v3 automatically; that logic lives in the
//! binary, not in this library.

/// Integration / load-test helpers (for workspace tests only — not for embedding).
pub mod testing;

/// Run a watermark snapshot+stream full sync and then continue with the
/// source's existing incremental runner from the handed-off stream position,
/// all in one process.
///
/// This is the shared orchestration used by every `from <source> sync`
/// subcommand. The snapshot phase returns its final stream position `P`;
/// `convert_handoff` turns `P` into the source's incremental checkpoint type
/// (an LSN checkpoint for wal2json, a `sequence_id` checkpoint for the trigger
/// sources); `run_incremental` then resumes live replication from exactly that
/// position. Because the snapshot is consistent at `P`, no replay window is
/// needed — incremental simply continues from `P`.
pub async fn orchestrate_snapshot_then_incremental<P, C, SnapFut, IncFut>(
    run_snapshot: SnapFut,
    convert_handoff: impl FnOnce(P) -> C,
    run_incremental: impl FnOnce(C) -> IncFut,
) -> anyhow::Result<()>
where
    SnapFut: std::future::Future<Output = anyhow::Result<P>>,
    IncFut: std::future::Future<Output = anyhow::Result<()>>,
{
    tracing::info!("Starting snapshot+stream full sync (snapshot phase)");
    let position = run_snapshot.await?;
    let checkpoint = convert_handoff(position);
    tracing::info!("Snapshot phase complete; continuing with incremental sync from handoff");
    run_incremental(checkpoint).await?;
    tracing::info!("snapshot+stream sync completed successfully");
    Ok(())
}

// Re-export CSV and JSONL crates for integration / load tests and the stock CLI
// binary handlers. Prefer depending on these crates directly in new code.
pub use surreal_sync_csv::from_csv as csv;
pub use surreal_sync_json::from_jsonl as jsonl;
