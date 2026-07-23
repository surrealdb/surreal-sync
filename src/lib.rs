//! SurrealSync Library
//!
//! A library for migrating data from Neo4j, MongoDB, PostgreSQL, and MySQL databases to SurrealDB.
//!
//! # Features
//!
//! - Full synchronization: Complete data migration from source to target
//! - Incremental synchronization: Real-time change capture and replication
//! - Multiple databases: Neo4j, MongoDB, PostgreSQL, MySQL support
//! - Reliable checkpointing: Resume sync from any point after failures
//! - Portability: Trigger-based approaches work in any environment
//!
//! # Embedding
//!
//! Depend on this crate and call [`mysql_binlog::run`] with in-process
//! [`InPlaceTransform`] stages (same CLI flags as `surreal-sync from mysql-binlog`,
//! source-shaped argv: `sync|snapshot …`). See
//! `examples/mysql_binlog_custom_transform.rs` and `docs/sync-pipeline.md`.
//!
//! # Database-Specific Sync Crates
//!
//! Each database has its own dedicated sync crate:
//!
//! - `surreal_sync_neo4j_source` - Neo4j timestamp-based tracking
//! - `surreal_sync_mongodb_changestream_source` - MongoDB change streams
//! - `surreal_sync_postgresql_trigger_source` - PostgreSQL trigger-based tracking
//! - `surreal_sync_mysql_trigger_source` - MySQL audit table tracking
//! - `surreal_sync_kafka_source` - Kafka consumer integration
//!
//! # CLI Usage
//!
//! ```bash
//! # Full sync from MongoDB
//! surreal-sync from mongodb full --connection-string mongodb://... --database mydb ...
//!
//! # Incremental sync from PostgreSQL (trigger-based)
//! surreal-sync from postgresql-trigger incremental --connection-string postgresql://... ...
//!
//! # WAL-based PostgreSQL sync (continuous)
//! surreal-sync from postgresql --connection-string postgresql://... --tables users,orders ...
//!
//! # Kafka consumer
//! surreal-sync from kafka --bootstrap-servers localhost:9092 --topic events ...
//! ```
//!
//! # Config File (PostgreSQL)
//!
//! PostgreSQL sources support a TOML config file via `-c` / `--config-file`:
//!
//! ```bash
//! surreal-sync from postgresql-trigger full -c surreal-sync.toml
//! ```
//!
//! See the `config::file` module (in the binary crate) for the config file format
//! and struct definitions.

use clap::Parser;
use rustls::crypto::CryptoProvider;
use sync_core::ZeroTemporalPolicy;

pub mod mysql_binlog;
pub(crate) mod sync_helpers;
pub mod testing;
pub mod transforms;

/// Install the TLS crypto provider and initialize tracing (same as the stock binary).
///
/// Called automatically by [`mysql_binlog::run`]. Call this yourself if you use
/// lower-level entrypoints such as [`mysql_binlog::run_sync`].
pub fn init() {
    if let Err(err) = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider())
    {
        eprintln!("Error setting up crypto provider for TLS: {err:?}");
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

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

// Re-export CSV and JSONL crates for convenience
pub use surreal_sync_csv_source as csv;
pub use surreal_sync_jsonl_source as jsonl;

// Transform + core types for embedders (one dependency: surreal-sync)
pub use sync_core::{UniversalChange, UniversalChangeOp, UniversalRow, UniversalValue};
pub use sync_transform::{ApplyOpts, FlattenId, InPlaceTransform, Pipeline};

#[derive(Parser, Clone)]
pub struct SurrealOpts {
    /// SurrealDB endpoint URL
    #[arg(
        long,
        default_value = "http://localhost:8000",
        env = "SURREAL_ENDPOINT"
    )]
    pub surreal_endpoint: String,

    /// SurrealDB username
    #[arg(long, default_value = "root", env = "SURREAL_USERNAME")]
    pub surreal_username: String,

    /// SurrealDB password
    #[arg(long, default_value = "root", env = "SURREAL_PASSWORD")]
    pub surreal_password: String,

    /// Batch size for data migration
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Dry run mode - don't actually write data
    #[arg(long)]
    pub dry_run: bool,

    /// SurrealDB SDK version to use. Auto-detects from server if not specified.
    #[arg(long, env = "SURREAL_SDK_VERSION", value_parser = ["v2", "v3"])]
    pub surreal_sdk_version: Option<String>,

    /// How zero temporal values (e.g. MySQL `0000-00-00`) are written to SurrealDB.
    /// Set via config file `[sink.surrealdb] zero_temporal`; not a CLI flag.
    #[arg(skip)]
    pub zero_temporal: ZeroTemporalPolicy,
}
