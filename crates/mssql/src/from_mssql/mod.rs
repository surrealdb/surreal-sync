//! SQL Server CDC source for surreal-sync.
//!
//! # Embed surface
//!
//! Only [`run`], [`FlattenId`], [`InPlaceTransform`], and [`Value`] are the
//! supported embed API:
//!
//! ```ignore
//! use surreal_sync_mssql::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! run::<Surreal3Sink>([Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>]).await?;
//! ```

pub(crate) mod embed;

mod catalog;
mod cdc;
mod checkpoint;
mod client;
mod incremental_sync;
mod naming;
mod regular;
mod schema;
mod sequential;
mod signal;
mod temporal;
mod watermark_source;

#[doc(hidden)]
pub mod testing;

pub use catalog::{collect_database_schema, list_user_tables, TableSyncKind};
pub use checkpoint::{MssqlCheckpoint, MssqlLsn};
pub use incremental_sync::{
    run_replication_tail, run_replication_tail_with_checkpoints,
    run_replication_tail_with_transforms, ReplicationTailOptions,
};
pub use naming::{detect_collisions, parse_table_ref, target_table_name, QualifiedName};
pub use sequential::run_sequential_snapshot_with_transforms;
pub use signal::SIGNAL_TABLE;
pub use temporal::version_id;
pub use watermark_source::{
    run_initial_interleaved_snapshot, run_initial_interleaved_snapshot_with_transforms,
    run_interleaved_snapshot_full_sync, run_interleaved_snapshot_full_sync_with_transforms,
    ConnectOptions, InterleavedFullSyncOptions, InterleavedFullSyncOutcome, MssqlWatermarkSource,
};

pub use regular::record_id;

/// SQL Server source connection options.
#[derive(Clone, Debug, Default)]
pub struct SourceOpts {
    pub connection_string: String,
    pub tables: Vec<String>,
    pub relation_tables: Vec<String>,
    pub schemafull: bool,
    pub dry_run: bool,
}

/// Sync options (non-connection related).
#[derive(Clone, Debug)]
pub struct SyncOpts {
    pub batch_size: usize,
    pub dry_run: bool,
    /// Emit `DEFINE TABLE` / `FIELD` / `INDEX` before copying rows (opt-in).
    pub schemafull: bool,
}

impl Default for SyncOpts {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            dry_run: false,
            schemafull: false,
        }
    }
}

/// Public embed surface — only these four items are the supported embed API.
pub use embed::{run, FlattenId, InPlaceTransform, Value};

/// Stock CLI argv helpers (`run_sync`, clap args). Not part of the embed API.
#[doc(hidden)]
pub mod cli {
    pub use super::embed::{
        run_args_with_sink, run_sync, Commands, Pipeline, SnapshotModeArg, SyncArgs, SyncStrategy,
        DEFAULT_CHUNK_SIZE,
    };
}
