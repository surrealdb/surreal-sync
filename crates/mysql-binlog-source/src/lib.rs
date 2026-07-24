//! MySQL/MariaDB binlog CDC source for surreal-sync.

mod catch_up;
mod change;
mod checkpoint;
mod client;
mod ddl;
mod flavor;
mod full_sync;
mod incremental_sync;
mod schema;
mod signal;
mod watermark_source;

#[doc(hidden)]
pub mod testing;

pub use testing::{MariaDBBinlogContainer, MySQLBinlogContainer};

pub use binlog_protocol::{MariaDbGtidStrictMode, SslMode, SslOptions};
pub use catch_up::{
    effective_sync_tables, emit_catch_up_progress, max_binlog_checkpoint, read_catch_up_progress,
    tables_pending_snapshot, CatchUpProgress, CoverageKind, TableCoverageEntry,
};
pub use change::cdc_to_change;
pub use checkpoint::{get_current_checkpoint, BinlogCheckpoint, BinlogReconciliationPos};
pub use client::{connect_binlog_client, new_mysql_pool_with_ssl, parse_mysql_uri};
pub use flavor::Flavor;
pub use full_sync::{
    capture_head_checkpoint, run_full_sync, run_full_sync_cancellable,
    run_full_sync_cancellable_with_transforms,
};
pub use incremental_sync::{
    run_replication_tail, run_replication_tail_with_checkpoints,
    run_replication_tail_with_transforms, ReplicationTailOptions,
};
pub use signal::SIGNAL_TABLE;
pub use watermark_source::{
    request_snapshot, run_initial_interleaved_snapshot,
    run_initial_interleaved_snapshot_with_transforms, run_interleaved_snapshot_full_sync,
    run_interleaved_snapshot_full_sync_with_transforms, BinlogWatermarkSource, ConnectOptions,
    InterleavedFullSyncOptions, InterleavedFullSyncOutcome,
};

/// MySQL binlog source connection options.
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub connection_string: String,
    pub database: Option<String>,
    pub tables: Vec<String>,
    pub server_id: Option<u32>,
    pub flavor: Option<Flavor>,
    pub ssl: SslMode,
    pub mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode,
}

/// Sync options (non-connection related).
#[derive(Clone, Debug)]
pub struct SyncOpts {
    pub batch_size: usize,
    pub dry_run: bool,
}
