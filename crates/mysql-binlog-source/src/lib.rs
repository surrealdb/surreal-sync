//! MySQL/MariaDB binlog CDC source for surreal-sync.

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
pub use change::cdc_change_to_universal;
pub use checkpoint::{get_current_checkpoint, BinlogCheckpoint, BinlogStreamPosition};
pub use flavor::Flavor;
pub use full_sync::{capture_head_checkpoint, run_full_sync, run_full_sync_cancellable};
pub use incremental_sync::{
    run_incremental_sync, run_incremental_sync_with_checkpoints, IncrementalRunMode,
    IncrementalSyncOptions,
};
pub use signal::SIGNAL_TABLE;
pub use watermark_source::{
    request_snapshot, run_interleaved_snapshot_full_sync, BinlogWatermarkSource,
    InterleavedFullSyncOutcome,
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
