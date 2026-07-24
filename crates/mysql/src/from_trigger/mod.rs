//! MySQL trigger-based sync for surreal-sync
//!
//! Uses audit table + triggers for change capture. Works with MySQL 5.6+.
//! Enable feature `from_binlog` for the binlog CDC origin.

mod change_tracking;
pub mod checkpoint;
mod client;
mod full_sync;
mod incremental_sync;
mod interleaved_snapshot;
pub mod json_columns;
mod schema;
mod source;

/// Testing utilities for MySQL trigger source
pub mod testing;

pub use crate::{get_primary_key_columns, read_table_chunk, TableChunk};
pub use change_tracking::setup_mysql_change_tracking;
pub use checkpoint::{get_current_checkpoint, MySQLCheckpoint};
pub use client::{new_mysql_pool, new_mysql_pool_with_ssl};
pub use full_sync::{run_full_sync, run_full_sync_with_transforms};
pub use incremental_sync::{
    run_incremental_sync, run_incremental_sync_with_transforms, ReplicationTailOptions,
};
pub use interleaved_snapshot::{
    request_snapshot, run_interleaved_snapshot_full_sync,
    run_interleaved_snapshot_full_sync_result,
    run_interleaved_snapshot_full_sync_result_with_overrides,
    run_interleaved_snapshot_full_sync_with_transforms,
    run_interleaved_snapshot_full_sync_with_transforms_and_overrides, MySqlWatermarkSource,
};
pub use source::{ChangeStream, IncrementalSource, MySQLChangeStream, MySQLIncrementalSource};

pub use crate::ssl::{SslMode, SslOptions};

/// MySQL source connection options
#[derive(Clone, Debug, Default)]
pub struct SourceOpts {
    /// MySQL connection URI
    pub source_uri: String,
    /// Source database name
    pub source_database: Option<String>,
    /// Tables to sync (empty means all tables)
    pub tables: Vec<String>,
    /// MySQL JSON paths that contain boolean values stored as 0/1
    pub mysql_boolean_paths: Option<Vec<String>>,
    /// Optional per-table primary-key overrides (`table → ordered columns`).
    pub id_column_overrides: surreal_sync_core::IdColumnOverrides,
    /// TLS mode for the SQL connection pool
    pub ssl: SslMode,
}

/// Sync options (non-connection related)
#[derive(Clone, Debug)]
pub struct SyncOpts {
    /// Batch size for data migration
    pub batch_size: usize,
    /// Dry run mode - don't actually write data
    pub dry_run: bool,
}
