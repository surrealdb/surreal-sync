//! PostgreSQL pgoutput WAL CDC source for surreal-sync.

mod catch_up;
mod change;
mod checkpoint;
mod client;
mod ddl;
mod full_sync;
mod incremental_sync;
mod schema;
mod signal;
mod watermark_source;

#[doc(hidden)]
pub mod testing;

pub use testing::PostgresPgoutputContainer;

pub use catch_up::{
    effective_sync_tables, emit_catch_up_progress, max_pgoutput_checkpoint, read_catch_up_progress,
    tables_pending_snapshot, CatchUpProgress, CoverageKind, TableCoverageEntry,
};
pub use change::cdc_change_to_universal;
pub use checkpoint::{get_current_checkpoint, PgoutputCheckpoint, PgoutputReconciliationPos};
pub use full_sync::{
    capture_head_checkpoint, run_full_sync, run_full_sync_cancellable,
    run_full_sync_cancellable_with_transforms,
};
pub use incremental_sync::{
    run_replication_tail, run_replication_tail_with_checkpoints,
    run_replication_tail_with_transforms, ReplicationTailOptions,
};
pub use pgoutput_protocol::Lsn;
pub use signal::SIGNAL_TABLE;
pub use watermark_source::{
    request_snapshot, run_initial_interleaved_snapshot,
    run_initial_interleaved_snapshot_with_transforms, run_interleaved_snapshot_full_sync,
    run_interleaved_snapshot_full_sync_with_transforms, ConnectOptions, InterleavedFullSyncOptions,
    InterleavedFullSyncOutcome, PgoutputWatermarkSource,
};

/// PostgreSQL WAL source connection options.
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub connection_string: String,
    pub schema: String,
    pub tables: Vec<String>,
    pub slot_name: String,
    pub publication_name: String,
}

impl Default for SourceOpts {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            schema: "public".to_string(),
            tables: Vec::new(),
            slot_name: "surreal_sync_slot".to_string(),
            publication_name: "surreal_sync_pub".to_string(),
        }
    }
}

/// Sync options (non-connection related).
#[derive(Clone, Debug)]
pub struct SyncOpts {
    pub batch_size: usize,
    pub dry_run: bool,
}
