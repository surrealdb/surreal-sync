//! PostgreSQL trigger-based sync for surreal-sync
//!
//! Uses audit table + triggers for change capture. Works with PostgreSQL 9.5+.
//! For alternative implementations (e.g., WAL logical replication), see separate crates.

pub mod checkpoint;
mod full_sync;
mod incremental_sync;
pub mod schema;

pub use checkpoint::{get_current_checkpoint, PostgreSQLCheckpoint};
pub use full_sync::run_full_sync;
pub use incremental_sync::{
    run_incremental_sync, ChangeStream, IncrementalSource, PostgresChangeStream,
    PostgresIncrementalSource,
};

/// PostgreSQL source connection options (trigger-specific)
#[derive(Clone, Debug)]
pub struct SourceOpts {
    /// PostgreSQL connection URI
    pub source_uri: String,
    /// Source database/schema name
    pub source_database: Option<String>,
    /// Tables to sync (empty means all tables)
    pub tables: Vec<String>,
}
