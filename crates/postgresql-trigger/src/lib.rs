//! PostgreSQL trigger-based sync for surreal-sync
//!
//! Uses audit table + triggers for change capture. Works with PostgreSQL 9.5+.
//! For alternative implementations (e.g., WAL logical replication), see separate crates.

mod autoconf;
pub mod checkpoint;
mod client;
mod full_sync;
mod incremental_sync;
mod schema;

pub use checkpoint::{get_current_checkpoint, PostgreSQLCheckpoint};
pub use client::new_postgresql_client;
pub use full_sync::{migrate_table, run_full_sync};
pub use incremental_sync::{
    run_incremental_sync, ChangeStream, IncrementalSource, PostgresChangeStream,
    PostgresIncrementalSource,
};

/// PostgreSQL source connection options
#[derive(Clone, Debug)]
pub struct SourceOpts {
    /// PostgreSQL connection URI
    pub source_uri: String,
    /// Source database/schema name
    pub source_database: Option<String>,
}

/// SurrealDB connection options
#[derive(Clone, Debug)]
pub struct SurrealOpts {
    /// SurrealDB endpoint URL
    pub surreal_endpoint: String,
    /// SurrealDB username
    pub surreal_username: String,
    /// SurrealDB password
    pub surreal_password: String,
    /// Batch size for data migration
    pub batch_size: usize,
    /// Dry run mode - don't actually write data
    pub dry_run: bool,
}
