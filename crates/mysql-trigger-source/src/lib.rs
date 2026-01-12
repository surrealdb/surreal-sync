//! MySQL trigger-based sync for surreal-sync
//!
//! Uses audit table + triggers for change capture. Works with MySQL 5.6+.
//! For alternative implementations (e.g., binlog CDC), see separate crates.

mod change_tracking;
pub mod checkpoint;
mod client;
mod full_sync;
mod incremental_sync;
mod schema;
mod source;

pub use change_tracking::setup_mysql_change_tracking;
pub use checkpoint::{get_current_checkpoint, MySQLCheckpoint};
pub use client::new_mysql_pool;
pub use full_sync::run_full_sync;
pub use incremental_sync::run_incremental_sync;
pub use source::{ChangeStream, IncrementalSource, MySQLChangeStream, MySQLIncrementalSource};

/// MySQL source connection options
#[derive(Clone, Debug)]
pub struct SourceOpts {
    /// MySQL connection URI
    pub source_uri: String,
    /// Source database name
    pub source_database: Option<String>,
    /// MySQL JSON paths that contain boolean values stored as 0/1
    pub mysql_boolean_paths: Option<Vec<String>>,
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
