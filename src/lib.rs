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
//! # Incremental Sync Architecture
//!
//! The library provides a universal incremental sync design that works across all
//! supported databases. See [`sync`] module for the core architecture and
//! individual database modules for specific implementations:
//!
//! - `surreal_sync_neo4j` - Neo4j timestamp-based tracking
//! - `surreal_sync_mongodb` - MongoDB change streams
//! - [`postgresql_incremental`] - PostgreSQL trigger-based tracking
//! - [`mysql_incremental`] - MySQL audit table tracking
//!
//! # Quick Start
//!
//! ```ignore
//! use surreal_sync::sync::{IncrementalSource, SyncCheckpoint};
//!
//! // PostgreSQL incremental sync
//! let mut source = postgresql_incremental::PostgresIncrementalSource::new(
//!     "postgres://user:pass@localhost/db"
//! ).await?;
//!
//! source.initialize(None).await?;
//! let mut stream = source.get_changes().await?;
//!
//! while let Some(change) = stream.next().await {
//!     // Process change...
//! }
//! ```

use clap::Parser;

pub mod checkpoint;
pub mod kafka;
pub mod mysql;
pub mod postgresql;
pub mod sync;
pub mod testing;

// Re-export CSV and JSONL crates for convenience
pub use surreal_sync_csv as csv;
pub use surreal_sync_jsonl as jsonl;

#[derive(Parser, Clone)]
pub struct SourceOpts {
    /// Source database connection string/URI
    #[arg(long, env = "SOURCE_URI")]
    pub source_uri: String,

    /// Source database name
    #[arg(long, env = "SOURCE_DATABASE")]
    pub source_database: Option<String>,

    /// Source database username
    #[arg(long, env = "SOURCE_USERNAME")]
    pub source_username: Option<String>,

    /// Source database password
    #[arg(long, env = "SOURCE_PASSWORD")]
    pub source_password: Option<String>,

    /// Timezone to use for converting Neo4j local datetime and time values
    /// Format: IANA timezone name (e.g., "America/New_York", "Europe/London", "UTC")
    /// Default: "UTC"
    #[arg(long, default_value = "UTC", env = "NEO4J_TIMEZONE")]
    pub neo4j_timezone: String,

    /// MySQL JSON paths that contain boolean values stored as 0/1
    /// Format: comma-separated entries like "users.metadata=settings.notifications,posts.config=enabled"
    /// Each entry: "tablename.columnname=json.path.to.bool"
    #[arg(long, value_delimiter = ',', env = "MYSQL_BOOLEAN_PATHS")]
    pub mysql_boolean_paths: Option<Vec<String>>,

    /// Neo4j properties that should be converted from JSON strings to SurrealDB objects
    /// Format: comma-separated entries like "User.metadata,Post.config"
    /// Each entry: "NodeLabel.propertyName"
    #[arg(long, value_delimiter = ',', env = "NEO4J_JSON_PROPERTIES")]
    pub neo4j_json_properties: Option<Vec<String>>,
}

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
}

// CLI type → MongoDB library type conversions
impl From<&SourceOpts> for surreal_sync_mongodb::SourceOpts {
    fn from(opts: &SourceOpts) -> Self {
        Self {
            source_uri: opts.source_uri.clone(),
            source_database: opts.source_database.clone(),
        }
    }
}

impl From<&SurrealOpts> for surreal_sync_mongodb::SurrealOpts {
    fn from(opts: &SurrealOpts) -> Self {
        Self {
            surreal_endpoint: opts.surreal_endpoint.clone(),
            surreal_username: opts.surreal_username.clone(),
            surreal_password: opts.surreal_password.clone(),
            batch_size: opts.batch_size,
            dry_run: opts.dry_run,
        }
    }
}

// CLI type → Neo4j library type conversions
impl From<&SourceOpts> for surreal_sync_neo4j::SourceOpts {
    fn from(opts: &SourceOpts) -> Self {
        Self {
            source_uri: opts.source_uri.clone(),
            source_database: opts.source_database.clone(),
            source_username: opts.source_username.clone(),
            source_password: opts.source_password.clone(),
            neo4j_timezone: opts.neo4j_timezone.clone(),
            neo4j_json_properties: opts.neo4j_json_properties.clone(),
        }
    }
}

impl From<&SurrealOpts> for surreal_sync_neo4j::SurrealOpts {
    fn from(opts: &SurrealOpts) -> Self {
        Self {
            surreal_endpoint: opts.surreal_endpoint.clone(),
            surreal_username: opts.surreal_username.clone(),
            surreal_password: opts.surreal_password.clone(),
            batch_size: opts.batch_size,
            dry_run: opts.dry_run,
        }
    }
}
