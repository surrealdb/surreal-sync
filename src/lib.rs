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
//! # Database-Specific Sync Crates
//!
//! Each database has its own dedicated sync crate:
//!
//! - `surreal_sync_neo4j` - Neo4j timestamp-based tracking
//! - `surreal_sync_mongodb` - MongoDB change streams
//! - `surreal_sync_postgresql_trigger` - PostgreSQL trigger-based tracking
//! - `surreal_sync_mysql_trigger` - MySQL audit table tracking
//! - `surreal_sync_kafka` - Kafka consumer integration
//!
//! # Quick Start
//!
//! ```ignore
//! // PostgreSQL incremental sync using the dedicated crate
//! use surreal_sync_postgresql_trigger::{PostgresIncrementalSource, PostgreSQLCheckpoint};
//!
//! let checkpoint = PostgreSQLCheckpoint::from_cli_string("postgresql:sequence:0")?;
//! let client = surreal_sync_postgresql_trigger::new_postgresql_client(&connection_string).await?;
//! let source = PostgresIncrementalSource::new(client, checkpoint.sequence_id);
//! // ... run incremental sync
//! ```

use clap::Parser;

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

// CLI type → MySQL trigger library type conversions
impl From<&SourceOpts> for surreal_sync_mysql_trigger::SourceOpts {
    fn from(opts: &SourceOpts) -> Self {
        Self {
            source_uri: opts.source_uri.clone(),
            source_database: opts.source_database.clone(),
            mysql_boolean_paths: opts.mysql_boolean_paths.clone(),
        }
    }
}

impl From<&SurrealOpts> for surreal_sync_mysql_trigger::SurrealOpts {
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

// CLI type → PostgreSQL trigger library type conversions
impl From<&SourceOpts> for surreal_sync_postgresql_trigger::SourceOpts {
    fn from(opts: &SourceOpts) -> Self {
        Self {
            source_uri: opts.source_uri.clone(),
            source_database: opts.source_database.clone(),
        }
    }
}

// CLI type → PostgreSQL shared library type conversions (used by trigger and logical-replication)
impl From<&SurrealOpts> for surreal_sync_postgresql::SurrealOpts {
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

// CLI type → Kafka library type conversions
impl From<&SurrealOpts> for surreal_sync_kafka::SurrealOpts {
    fn from(opts: &SurrealOpts) -> Self {
        Self {
            surreal_endpoint: opts.surreal_endpoint.clone(),
            surreal_username: opts.surreal_username.clone(),
            surreal_password: opts.surreal_password.clone(),
        }
    }
}

// CLI type → PostgreSQL logical replication library type conversions
impl From<&SurrealOpts> for surreal_sync_postgresql_logical_replication::sync::SurrealOpts {
    fn from(opts: &SurrealOpts) -> Self {
        Self {
            surreal_endpoint: opts.surreal_endpoint.clone(),
            surreal_username: opts.surreal_username.clone(),
            surreal_password: opts.surreal_password.clone(),
        }
    }
}
