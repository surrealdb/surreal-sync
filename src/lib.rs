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
//! # CLI Usage
//!
//! ```bash
//! # Full sync from MongoDB
//! surreal-sync from mongodb full --connection-string mongodb://... --database mydb ...
//!
//! # Incremental sync from PostgreSQL (trigger-based)
//! surreal-sync from postgresql-trigger incremental --connection-string postgresql://... ...
//!
//! # WAL-based PostgreSQL sync (continuous)
//! surreal-sync from postgresql --connection-string postgresql://... --tables users,orders ...
//!
//! # Kafka consumer
//! surreal-sync from kafka --bootstrap-servers localhost:9092 --topic events ...
//! ```

use clap::Parser;

pub mod testing;

// Re-export CSV and JSONL crates for convenience
pub use surreal_sync_csv as csv;
pub use surreal_sync_jsonl as jsonl;

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
