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
//! - [`neo4j_incremental`] - Neo4j timestamp-based tracking
//! - [`mongodb_incremental`] - MongoDB change streams
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
use surrealdb::Surreal;

pub mod checkpoint;
pub mod jsonl;
pub mod kafka;
pub mod mongodb;
pub mod mysql;
pub mod neo4j;
pub mod postgresql;
pub mod surreal;
pub mod sync;
pub mod testing;

pub mod connect;

// Re-export types and schema functionality for easy access
pub use surreal::{
    json_to_surreal_without_schema, Change, Record, Relation, SurrealDatabaseSchema,
    SurrealTableSchema, SurrealType, SurrealValue,
};

// Re-export main migration functions for easy access
pub use jsonl::migrate_from_jsonl;
pub use mongodb::migrate_from_mongodb;

/// Parsed configuration for Neo4j JSON-to-object conversion
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Neo4jJsonProperty {
    pub label: String,
    pub property: String,
}

impl Neo4jJsonProperty {
    /// Parse "Label.property" format into Neo4jJsonProperty
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid Neo4j JSON property format: '{s}'. Expected format: 'NodeLabel.propertyName'",
            );
        }
        Ok(Neo4jJsonProperty {
            label: parts[0].to_string(),
            property: parts[1].to_string(),
        })
    }

    /// Parse multiple entries from CLI argument
    pub fn parse_vec(entries: &[String]) -> anyhow::Result<Vec<Self>> {
        entries.iter().map(|s| Self::parse(s)).collect()
    }
}

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

// Apply a single change event to SurrealDB
async fn apply_change(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    change: &Change,
) -> anyhow::Result<()> {
    match change {
        Change::UpsertRecord(record) => {
            write_record(surreal, record).await?;

            tracing::trace!("Successfully upserted record: {record:?}");
        }
        Change::DeleteRecord(thing) => {
            let query = "DELETE $record_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);
            log::info!("🔧 migrate_change executing: {query} for record: {thing:?}");

            let mut q = surreal.query(query);
            q = q.bind(("record_id", thing.clone()));

            q.await?;

            tracing::trace!("Successfully deleted record: {:?}", thing);
        }
        Change::UpsertRelation(relation) => {
            write_relation(surreal, relation).await?;

            tracing::trace!("Successfully upserted relation: {relation:?}");
        }
        Change::DeleteRelation(thing) => {
            let query = "DELETE $relation_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);
            log::info!("🔧 migrate_change executing: {query} for relation: {thing:?}");

            let mut q = surreal.query(query);
            q = q.bind(("relation_id", thing.clone()));
            q.await?;
            tracing::trace!("Successfully deleted relation: {thing:?}");
        }
    }

    tracing::debug!("Successfully applied {change:?}");

    Ok(())
}

// Write a single record to SurrealDB using UPSERT
async fn write_record(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    document: &Record,
) -> anyhow::Result<()> {
    let upsert_content = document.get_upsert_content();
    let record_id = &document.id;

    // Build parameterized query using proper variable binding to prevent injection
    let query = "UPSERT $record_id CONTENT $content".to_string();

    tracing::trace!("Executing SurrealDB query with flattened fields: {}", query);

    log::info!("🔧 migrate_batch executing: {query} for record: {record_id:?}");

    // Add debug logging to see the document being bound
    if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
        tracing::debug!("Binding document to SurrealDB query for record {document:?}",);
    }

    // Build query with proper parameter binding
    let mut q = surreal.query(query);
    q = q.bind(("record_id", record_id.clone()));
    q = q.bind(("content", upsert_content.clone()));

    let mut response: surrealdb::Response = q.await.map_err(|e| {
        tracing::error!(
            "SurrealDB query execution failed for record {:?}: {}",
            record_id,
            e
        );
        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
            tracing::error!("Failed query content: {upsert_content:?}");
        }
        e
    })?;

    let result: Result<Vec<surrealdb::sql::Thing>, surrealdb::Error> =
        response.take("id").map_err(|e| {
            tracing::error!(
                "SurrealDB response.take() failed for record {:?}: {}",
                record_id,
                e
            );
            e
        });

    match result {
        Ok(res) => {
            if res.is_empty() {
                tracing::warn!("Failed to create record: {:?}", record_id);
            } else {
                tracing::trace!("Successfully created record: {:?}", record_id);
            }
        }
        Err(e) => {
            tracing::error!("Error creating record {:?}: {}", record_id, e);
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::error!("Problematic document: {:?}", document);
            }
            return Err(e.into());
        }
    }

    Ok(())
}

// Write a batch of records to SurrealDB using UPSERT
pub async fn write_records(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[Record],
) -> anyhow::Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for (i, r) in batch.iter().enumerate() {
        tracing::trace!("Processing record {}/{}", i + 1, batch.len());
        write_record(surreal, r).await?;
    }

    tracing::debug!(
        "Completed migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}

async fn write_relation(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    r: &Relation,
) -> anyhow::Result<()> {
    // Build parameterized query using proper variable binding to prevent injection
    let query = format!("RELATE $in->{}->$out CONTENT $content", r.id.tb);

    let record_id = &r.id;

    tracing::trace!("Executing SurrealDB query with flattened fields: {}", query);
    log::info!("🔧 migrate_batch executing: {query} for record: {record_id:?}");

    // Add debug logging to see the document being bound
    if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
        tracing::debug!(
            "Binding document to SurrealDB query for record {:?}: {:?}",
            record_id,
            r
        );
    }

    // Build query with proper parameter binding
    let mut q = surreal.query(query);
    q = q.bind(("in", r.get_in()));
    q = q.bind(("out", r.get_out()));
    q = q.bind(("content", r.get_relate_content()));

    let mut response: surrealdb::Response = q.await.map_err(|e| {
        tracing::error!(
            "SurrealDB query execution failed for record {:?}: {}",
            record_id,
            e
        );
        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
            tracing::error!("Failed query content: {:?}", r.get_relate_content());
        }
        e
    })?;

    let result: Result<Vec<surrealdb::sql::Thing>, surrealdb::Error> =
        response.take("id").map_err(|e| {
            tracing::error!(
                "SurrealDB response.take() failed for record {:?}: {}",
                record_id,
                e
            );
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::error!("Response take error content: {:?}", r.get_relate_content());
            }
            e
        });

    match result {
        Ok(res) => {
            if res.is_empty() {
                tracing::warn!("Failed to create record: {:?}", record_id);
            } else {
                tracing::trace!("Successfully created record: {:?}", record_id);
            }
        }
        Err(e) => {
            tracing::error!("Error creating record {:?}: {}", record_id, e);
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::error!("Problematic document: {:?}", r);
            }
            return Err(e.into());
        }
    }

    Ok(())
}

pub async fn write_relations(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[Relation],
) -> anyhow::Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for (i, r) in batch.iter().enumerate() {
        tracing::trace!("Processing record {}/{}", i + 1, batch.len());
        write_relation(surreal, r).await?;
    }

    tracing::debug!(
        "Completed migrating relations for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}
