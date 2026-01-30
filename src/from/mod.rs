//! Source-specific sync implementations.
//!
//! This module contains the handler implementations for each source database.
//! Each submodule handles both full and incremental sync for its source.
//!
//! ## Module Organization
//!
//! - `common`: Shared utilities (schema loading, connection string parsing)
//! - `mongodb`: MongoDB full and incremental sync using change streams
//! - `neo4j`: Neo4j full and incremental sync using timestamp-based tracking
//! - `postgresql_trigger`: PostgreSQL trigger-based CDC sync
//! - `postgresql_wal2json`: PostgreSQL WAL-based logical replication sync
//! - `mysql`: MySQL trigger-based CDC sync
//! - `kafka`: Kafka streaming sync
//! - `csv`: CSV file import
//! - `jsonl`: JSONL file import

pub mod common;
pub mod csv;
pub mod jsonl;
pub mod kafka;
pub mod mongodb;
pub mod mysql;
pub mod neo4j;
pub mod postgresql_trigger;
pub mod postgresql_wal2json;

// Re-export common utilities (crate-internal only)
pub(crate) use crate::config::parse_duration_to_secs;
pub(crate) use common::{
    extract_json_fields_from_schema, extract_postgresql_database, get_sdk_version,
    load_schema_if_provided, SdkVersion,
};
