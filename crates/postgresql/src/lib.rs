//! Shared PostgreSQL utilities for surreal-sync
//!
//! This crate provides common PostgreSQL functionality used by both
//! `postgresql-trigger` and `postgresql-logical-replication` sub-crates:
//!
//! - Client connection utilities
//! - Table discovery (autoconf)
//! - Table migration to SurrealDB

mod autoconf;
mod client;
pub mod fk_transform;
mod full_sync;
pub mod schema;
pub mod testing;

pub use autoconf::get_user_tables;
pub use client::new_postgresql_client;
#[allow(deprecated)]
pub use full_sync::{
    convert_table, get_primary_key_columns, migrate_table, read_offset_relation_chunk,
    read_offset_table_chunk, read_relation_chunk, read_table_chunk, RelationChunk, SyncOpts,
    TableChunk,
};
pub use schema::{
    collect_database_schema, collect_database_schema_with_fks, collect_foreign_keys,
    enrich_schema_with_fks,
};
