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
pub use full_sync::{migrate_table, SyncOpts};
pub use schema::{
    collect_database_schema, collect_database_schema_with_fks, collect_foreign_keys,
    enrich_schema_with_fks,
};
