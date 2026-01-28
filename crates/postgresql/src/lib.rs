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
mod full_sync;

pub use autoconf::get_user_tables;
pub use client::new_postgresql_client;
pub use full_sync::{migrate_table, SyncOpts};
