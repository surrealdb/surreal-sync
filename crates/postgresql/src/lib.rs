//! PostgreSQL utilities and CDC origins for surreal-sync.
//!
//! Shared helpers (`client`, `autoconf`, `full_sync`, `schema`, …) are always
//! available. Origin modules are feature-gated:
//!
//! - `types` (default) — type conversions / DDL
//! - `pgoutput_protocol` — thin pgoutput/pg_walstream API
//! - `from_pgoutput` — pgoutput WAL CDC origin
//! - `from_wal2json` — wal2json logical replication origin
//! - `from_trigger` — trigger / audit-table CDC origin

mod autoconf;
mod client;
pub mod fk_transform;
mod full_sync;
pub mod schema;
pub mod testing;

/// Type conversions / DDL (always available; `types` feature is the default opt-in marker).
pub mod types;

#[cfg(feature = "pgoutput_protocol")]
pub mod pgoutput_protocol;

#[cfg(feature = "from_pgoutput")]
pub mod from_pgoutput;

#[cfg(feature = "from_wal2json")]
pub mod from_wal2json;

#[cfg(feature = "from_trigger")]
pub mod from_trigger;

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
