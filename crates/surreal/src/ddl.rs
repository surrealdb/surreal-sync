//! SurrealQL SCHEMAFULL helpers (`DEFINE TABLE` / `FIELD` / `INDEX`).
//!
//! Re-exported from `surreal-sync-core` so origin crates can emit DDL through
//! [`SurrealSink::query`](surreal_sync_core::SurrealSink::query) without linking
//! a SurrealDB SDK.

pub use surreal_sync_core::ddl::{
    emit_schemafull, ident, maybe_emit_schemafull, schemafull_statements, PeriodBound,
    SchemaFieldExtra, SchemaIndex, SchemafullExtras, SurrealDdl,
};
