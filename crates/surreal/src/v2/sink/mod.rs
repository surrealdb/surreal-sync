//! SurrealDB v2 connection and write utilities
//!
//! Provides functions for connecting to SurrealDB v2 and writing records/relations.

mod change;
mod connect;
mod rows;
mod sink_impl;
mod write;

pub use change::{Mutation, MutationOp};
pub use connect::{surreal_connect, surreal_connect_with_retries, SurrealOpts};
pub use rows::{
    relation_to_surreal_relation, row_to_surreal_record, value_to_surreal_id, write_relations,
    write_rows,
};
pub use sink_impl::Surreal2Sink;
pub use surreal_sync_core::ZeroTemporalPolicy;
pub use write::{
    apply_change, apply_mutation, write_native_relations, write_record, write_records,
    write_relation,
};

// Re-export SurrealDB types for use by source crates
pub use surrealdb2::engine::any::Any as SurrealEngine;
pub use surrealdb2::Surreal;

/// Connected SurrealDB v2 client used by [`Surreal2Sink`].
pub type SurrealClient = Surreal<SurrealEngine>;

// Re-export the SurrealSink trait
pub use surreal_sync_core::SurrealSink;
