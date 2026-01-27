//! SurrealDB connection and write utilities
//!
//! Provides functions for connecting to SurrealDB and writing records/relations.

mod change;
mod connect;
mod rows;
mod write;

pub use change::{Change, ChangeOp};
pub use connect::{surreal_connect, surreal_connect_with_retries, SurrealOpts};
pub use rows::{
    universal_relation_to_surreal_relation, universal_row_to_surreal_record,
    universal_value_to_surreal_id, write_universal_relations, write_universal_rows,
};
pub use write::{
    apply_change, apply_universal_change, write_record, write_records, write_relation,
    write_relations,
};

// Re-export SurrealDB types for use by source crates
pub use surrealdb::engine::any::Any as SurrealEngine;
pub use surrealdb::Surreal;
