//! SurrealDB v2 support (feature `v2`).
//!
//! - [`client`] — test-client wrapper (`Surreal2Client`)
//! - [`types`] — TypedValue ↔ SurrealDB v2 conversions
//! - [`sink`] — connection + write utilities (`Surreal2Sink`)
//! - [`checkpoint`] — table-backed checkpoint store (`Surreal2Store`)
//!
//! ```ignore
//! use surreal_sync_surreal::Surreal2Sink;
//! // or: use surreal_sync_surreal::v2::Surreal2Sink;
//! ```

pub mod checkpoint;
pub mod client;
pub mod sink;
pub mod types;

// Former surreal2 sink-crate root surface.
pub use sink::{
    apply_change, apply_mutation, relation_to_surreal_relation, row_to_surreal_record,
    surreal_connect, surreal_connect_with_retries, value_to_surreal_id, write_native_relations,
    write_record, write_records, write_relation, write_relations, write_rows, Mutation, MutationOp,
    Surreal2Sink, SurrealClient, SurrealEngine, SurrealOpts, SurrealSink, ZeroTemporalPolicy,
};

pub use checkpoint::Surreal2Store;
