//! SurrealDB v3 support (feature `v3`).
//!
//! - [`client`] — test-client wrapper (`Surreal3Client`)
//! - [`types`] — TypedValue ↔ SurrealDB v3 conversions
//! - [`sink`] — connection + write utilities (`Surreal3Sink`)
//! - [`checkpoint`] — table-backed checkpoint store (`Surreal3Store`)
//!
//! ```ignore
//! use surreal_sync_surreal::Surreal3Sink;
//! // or: use surreal_sync_surreal::v3::Surreal3Sink;
//! ```

pub mod checkpoint;
pub mod client;
pub mod sink;
pub mod types;

// Former surreal3 sink-crate root surface.
pub use sink::{
    apply_change, apply_mutation, relation_to_surreal_relation, row_to_surreal_record,
    surreal_connect, surreal_connect_with_retries, value_to_surreal_id, write_native_relations,
    write_record, write_records, write_relation, write_relations, write_rows, Mutation, MutationOp,
    Surreal3Sink, SurrealClient, SurrealEngine, SurrealOpts, SurrealSink, ZeroTemporalPolicy,
};

pub use checkpoint::Surreal3Store;
