//! Transform pipeline framework for surreal-sync.
//!
//! Layering:
//!
//! ```text
//! InPlaceTransform          ← core (per-item + slice helpers)
//!         ↑
//! CowBatch::apply_inplace   ← adapter (Arc COW sharing via make_mut)
//!         ↑
//! Pipeline                  ← InPlace | External (child-stdio)
//!         ↑
//! ApplyContext / run_change_feed / write_rows   ← ChangeFeed framework
//! ```
//!
//! An empty [`Pipeline`] is identity: docs pass through with **zero** transform
//! dispatch overhead. The apply framework gates on
//! [`BatchTransformer::is_identity`] (for [`Pipeline`], equivalent to
//! [`Pipeline::is_identity`] — only an empty stage list). A lone
//! [`Passthrough`] stage is *not* identity.

mod apply;
mod config;
mod cow;
mod external;
mod framer;
mod inplace;
mod pipeline;

pub use apply::{
    apply_changes, apply_changes_with, run_change_feed, run_change_feed_with, write_rows,
    write_rows_with, ApplyContext, ApplyOpts, BatchTransformer, ChangeFeed, FailurePolicy,
    PositionedChange,
};
pub use config::{
    ensure_command_resolvable, load_pipeline_and_opts, load_transforms_config, parse_humantime,
    parse_transforms_toml, ConfiguredStage, ExternalStageConfig, FramerKind, StdinConfig,
    TransformsConfig, TransportKind,
};
pub use cow::CowBatch;
pub use external::{
    ChildStdioMode, ExternalTransform, ExternalTransport, PersistentChildStdio, RequestHeader,
    ResponseHeader, TransientChildStdio, WireResponse,
};
pub use framer::{Framer, NdjsonFramer};
pub use inplace::{InPlaceTransform, Passthrough};
pub use pipeline::{Pipeline, Stage};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod apply_tests;

#[cfg(test)]
mod external_tests;

// Compile-time Send + Sync assertions for types used across async apply paths.
const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}

    fn check() {
        assert_send_sync::<Pipeline>();
        assert_send_sync::<CowBatch<sync_core::UniversalRow>>();
        assert_send_sync::<CowBatch<sync_core::UniversalChange>>();
        assert_send_sync::<ApplyOpts>();
        assert_send_sync::<ExternalTransform>();
        assert_send_sync::<NdjsonFramer>();
    }

    let _ = check;
};
