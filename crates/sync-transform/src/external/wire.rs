//! NDJSON wire headers for external transform requests/responses.

use serde::{Deserialize, Serialize};

/// Item payload kind for an External NDJSON exchange.
///
/// Workers that only handle row CDC may ignore `kind` (default / omitted =
/// [`WireItemKind::Change`]). Relation-aware workers must honor `kind` so
/// relation batches are not silently treated as row changes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum WireItemKind {
    /// Incremental row [`sync_core::Change`] items.
    #[default]
    Change,
    /// Full-sync / snapshot [`sync_core::Row`] items.
    Row,
    /// Incremental relation [`sync_core::RelationChange`] items.
    RelationChange,
    /// Full-sync [`sync_core::Relation`] items.
    Relation,
}

/// Request header line: `{"batch_id","count"}` plus optional `kind`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestHeader {
    pub batch_id: u64,
    pub count: usize,
    /// Payload kind. Omitted / default = [`WireItemKind::Change`] for back-compat.
    #[serde(default, skip_serializing_if = "is_default_kind")]
    pub kind: WireItemKind,
}

fn is_default_kind(kind: &WireItemKind) -> bool {
    *kind == WireItemKind::Change
}

/// Response header line: must echo `batch_id`; either `count` + items or `error`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseHeader {
    pub batch_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
