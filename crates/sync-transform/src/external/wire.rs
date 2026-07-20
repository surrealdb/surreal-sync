//! NDJSON wire headers for external transform requests/responses.

use serde::{Deserialize, Serialize};

/// Request header line: `{"batch_id","count"}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestHeader {
    pub batch_id: u64,
    pub count: usize,
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
