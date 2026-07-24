//! SurrealDB connection and write settings (plain fields, no CLI parsing).

use crate::ZeroTemporalPolicy;

/// Plain-field SurrealDB connection and write options (no clap).
///
/// Use this from embedders and [`SinkConnect`](super::SinkConnect) helpers.
/// The stock CLI continues to parse argv into clap-derived structs and maps
/// them into this type (or sink-crate connect helpers) at the boundary.
#[derive(Debug, Clone)]
pub struct SurrealConfig {
    /// SurrealDB endpoint URL (`http://…` or `ws://…`).
    pub endpoint: String,
    /// Username for SurrealDB sign-in.
    pub username: String,
    /// Password for SurrealDB sign-in.
    pub password: String,
    /// Target namespace.
    pub namespace: String,
    /// Target database.
    pub database: String,
    /// How zero temporal values are written.
    pub zero_temporal: ZeroTemporalPolicy,
    /// Batch size hint for full-sync writers (sources that honor it).
    pub batch_size: usize,
    /// When true, sources should not write to the sink.
    pub dry_run: bool,
}

impl Default for SurrealConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8000".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            namespace: "test".to_string(),
            database: "test".to_string(),
            zero_temporal: ZeroTemporalPolicy::default(),
            batch_size: 1000,
            dry_run: false,
        }
    }
}

impl SurrealConfig {
    /// Create a config with endpoint / credentials / ns / db.
    pub fn new(
        endpoint: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            username: username.into(),
            password: password.into(),
            namespace: namespace.into(),
            database: database.into(),
            ..Self::default()
        }
    }
}
