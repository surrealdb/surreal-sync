//! Clap-derived SurrealDB options for argv-parsing `run` entrypoints.
//!
//! Prefer [`crate::SurrealConfig`] when not parsing CLI flags.

use clap::Args;
use surreal_sync_core::ZeroTemporalPolicy;

use crate::SurrealConfig;

/// Clap SurrealDB connection / write flags (shared by stock CLI and embed `run`).
#[derive(Args, Clone, Debug)]
pub struct SurrealCliOpts {
    /// SurrealDB endpoint URL
    #[arg(
        long,
        default_value = "http://localhost:8000",
        env = "SURREAL_ENDPOINT"
    )]
    pub surreal_endpoint: String,

    /// SurrealDB username
    #[arg(long, default_value = "root", env = "SURREAL_USERNAME")]
    pub surreal_username: String,

    /// SurrealDB password
    #[arg(long, default_value = "root", env = "SURREAL_PASSWORD")]
    pub surreal_password: String,

    /// Batch size for data migration
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Dry run mode - don't actually write data
    #[arg(long)]
    pub dry_run: bool,

    /// SurrealDB SDK version to use. Auto-detects from server if not specified.
    ///
    /// **CLI-only:** embed `run::<OneSink>` ignores this and monomorphizes one sink.
    #[arg(long, env = "SURREAL_SDK_VERSION", value_parser = ["v2", "v3"])]
    pub surreal_sdk_version: Option<String>,

    /// How zero temporal values are written to SurrealDB.
    #[arg(skip)]
    pub zero_temporal: ZeroTemporalPolicy,
}

impl SurrealCliOpts {
    /// Build a [`SurrealConfig`] for [`SinkConnect`](crate::SinkConnect).
    pub fn to_config(
        &self,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> SurrealConfig {
        SurrealConfig {
            endpoint: self.surreal_endpoint.clone(),
            username: self.surreal_username.clone(),
            password: self.surreal_password.clone(),
            namespace: namespace.into(),
            database: database.into(),
            zero_temporal: self.zero_temporal,
            batch_size: self.batch_size,
            dry_run: self.dry_run,
        }
    }
}
