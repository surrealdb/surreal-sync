//! CLI argument definitions for the loadtest verifier.

use clap::Args;
use std::path::PathBuf;

/// Arguments for verifying synced data in SurrealDB.
#[derive(Args, Clone, Debug)]
pub struct VerifyArgs {
    /// Path to schema YAML file (same as used for populate)
    #[arg(long, short = 's')]
    pub schema: PathBuf,

    /// Random seed (must match the seed used during populate for deterministic verification)
    #[arg(long, default_value = "42")]
    pub seed: u64,

    /// Number of rows to verify per table
    #[arg(long, default_value = "1000")]
    pub row_count: u64,

    /// Specific tables to verify (comma-separated, empty = all tables from schema)
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// SurrealDB endpoint URL
    #[arg(
        long,
        default_value = "http://localhost:8000",
        env = "SURREAL_ENDPOINT"
    )]
    pub surreal_endpoint: String,

    /// SurrealDB namespace
    #[arg(long, default_value = "test", env = "SURREAL_NAMESPACE")]
    pub surreal_namespace: String,

    /// SurrealDB database
    #[arg(long, default_value = "test", env = "SURREAL_DATABASE")]
    pub surreal_database: String,

    /// SurrealDB username
    #[arg(long, default_value = "root", env = "SURREAL_USERNAME")]
    pub surreal_username: String,

    /// SurrealDB password
    #[arg(long, default_value = "root", env = "SURREAL_PASSWORD")]
    pub surreal_password: String,
}
