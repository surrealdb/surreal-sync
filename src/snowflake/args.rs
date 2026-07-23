//! Command-line flags for Snowflake ingestion.
//!
//! Embedders parse these via [`super::run`] (same flags as `surreal-sync from snowflake`,
//! without the `from snowflake` prefix). The stock binary nests the same type
//! under `from`.

use clap::Args as ClapArgs;
use std::path::PathBuf;

use crate::SurrealOpts;

/// Flags for a one-shot Snowflake → SurrealDB import.
///
/// Match `surreal-sync from snowflake …`. Env vars (`SNOWFLAKE_*`, `SURREAL_*`)
/// work the same way.
#[derive(ClapArgs, Clone)]
pub struct Args {
    /// Snowflake account identifier (as used in the host
    /// `<account>.snowflakecomputing.com`, e.g. "myorg-myaccount")
    #[arg(long, env = "SNOWFLAKE_ACCOUNT")]
    pub account: String,

    /// Snowflake user whose key-pair is registered for JWT auth
    #[arg(long, env = "SNOWFLAKE_USER")]
    pub user: String,

    /// Path to the unencrypted PKCS#8 private key PEM file
    #[arg(long, value_name = "PATH", env = "SNOWFLAKE_PRIVATE_KEY_PATH")]
    pub private_key_path: PathBuf,

    /// Passphrase for an encrypted private key (currently unsupported)
    #[arg(long, env = "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")]
    pub private_key_passphrase: Option<String>,

    /// Virtual warehouse used to run the queries
    #[arg(long, env = "SNOWFLAKE_WAREHOUSE")]
    pub warehouse: String,

    /// Database to read from
    #[arg(long, env = "SNOWFLAKE_DATABASE")]
    pub database: String,

    /// Schema within the database
    #[arg(long, default_value = "PUBLIC", env = "SNOWFLAKE_SCHEMA")]
    pub schema: String,

    /// Role to assume for the session (optional)
    #[arg(long, env = "SNOWFLAKE_ROLE")]
    pub role: Option<String>,

    /// Tables to ingest (comma-separated, empty means all tables in the schema)
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Columns forming the SurrealDB record ID (comma-separated). When omitted, a
    /// sequential per-table index is generated.
    #[arg(long, value_delimiter = ',')]
    pub id_columns: Vec<String>,

    /// Target SurrealDB namespace
    #[arg(long)]
    pub to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    pub to_database: String,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    pub transforms_config: Option<PathBuf>,

    #[command(flatten)]
    pub surreal: SurrealOpts,
}
