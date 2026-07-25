//! Command-line flags for Kafka → SurrealDB sync.
//!
//! Embedders parse these via [`super::run`] (same flags as `surreal-sync from kafka`,
//! without the `from kafka` prefix). The stock binary nests the same type under `from`.

use clap::Args as ClapArgs;
use std::path::PathBuf;

use surreal_sync_runtime::SurrealCliOpts as SurrealOpts;

use crate::from_kafka::Config;

/// Flags for Kafka topic sync into SurrealDB.
///
/// Match `surreal-sync from kafka …`. Progress uses Kafka consumer-group offsets
/// (not Surreal checkpoint flags).
#[derive(ClapArgs, Clone)]
pub struct Args {
    /// Kafka source configuration
    #[command(flatten)]
    pub config: Config,

    /// Target SurrealDB namespace
    #[arg(long)]
    pub to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    pub to_database: String,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    pub schema_file: Option<PathBuf>,

    /// Timeout for consuming messages (e.g. "1h", "30m", "300s").
    /// After this time, the consumer stops and exits.
    #[arg(long, default_value = "1h")]
    pub timeout: String,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    pub transforms_config: Option<PathBuf>,

    #[command(flatten)]
    pub surreal: SurrealOpts,
}
