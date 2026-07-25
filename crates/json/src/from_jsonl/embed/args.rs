//! Command-line flags for JSONL import.
//!
//! Embedders parse these via [`super::run`] (same flags as `surreal-sync from jsonl`,
//! without the `from jsonl` prefix). The stock binary nests the same type under `from`.

use clap::Args as ClapArgs;
use std::path::PathBuf;

use surreal_sync_runtime::SurrealCliOpts as SurrealOpts;

/// Flags for a one-shot JSONL → SurrealDB import.
///
/// Match `surreal-sync from jsonl …`.
#[derive(ClapArgs, Clone)]
pub struct Args {
    /// Path to a JSONL directory or a single `.jsonl` file
    #[arg(long)]
    pub path: String,

    /// Target SurrealDB namespace
    #[arg(long)]
    pub to_namespace: String,

    /// Target SurrealDB database
    #[arg(long)]
    pub to_database: String,

    /// ID field name (default: "id")
    #[arg(long, default_value = "id")]
    pub id_field: String,

    /// Columns forming the SurrealDB record ID (comma-separated). When two or
    /// more are set, the ID is a Surreal array key. Takes precedence over `--id-field`.
    #[arg(long, value_delimiter = ',')]
    pub id_columns: Vec<String>,

    /// Conversion rules (format: `type="page_id",page_id page:page_id`)
    #[arg(long = "rule", value_name = "RULE")]
    pub conversion_rules: Vec<String>,

    /// Schema file for type-aware conversion
    #[arg(long, value_name = "PATH")]
    pub schema_file: Option<PathBuf>,

    /// TOML file describing the transform pipeline (`[[transforms]]`).
    /// Omit for identity (docs pass through unchanged; no transform stage dispatch).
    #[arg(long, value_name = "PATH")]
    pub transforms_config: Option<PathBuf>,

    #[command(flatten)]
    pub surreal: SurrealOpts,
}
