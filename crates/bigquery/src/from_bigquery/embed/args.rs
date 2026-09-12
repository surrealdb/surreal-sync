//! Command-line flags for BigQuery ingestion.
//!
//! Embedders parse these via [`super::run`] (same flags as `surreal-sync from bigquery`,
//! without the `from bigquery` prefix). The stock binary nests the same type
//! under `from`.

use clap::Args as ClapArgs;
use std::path::PathBuf;

use surreal_sync_runtime::SurrealCliOpts as SurrealOpts;

use crate::from_bigquery::{DEFAULT_API_ENDPOINT, DEFAULT_PAGE_SIZE};

/// Flags for a one-shot BigQuery → SurrealDB import.
///
/// Match `surreal-sync from bigquery …`. Env vars (`BIGQUERY_*`, `GOOGLE_*`,
/// `SURREAL_*`) work the same way.
#[derive(ClapArgs, Clone)]
pub struct Args {
    /// Google Cloud project that owns the dataset
    #[arg(long, env = "BIGQUERY_PROJECT_ID")]
    pub project_id: String,

    /// BigQuery dataset to read from
    #[arg(long, env = "BIGQUERY_DATASET")]
    pub dataset: String,

    /// Project billed for the query jobs (defaults to --project-id)
    #[arg(long, env = "BIGQUERY_BILLING_PROJECT_ID")]
    pub billing_project_id: Option<String>,

    /// Path to a service-account JSON key file
    #[arg(long, value_name = "PATH", env = "GOOGLE_APPLICATION_CREDENTIALS")]
    pub credentials_path: Option<PathBuf>,

    /// Dataset location (e.g. "US", "EU", "europe-west2"). Inferred when omitted.
    #[arg(long, env = "BIGQUERY_LOCATION")]
    pub location: Option<String>,

    /// BigQuery API root. Override to target a local emulator.
    #[arg(long, default_value = DEFAULT_API_ENDPOINT, env = "BIGQUERY_API_ENDPOINT")]
    pub api_endpoint: String,

    /// Tables to ingest (comma-separated, empty means all tables in the dataset)
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Columns forming the SurrealDB record ID (comma-separated). When omitted, a
    /// sequential per-table index is generated.
    #[arg(long, value_delimiter = ',')]
    pub id_columns: Vec<String>,

    /// Rows fetched per BigQuery result page
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    pub page_size: usize,

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
