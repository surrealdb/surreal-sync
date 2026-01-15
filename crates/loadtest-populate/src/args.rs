//! Common CLI argument definitions shared by all populators.

use clap::Args;
use std::path::PathBuf;

/// Common arguments shared by all populators.
///
/// This struct is used by all loadtest populate commands (MySQL, PostgreSQL,
/// MongoDB, CSV, JSONL, Kafka) to ensure consistent CLI interface.
#[derive(Args, Clone, Debug)]
pub struct CommonPopulateArgs {
    /// Path to schema YAML file
    #[arg(long, short = 's')]
    pub schema: PathBuf,

    /// Number of rows to generate per table
    #[arg(long, default_value = "1000")]
    pub row_count: u64,

    /// Batch size for database inserts
    #[arg(long, default_value = "100")]
    pub batch_size: usize,

    /// Random seed for deterministic generation (same seed = same data)
    #[arg(long, default_value = "42")]
    pub seed: u64,

    /// Specific tables to populate (comma-separated, empty = all tables from schema)
    #[arg(long, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Dry-run mode: validate schema and configuration without actual database operations
    #[arg(long)]
    pub dry_run: bool,

    /// Aggregator server URL for HTTP-based metrics collection (e.g., http://aggregator:9090)
    #[arg(long)]
    pub aggregator_url: Option<String>,
}
