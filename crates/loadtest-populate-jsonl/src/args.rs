//! CLI argument definitions for JSONL populator.

use clap::Args;
use std::path::PathBuf;

/// Common arguments shared by all populators.
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
}

/// JSONL-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct JSONLPopulateArgs {
    /// Output directory for JSONL files (one file per table)
    #[arg(long, short = 'o')]
    pub output_dir: PathBuf,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
