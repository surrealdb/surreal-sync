//! CLI argument definitions for MySQL populator.

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

/// MySQL-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct MySQLPopulateArgs {
    /// MySQL connection string (e.g., mysql://user:pass@host:3306/database)
    #[arg(long, env = "MYSQL_CONNECTION_STRING")]
    pub mysql_connection_string: String,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
