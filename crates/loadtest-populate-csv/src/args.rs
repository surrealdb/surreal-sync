//! CLI argument definitions for CSV populator.

use clap::Args;
use std::path::PathBuf;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// CSV-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct CSVPopulateArgs {
    /// Output directory for CSV files (one file per table)
    #[arg(long, short = 'o')]
    pub output_dir: PathBuf,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
