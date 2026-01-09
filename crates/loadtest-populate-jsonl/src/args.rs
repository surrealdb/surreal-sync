//! CLI argument definitions for JSONL populator.

use clap::Args;
use std::path::PathBuf;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// JSONL-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct JSONLPopulateArgs {
    /// Output directory for JSONL files (one file per table)
    #[arg(long, short = 'o')]
    pub output_dir: PathBuf,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
