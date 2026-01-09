//! CLI argument definitions for MySQL populator.

use clap::Args;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// MySQL-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct MySQLPopulateArgs {
    /// MySQL connection string (e.g., mysql://user:pass@host:3306/database)
    #[arg(long, env = "MYSQL_CONNECTION_STRING")]
    pub mysql_connection_string: String,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
