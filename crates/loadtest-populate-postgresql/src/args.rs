//! CLI argument definitions for PostgreSQL populator.

use clap::Args;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// PostgreSQL-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct PostgreSQLPopulateArgs {
    /// PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/database)
    #[arg(long, env = "POSTGRESQL_CONNECTION_STRING")]
    pub postgresql_connection_string: String,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
