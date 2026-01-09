//! CLI argument definitions for MongoDB populator.

use clap::Args;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// MongoDB-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct MongoDBPopulateArgs {
    /// MongoDB connection string (e.g., mongodb://user:pass@host:27017)
    #[arg(long, env = "MONGODB_CONNECTION_STRING")]
    pub mongodb_connection_string: String,

    /// MongoDB database name
    #[arg(long, env = "MONGODB_DATABASE")]
    pub mongodb_database: String,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
