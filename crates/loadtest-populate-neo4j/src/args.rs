//! CLI argument definitions for Neo4j populator.

use clap::Args;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// Neo4j-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct Neo4jPopulateArgs {
    /// Neo4j connection string (e.g., bolt://localhost:7687)
    #[arg(long, env = "NEO4J_CONNECTION_STRING")]
    pub neo4j_connection_string: String,

    /// Neo4j username
    #[arg(long, env = "NEO4J_USERNAME", default_value = "neo4j")]
    pub neo4j_username: String,

    /// Neo4j password
    #[arg(long, env = "NEO4J_PASSWORD")]
    pub neo4j_password: String,

    /// Neo4j database name
    #[arg(long, env = "NEO4J_DATABASE", default_value = "neo4j")]
    pub neo4j_database: String,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
