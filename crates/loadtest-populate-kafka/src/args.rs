//! CLI argument definitions for Kafka populator.

use clap::Args;

// Re-export CommonPopulateArgs for convenience
pub use loadtest_populate::CommonPopulateArgs;

/// Kafka-specific populate arguments.
#[derive(Args, Clone, Debug)]
pub struct KafkaPopulateArgs {
    /// Kafka brokers (comma-separated, e.g., "localhost:9092")
    #[arg(long, env = "KAFKA_BROKERS", default_value = "localhost:9092")]
    pub kafka_brokers: String,

    #[command(flatten)]
    pub common: CommonPopulateArgs,
}
