//! Aggregate server command handler.

use crate::AggregateServerArgs;

/// Run the aggregate server command.
pub async fn run_aggregate_server(args: AggregateServerArgs) -> anyhow::Result<()> {
    loadtest_distributed::run_aggregator_server(args).await
}
