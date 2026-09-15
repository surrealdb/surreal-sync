//! Table discovery for a BigQuery dataset.

use anyhow::Result;

use super::client::BigQueryClient;

/// List base-table names in the client's `{project}.{dataset}`.
///
/// Uses the `datasets.tables.list` REST endpoint rather than
/// `INFORMATION_SCHEMA.TABLES`: it costs no query slots and no bytes billed, it
/// reports each entry's `type` so views and snapshots can be filtered without SQL,
/// and — unlike `INFORMATION_SCHEMA` — it is supported by the BigQuery emulator the
/// integration tests run against.
pub async fn list_tables(client: &BigQueryClient) -> Result<Vec<String>> {
    client.list_dataset_tables().await
}
