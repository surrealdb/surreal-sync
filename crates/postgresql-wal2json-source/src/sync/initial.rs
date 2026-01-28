//! Initial sync phase implementation for PostgreSQL logical decoding
//!
//! This module handles the initial full snapshot synchronization phase
//! before starting logical replication.

use anyhow::Result;
use surreal2_sink::Surreal2Sink;
use surreal_sync_postgresql::SyncOpts;
use tokio_postgres::NoTls;
use tracing::error;

use super::config_and_sync::Config;

/// Perform initial sync and return the pre-LSN
///
/// This function:
/// 1. Connects to PostgreSQL
/// 2. Spawns a connection handler
/// 3. Creates a replication client
/// 4. Gets the current WAL LSN position
///
/// # Arguments
/// * `surreal` - SurrealDB client
/// * `config` - Configuration for the sync operation
/// * `sync_opts` - Sync options (batch_size, dry_run)
///
/// # Returns
/// The pre-LSN string that marks the position before the initial sync
pub async fn sync(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
    sync_opts: &SyncOpts,
) -> Result<String> {
    // Connect to PostgreSQL
    let (psql_client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {e}"))?;

    // Wrap the v2 SDK connection in Surreal2Sink for use with migrate_table
    let sink = Surreal2Sink::new(surreal.clone());

    for tb in &config.tables {
        surreal_sync_postgresql::migrate_table(&psql_client, &sink, tb, sync_opts).await?;
    }

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {e}");
        }
    });
    let client = crate::Client::new(psql_client, config.tables.clone());
    let pre_lsn = client.get_current_wal_lsn().await?;

    Ok(pre_lsn)
}
