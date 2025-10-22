//! Initial sync phase implementation for PostgreSQL logical decoding
//!
//! This module handles the initial full snapshot synchronization phase
//! before starting logical replication.

use anyhow::Result;
use tokio_postgres::NoTls;
use tracing::error;

/// Perform initial sync and return the pre-LSN
///
/// This function:
/// 1. Connects to PostgreSQL
/// 2. Spawns a connection handler
/// 3. Creates a replication client
/// 4. Gets the current WAL LSN position
///
/// # Arguments
/// * `config` - Configuration for the sync operation
///
/// # Returns
/// The pre-LSN string that marks the position before the initial sync
pub async fn sync(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &super::Config,
) -> Result<String> {
    // Connect to PostgreSQL
    let (psql_client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {e}"))?;

    for tb in &config.tables {
        crate::postgresql::migrate_table(&psql_client, surreal, tb, &config.to_opts).await?;
    }

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {e}");
        }
    });
    let client =
        surreal_sync_postgresql_replication::Client::new(psql_client, config.tables.clone());
    let pre_lsn = client.get_current_wal_lsn().await?;

    Ok(pre_lsn)
}
