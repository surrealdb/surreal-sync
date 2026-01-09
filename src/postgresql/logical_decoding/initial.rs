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

    // Convert main crate SurrealOpts to sub-crate SurrealOpts
    let sub_crate_opts = surreal_sync_postgresql_trigger::SurrealOpts {
        surreal_endpoint: config.to_opts.surreal_endpoint.clone(),
        surreal_username: config.to_opts.surreal_username.clone(),
        surreal_password: config.to_opts.surreal_password.clone(),
        batch_size: config.to_opts.batch_size,
        dry_run: config.to_opts.dry_run,
    };

    for tb in &config.tables {
        surreal_sync_postgresql_trigger::migrate_table(&psql_client, surreal, tb, &sub_crate_opts)
            .await?;
    }

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {e}");
        }
    });
    let client = surreal_sync_postgresql_logical_replication::Client::new(
        psql_client,
        config.tables.clone(),
    );
    let pre_lsn = client.get_current_wal_lsn().await?;

    Ok(pre_lsn)
}
