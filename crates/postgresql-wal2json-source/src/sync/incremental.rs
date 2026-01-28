//! Incremental sync phase implementation for PostgreSQL logical decoding
//!
//! This module handles the incremental logical replication phase after
//! the initial full snapshot has been completed.

use anyhow::Result;
use surreal2_sink::apply_universal_change;
use sync_core::{UniversalChange, UniversalChangeOp};
use tokio_postgres::NoTls;
use tracing::{debug, error, info};

use super::config_and_sync::Config;

/// Perform incremental sync using logical replication
///
/// This function starts logical replication from the position
/// saved during the initial sync phase.
///
/// # Arguments
/// * `surreal` - SurrealDB client
/// * `config` - Configuration for the sync operation
///
/// # Returns
/// Returns Ok(()) on successful completion, or an error if the sync fails
pub async fn sync(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    config: &Config,
) -> Result<()> {
    info!(
        "Starting incremental sync from replication slot: {}",
        config.slot
    );

    // Connect to PostgreSQL
    let (psql_client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {e}"))?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {e}");
        }
    });

    let client = crate::Client::new(psql_client, config.tables.clone());

    // Start replication with the existing slot
    info!("Starting replication with slot: {}", config.slot);
    let slot = client.start_replication(Some(&config.slot)).await?;

    info!("Replication started successfully!");
    info!("Waiting for changes... (Press Ctrl+C to stop)");

    // Setup signal handler for graceful shutdown
    let shutdown = setup_shutdown_handler();

    // Stream changes and debug print them
    stream_changes(surreal, &slot, shutdown).await?;

    info!("Incremental sync completed");
    Ok(())
}

/// Streams changes from the replication slot and debug prints them
async fn stream_changes(
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    slot: &crate::Slot,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                info!("Received shutdown signal");
                break;
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                // Peek at available changes
                match slot.peek().await {
                    Ok((changes, nextlsn)) => {
                        if changes.is_empty() {
                            debug!("No new changes available");
                            continue;
                        }

                        // Process all changes in the batch
                        for change in &changes {
                            // Debug print the change
                            debug!("=== Change Received ===");
                            debug!("{change:#?}");
                            debug!("======================\n");

                            // Log the change type for analysis
                            match change {
                                crate::Action::Insert(row) => {
                                    debug!("INSERT: schema={}, table={}, primary_key={:?}, columns={:?}",
                                        row.schema, row.table, row.primary_key, row.columns);

                                    let universal_change = row_to_universal_change(row, UniversalChangeOp::Create);
                                    apply_universal_change(surreal, &universal_change).await?;
                                }
                                crate::Action::Update(row) => {
                                    debug!("UPDATE: schema={}, table={}, primary_key={:?}, columns={:?}",
                                        row.schema, row.table, row.primary_key, row.columns);

                                    let universal_change = row_to_universal_change(row, UniversalChangeOp::Update);
                                    apply_universal_change(surreal, &universal_change).await?;
                                }
                                crate::Action::Delete(row) => {
                                    debug!("DELETE: schema={}, table={}, primary_key={:?}",
                                        row.schema, row.table, row.primary_key);

                                    let universal_change = row_to_universal_change(row, UniversalChangeOp::Delete);
                                    apply_universal_change(surreal, &universal_change).await?;
                                }
                                crate::Action::Begin { xid, timestamp } => {
                                    debug!("BEGIN: xid={}, timestamp={:?}", xid, timestamp);
                                }
                                crate::Action::Commit { xid, nextlsn, timestamp } => {
                                    debug!("COMMIT: xid={}, nextlsn={}, timestamp={:?}", xid, nextlsn, timestamp);
                                }
                            }
                        }

                        // Advance to nextlsn after successfully processing all changes
                        debug!("Batch processed {} changes, advancing to nextlsn: {}", changes.len(), nextlsn);

                        match slot.advance(&nextlsn).await {
                            Ok(_) => {
                                debug!("Successfully advanced slot to {}", nextlsn);
                            }
                            Err(e) => {
                                error!("Failed to advance slot to {}: {}. Changes will be redelivered.", nextlsn, e);
                                // The changes will be redelivered on next iteration
                                // This ensures at-least-once delivery
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error peeking changes from slot: {}", e);
                        // Wait longer before retrying on error
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Sets up a shutdown signal handler
fn setup_shutdown_handler() -> tokio::sync::broadcast::Receiver<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");

        info!("\nReceived interrupt signal (Ctrl+C)");
        let _ = shutdown_tx.send(());
    });

    shutdown_rx
}

/// Convert a Row to UniversalChange
fn row_to_universal_change(row: &crate::Row, op: UniversalChangeOp) -> UniversalChange {
    let data = if op == UniversalChangeOp::Delete {
        None
    } else {
        Some(row.columns.clone())
    };
    UniversalChange::new(op, row.table.clone(), row.primary_key.clone(), data)
}
