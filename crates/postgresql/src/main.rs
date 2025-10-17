//! Demo program for PostgreSQL logical replication
//!
//! This program demonstrates how to use the logical replication client
//! to create a replication slot and stream changes to stdout.

use anyhow::Result;
use std::env;
use surreal_sync_postgresql_replication::{Client, Slot};
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Main entry point for the demo program
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "postgresql_replication=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("PostgreSQL Logical Replication Demo");
    info!("====================================");

    // Get connection string from environment or use default
    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "host=localhost user=postgres password=postgres dbname=postgres".to_string()
    });

    info!("Connecting to PostgreSQL...");
    info!("Connection string: {}", connection_string);

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {e}"))?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {e}");
        }
    });

    info!("Successfully connected to PostgreSQL");

    // Create the replication client
    // You can specify table names to filter, or leave empty to receive all changes
    let table_names = env::var("TABLES")
        .map(|tables| tables.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_else(|_| Vec::new());

    if table_names.is_empty() {
        info!("Tracking changes for all tables");
    } else {
        info!("Tracking changes for tables: {:?}", table_names);
    }

    let repl_client = Client::new(client, table_names);

    // Get slot name from environment
    let slot_name = env::var("SLOT_NAME").unwrap_or_else(|_| "demo_slot".to_string());

    // Create the replication slot if it doesn't exist
    info!("Creating replication slot: {}", slot_name);
    repl_client
        .create_slot(&slot_name)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create replication slot: {e}"))?;

    // Start replication with the slot
    info!("Starting replication with slot: {}", slot_name);
    let slot = repl_client
        .start_replication(Some(&slot_name))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start replication: {e}"))?;

    info!("Replication started successfully!");
    info!("Waiting for changes... (Press Ctrl+C to stop)");
    info!("");

    // Setup signal handler for graceful shutdown
    let shutdown = setup_shutdown_handler();

    // Stream changes to stdout
    stream_changes(&slot, shutdown).await?;

    // Cleanup: drop the replication slot
    info!("");
    info!("Shutting down...");

    match repl_client.drop_slot(&slot_name).await {
        Ok(_) => info!("Replication slot dropped successfully"),
        Err(e) => warn!("Failed to drop replication slot: {}", e),
    }

    info!("Demo completed");
    Ok(())
}

/// Streams changes to stdout until shutdown signal is received
async fn stream_changes(
    slot: &Slot,
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
                    Ok(changes) => {
                        if changes.is_empty() {
                            debug!("No new changes available");
                            continue;
                        }

                        let mut last_lsn = String::new();

                        // Process all changes in the batch
                        for (lsn, change) in &changes {
                            // Pretty print the JSON to stdout
                            let pretty = serde_json::to_string_pretty(&change)?;
                            println!("=== Change Received ===");
                            println!("LSN: {lsn}");
                            println!("{pretty}");
                            println!("======================\n");

                            // Track the last LSN for batch advancement
                            last_lsn = lsn.clone();
                        }

                        // Advance to the last LSN after successfully processing ALL changes
                        // This batches the acknowledgment for better performance
                        // In a real application, you would advance only after
                        // successfully writing all changes to your target database
                        if !last_lsn.is_empty() {
                            match slot.advance(&last_lsn).await {
                                Ok(_) => {
                                    debug!("Batch advanced slot to LSN: {} ({} changes processed)", last_lsn, changes.len());
                                }
                                Err(e) => {
                                    error!("Failed to advance slot to {}: {}. Changes will be redelivered.", last_lsn, e);
                                    // The changes will be redelivered on next iteration
                                    // This ensures at-least-once delivery
                                }
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

/// Instructions for testing the demo
#[allow(dead_code)]
fn print_test_instructions() {
    println!(
        r#"
TESTING INSTRUCTIONS
====================

1. Start PostgreSQL with wal2json plugin installed
2. Set environment variables:
   - DATABASE_URL: PostgreSQL connection string
   - SLOT_NAME: Name for the replication slot (optional, default: demo_slot)
   - TABLES: Comma-separated list of tables to track (optional, default: all)

3. Run the demo:
   cargo run --bin postgresql-replication-demo

4. In another terminal, make some database changes:
   psql -U postgres -d postgres

   CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT, value INTEGER);
   INSERT INTO test_table (name, value) VALUES ('test1', 100);
   UPDATE test_table SET value = 200 WHERE name = 'test1';
   DELETE FROM test_table WHERE name = 'test1';

5. Watch the changes appear in the demo output!

Note: Make sure wal2json is installed and PostgreSQL is configured for logical replication:
- wal_level = logical
- max_replication_slots = 10 (or higher)
- shared_preload_libraries = 'wal2json' (if needed)
"#
    );
}
