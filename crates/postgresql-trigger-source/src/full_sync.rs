//! PostgreSQL full sync implementation for surreal-sync
//!
//! This module provides functionality to perform full database migration from PostgreSQL
//! to SurrealDB, including support for checkpoint emission for incremental sync coordination.

use crate::SourceOpts;
use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::Utc;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::SyncOpts;
use tokio_postgres::NoTls;
use tracing::info;

/// Main entry point for PostgreSQL to SurrealDB migration with checkpoint support
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `from_opts` - Source database options
/// * `sync_opts` - Sync configuration options
/// * `sync_manager` - Optional sync manager for checkpoint emission
pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    info!("Starting PostgreSQL migration to SurrealDB");

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&from_opts.source_uri, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Emit checkpoint t1 (before full sync starts) if configured
    if let Some(manager) = sync_manager {
        // Set up triggers on user tables so changes during full sync are captured
        let tables = surreal_sync_postgresql::get_user_tables(
            &client,
            from_opts.source_database.as_deref().unwrap_or("public"),
        )
        .await?;
        let incremental_client =
            surreal_sync_postgresql::new_postgresql_client(&from_opts.source_uri).await?;
        let mut incremental_source =
            super::incremental_sync::PostgresIncrementalSource::new(incremental_client, 0);
        incremental_source.setup_tracking(tables).await?;

        let current_sequence = incremental_source.get_current_sequence().await?;

        let checkpoint = super::checkpoint::PostgreSQLCheckpoint {
            sequence_id: current_sequence,
            timestamp: Utc::now(),
        };

        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;

        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_cli_string()
        );
    }

    // Get database name from connection string or options
    let database_name = from_opts
        .source_database
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL database name is required"))?;

    // Get list of tables to migrate (excluding system tables)
    let tables = surreal_sync_postgresql::get_user_tables(&client, database_name).await?;

    info!("Found {} tables to migrate", tables.len());

    let mut total_migrated = 0;

    // Migrate each table
    for table_name in &tables {
        info!("Migrating table: {}", table_name);

        let count =
            surreal_sync_postgresql::migrate_table(&client, surreal, table_name, &sync_opts)
                .await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(manager) = sync_manager {
        // For trigger-based sync, checkpoint reflects the current state
        let checkpoint = super::checkpoint::PostgreSQLCheckpoint {
            sequence_id: 0, // Full sync complete, incremental will start from audit table
            timestamp: Utc::now(),
        };

        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;

        info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_cli_string()
        );
    }

    info!(
        "PostgreSQL migration completed: {} total records migrated",
        total_migrated
    );
    Ok(())
}
