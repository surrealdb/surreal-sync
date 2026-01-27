//! PostgreSQL full sync implementation for surreal-sync
//!
//! This module provides functionality to perform full database migration from PostgreSQL
//! to SurrealDB, including support for checkpoint emission for incremental sync coordination.

use crate::SourceOpts;
use anyhow::Result;
use checkpoint::{Checkpoint, SyncConfig, SyncManager, SyncPhase};
use chrono::Utc;
use surreal_sync_postgresql::SurrealOpts;
use surreal_sync_surreal::{surreal_connect, SurrealOpts as SurrealConnOpts};
use tokio_postgres::NoTls;
use tracing::info;

/// Main entry point for PostgreSQL to SurrealDB migration with checkpoint support
pub async fn run_full_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    sync_config: Option<SyncConfig>,
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
    if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

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

        sync_manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;

        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_cli_string()
        );
    }

    // Connect to SurrealDB
    let surreal_conn_opts = SurrealConnOpts {
        surreal_endpoint: to_opts.surreal_endpoint.clone(),
        surreal_username: to_opts.surreal_username.clone(),
        surreal_password: to_opts.surreal_password.clone(),
    };
    let surreal = surreal_connect(&surreal_conn_opts, &to_namespace, &to_database).await?;

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
            surreal_sync_postgresql::migrate_table(&client, &surreal, table_name, &to_opts).await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = SyncManager::new(config.clone(), None);

        // For trigger-based sync, checkpoint reflects the current state
        let checkpoint = super::checkpoint::PostgreSQLCheckpoint {
            sequence_id: 0, // Full sync complete, incremental will start from audit table
            timestamp: Utc::now(),
        };

        sync_manager
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
