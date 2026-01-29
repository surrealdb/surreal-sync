//! PostgreSQL logical replication full sync implementation
//!
//! This module provides functionality to perform full database migration from PostgreSQL
//! to SurrealDB using logical replication infrastructure, with checkpoint emission support
//! for coordinated incremental sync.

use anyhow::Result;
use checkpoint::{CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::SyncOpts;
use tokio_postgres::NoTls;
use tracing::info;

/// Options for the PostgreSQL logical replication source
#[derive(Clone, Debug)]
pub struct SourceOpts {
    /// PostgreSQL connection string
    pub connection_string: String,
    /// Replication slot name
    pub slot_name: String,
    /// Tables to sync (empty means all user tables)
    pub tables: Vec<String>,
    /// PostgreSQL schema (default: public)
    pub schema: String,
}

/// Run full sync from PostgreSQL to SurrealDB with checkpoint support
///
/// This function:
/// 1. Connects to PostgreSQL
/// 2. Creates/ensures the replication slot exists
/// 3. Captures current WAL LSN position (for t1 checkpoint)
/// 4. Migrates all tables to SurrealDB
/// 5. Captures final WAL LSN position (for t2 checkpoint)
///
/// # Arguments
/// * `surreal` - SurrealDB sink for writing data
/// * `from_opts` - PostgreSQL source options
/// * `sync_opts` - Sync options (batch_size, dry_run)
/// * `sync_manager` - Optional sync manager for checkpoint emission
pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    info!("Starting PostgreSQL logical replication full sync to SurrealDB");

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Create logical replication client and ensure slot exists
    let pg_client = crate::Client::new(client, from_opts.tables.clone());
    pg_client.create_slot(&from_opts.slot_name).await?;

    // Emit checkpoint t1 (before full sync starts) if configured
    if let Some(manager) = sync_manager {
        // Get current WAL LSN position - this is where incremental sync will start
        let checkpoint = pg_client.get_current_wal_lsn_checkpoint().await?;

        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;

        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.lsn
        );
    }

    // Get list of tables to migrate
    let tables = if from_opts.tables.is_empty() {
        // Get all user tables from the specified schema
        surreal_sync_postgresql::get_user_tables(pg_client.pg_client(), &from_opts.schema).await?
    } else {
        from_opts.tables.clone()
    };

    info!("Found {} tables to migrate", tables.len());

    let mut total_migrated = 0;

    // Migrate each table
    for table_name in &tables {
        info!("Migrating table: {}", table_name);

        let count = surreal_sync_postgresql::migrate_table(
            pg_client.pg_client(),
            surreal,
            table_name,
            &sync_opts,
        )
        .await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(manager) = sync_manager {
        // Get final WAL LSN position
        let checkpoint = pg_client.get_current_wal_lsn_checkpoint().await?;

        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;

        info!("Emitted full sync end checkpoint (t2): {}", checkpoint.lsn);
    }

    info!(
        "PostgreSQL logical replication full sync completed: {} total records migrated",
        total_migrated
    );
    Ok(())
}
