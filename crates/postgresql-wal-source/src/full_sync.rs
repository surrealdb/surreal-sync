//! PostgreSQL WAL full sync implementation.

use std::sync::Arc;

use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::{get_user_tables, migrate_table, SyncOpts as PgSyncOpts};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::info;

use crate::catch_up::{
    emit_catch_up_progress, read_catch_up_progress, CatchUpProgress, CoverageKind,
};
use crate::checkpoint::WalCheckpoint;
use crate::client::{
    connect_wal_client, ensure_publication_for_source, get_current_wal_lsn, new_sql_client,
    resolve_schema,
};
use crate::schema::collect_postgresql_database_schema;
use crate::{SourceOpts, SyncOpts};

pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    run_full_sync_cancellable(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
}

/// [`run_full_sync`] with cooperative cancellation. On cancel, the snapshot
/// stops between tables and returns cleanly **without** emitting a
/// `FullSyncEnd` checkpoint: the `FullSyncStart` LSN (the streaming lower bound
/// captured before the dump) remains the safe resume point.
pub async fn run_full_sync_cancellable<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    info!("Starting PostgreSQL WAL full sync to SurrealDB");

    let sql = new_sql_client(&from_opts.connection_string).await?;
    let schema = resolve_schema(from_opts).await;
    {
        let client = sql.lock().await;
        ensure_publication_for_source(&client, from_opts, &schema).await?;
    }

    if let Some(manager) = sync_manager {
        let checkpoint = capture_wal_checkpoint(&sql).await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_cli_string()
        );
    }

    let db_schema = {
        let client = sql.lock().await;
        collect_postgresql_database_schema(&client).await?
    };

    let tables = {
        let client = sql.lock().await;
        resolve_user_tables(&client, &schema, from_opts).await?
    };
    info!("Found {} tables to migrate", tables.len());

    let pg_sync_opts = to_pg_sync_opts(sync_opts);
    let mut total_migrated = 0;
    for table_name in &tables {
        if cancel.is_cancelled() {
            info!(
                "Cancellation requested during full sync; stopping before table '{table_name}'. \
                 Resume point remains the FullSyncStart LSN."
            );
            return Ok(());
        }
        info!("Migrating table: {table_name}");
        let count = {
            let client = sql.lock().await;
            migrate_table(
                &client,
                surreal,
                table_name,
                &pg_sync_opts,
                Some(&db_schema),
                &[],
            )
            .await?
        };
        total_migrated += count;
        info!("Migrated {count} records from table {table_name}");
        if cancel.is_cancelled() {
            info!(
                "Cancellation requested after table '{table_name}'; stopping full sync. \
                 Resume point remains the FullSyncStart LSN."
            );
            return Ok(());
        }
    }

    if let Some(manager) = sync_manager {
        let checkpoint = capture_wal_checkpoint(&sql).await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;
        info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_cli_string()
        );

        let table_names = tables.clone();
        let existing = read_catch_up_progress(manager).await?;
        let mut progress = existing.unwrap_or_else(|| CatchUpProgress::new(checkpoint.clone()));
        progress.merge_tables(&table_names, CoverageKind::Initial, &checkpoint);
        emit_catch_up_progress(manager, &progress).await?;
    }

    info!("PostgreSQL WAL full sync completed: {total_migrated} total records migrated");
    Ok(())
}

fn to_pg_sync_opts(opts: &SyncOpts) -> PgSyncOpts {
    PgSyncOpts {
        batch_size: opts.batch_size,
        dry_run: opts.dry_run,
    }
}

async fn resolve_user_tables(
    client: &Client,
    schema: &str,
    from_opts: &SourceOpts,
) -> Result<Vec<String>> {
    if !from_opts.tables.is_empty() {
        return Ok(from_opts.tables.clone());
    }
    if schema == "public" {
        return get_user_tables(client, schema).await;
    }
    let rows = client
        .query(
            "SELECT tablename FROM pg_tables \
             WHERE schemaname = $1 AND tablename NOT LIKE 'surreal_sync_%' \
             ORDER BY tablename",
            &[&schema],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Resolve a "start at head" checkpoint for incremental sync: the server's
/// current WAL LSN. Resuming from it streams only changes committed after this
/// instant.
pub async fn capture_head_checkpoint(from_opts: &SourceOpts) -> Result<WalCheckpoint> {
    let sql = new_sql_client(&from_opts.connection_string).await?;
    let schema = resolve_schema(from_opts).await;
    {
        let client = sql.lock().await;
        ensure_publication_for_source(&client, from_opts, &schema).await?;
    }

    // Capture the WAL position *before* creating the slot so subsequent changes
    // (after this returns) are guaranteed to be at or after the checkpoint.
    let lsn = {
        let client = sql.lock().await;
        get_current_wal_lsn(&client).await?
    };

    let mut wal = connect_wal_client(from_opts).await?;
    wal.ensure_replication_slot().await?;
    drop(wal);

    Ok(WalCheckpoint {
        lsn,
        timestamp: chrono::Utc::now(),
    })
}

async fn capture_wal_checkpoint(sql: &Arc<Mutex<Client>>) -> Result<WalCheckpoint> {
    let client = sql.lock().await;
    let lsn = get_current_wal_lsn(&client).await?;
    Ok(WalCheckpoint {
        lsn,
        timestamp: chrono::Utc::now(),
    })
}

pub use surreal_sync_postgresql::read_table_chunk;

#[allow(dead_code)]
pub async fn load_schema(sql: &Arc<Mutex<Client>>) -> Result<sync_core::DatabaseSchema> {
    let client = sql.lock().await;
    collect_postgresql_database_schema(&client).await
}
