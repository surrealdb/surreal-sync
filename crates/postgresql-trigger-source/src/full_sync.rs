//! PostgreSQL full sync implementation for surreal-sync
//!
//! This module provides functionality to perform full database migration from PostgreSQL
//! to SurrealDB, including support for checkpoint emission for incremental sync coordination.

use crate::SourceOpts;
use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::Utc;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::{convert_table, SyncOpts};
use sync_transform::{write_relations, write_rows, ApplyOpts, Pipeline};
use tokio_postgres::NoTls;
use tracing::{debug, info};

/// Main entry point for PostgreSQL to SurrealDB migration with checkpoint support (identity).
pub async fn run_full_sync<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_with_transforms(surreal, from_opts, sync_opts, sync_manager, &pipeline, &apply_opts)
        .await
}

/// Full sync with an explicit transform pipeline.
pub async fn run_full_sync_with_transforms<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Starting PostgreSQL migration to SurrealDB");
    if pipeline.is_identity() {
        debug!("Full sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            "Full sync using transform pipeline"
        );
    }

    let (client, connection) = tokio_postgres::connect(&from_opts.source_uri, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    if let Some(manager) = sync_manager {
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

    let database_name = from_opts
        .source_database
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL database name is required"))?;

    let tables = surreal_sync_postgresql::get_user_tables(&client, database_name).await?;
    info!("Found {} tables to migrate", tables.len());

    let db_schema =
        surreal_sync_postgresql::schema::collect_database_schema_with_fks(&client).await?;

    let mut total_migrated = 0;
    let batch_size = sync_opts.batch_size.max(1);

    for table_name in &tables {
        info!("Migrating table: {}", table_name);
        let (rows, relations) = convert_table(
            &client,
            table_name,
            Some(&db_schema),
            &from_opts.relation_tables,
        )
        .await?;

        let mut count = 0usize;
        for chunk in rows.chunks(batch_size) {
            if !sync_opts.dry_run {
                write_rows(surreal, pipeline, chunk.to_vec(), apply_opts).await?;
            }
            count += chunk.len();
        }
        for chunk in relations.chunks(batch_size) {
            if !sync_opts.dry_run {
                write_relations(surreal, pipeline, chunk.to_vec(), apply_opts).await?;
            }
            count += chunk.len();
        }

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    if let Some(manager) = sync_manager {
        let incremental_client =
            surreal_sync_postgresql::new_postgresql_client(&from_opts.source_uri).await?;
        let incremental_source =
            super::incremental_sync::PostgresIncrementalSource::new(incremental_client, 0);
        let current_sequence = incremental_source.get_current_sequence().await?;
        let checkpoint = super::checkpoint::PostgreSQLCheckpoint {
            sequence_id: current_sequence,
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
