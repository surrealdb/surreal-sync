//! PostgreSQL WAL full sync implementation.

use std::sync::Arc;

use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::get_user_tables;
use sync_transform::{ApplyOpts, Pipeline};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::{debug, info};

use crate::catch_up::{
    emit_catch_up_progress, read_catch_up_progress, CatchUpProgress, CoverageKind,
};
use crate::checkpoint::PgoutputCheckpoint;
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
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_cancellable_with_transforms(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        &tokio_util::sync::CancellationToken::new(),
        &pipeline,
        &apply_opts,
    )
    .await
}

/// [`run_full_sync`] with cooperative cancellation. On cancel, the snapshot
/// stops between tables and returns cleanly **without** emitting a
/// `FullSyncEnd` checkpoint: the `FullSyncStart` LSN (the streaming lower bound
/// captured before the dump) remains the safe resume point.
///
/// Identity-pipeline overload (no transforms).
pub async fn run_full_sync_cancellable<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_full_sync_cancellable_with_transforms(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        cancel,
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Full sync with an explicit transform pipeline.
///
/// Rows are converted with existing PostgreSQL FK helpers first, then applied
/// via [`write_rows`] / [`write_relations`] (preserves full-sync FK enrichment;
/// does not invent incremental FK).
pub async fn run_full_sync_cancellable_with_transforms<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Starting PostgreSQL WAL full sync to SurrealDB");
    if pipeline.is_identity() {
        debug!("Full sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            "Full sync using transform pipeline"
        );
    }

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
            migrate_table_with_transforms(
                &client,
                surreal,
                table_name,
                sync_opts,
                Some(&db_schema),
                cancel,
                pipeline,
                apply_opts,
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

#[allow(clippy::too_many_arguments)]
async fn migrate_table_with_transforms<S: SurrealSink>(
    client: &Client,
    surreal: &S,
    table_name: &str,
    sync_opts: &SyncOpts,
    schema: Option<&sync_core::DatabaseSchema>,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    use surreal_sync_postgresql::{get_primary_key_columns, read_table_chunk};
    use sync_core::{classify_table, TableKind, UniversalValue};

    let pk_columns = get_primary_key_columns(client, table_name).await?;
    let table_kind = schema
        .and_then(|s| s.get_table(table_name))
        .map(|td| classify_table(td, &[]));
    let batch_size = sync_opts.batch_size.max(1);

    // Relation tables: keyset when PK exists, else OFFSET streaming.
    if let Some(TableKind::Relation { in_fk, out_fk }) = table_kind.clone() {
        return migrate_relation_streaming(
            client,
            surreal,
            table_name,
            &pk_columns,
            batch_size,
            sync_opts.dry_run,
            cancel,
            pipeline,
            apply_opts,
            in_fk,
            out_fk,
        )
        .await;
    }

    // No PK: OFFSET/LIMIT chunks through RowChunkDriver (avoid monolithic SELECT *).
    if pk_columns.is_empty() {
        tracing::warn!(
            "Table '{table_name}' has no primary key; streaming via OFFSET/LIMIT chunks"
        );
        return migrate_offset_rows_streaming(
            client,
            surreal,
            table_name,
            batch_size,
            schema,
            sync_opts.dry_run,
            cancel,
            pipeline,
            apply_opts,
        )
        .await;
    }

    let mut total_processed = 0usize;
    let mut after: Option<Vec<UniversalValue>> = None;
    if sync_opts.dry_run {
        loop {
            if cancel.is_cancelled() {
                return Ok(total_processed);
            }
            let chunk = read_table_chunk(
                client,
                table_name,
                &pk_columns,
                after.as_deref(),
                batch_size,
                schema,
            )
            .await?;
            if chunk.rows.is_empty() {
                break;
            }
            let n = chunk.rows.len();
            total_processed += n;
            after = chunk.last_pk;
            if n < batch_size {
                break;
            }
        }
        return Ok(total_processed);
    }

    use async_trait::async_trait;
    use std::sync::Arc;
    use sync_core::UniversalRow;
    use sync_transform::{
        run_source_runtime_with, RowChunkDriver, RowChunkSource, SourceRuntimeOpts,
    };

    struct PgKeysetChunks<'a> {
        client: &'a Client,
        table_name: &'a str,
        pk_columns: &'a [String],
        after: Option<Vec<UniversalValue>>,
        batch_size: usize,
        schema: Option<&'a sync_core::DatabaseSchema>,
        cancel: &'a tokio_util::sync::CancellationToken,
        exhausted: bool,
    }

    #[async_trait]
    impl RowChunkSource for PgKeysetChunks<'_> {
        async fn next_chunk(&mut self) -> Result<Option<Vec<UniversalRow>>> {
            if self.exhausted || self.cancel.is_cancelled() {
                self.exhausted = true;
                return Ok(None);
            }
            let chunk = read_table_chunk(
                self.client,
                self.table_name,
                self.pk_columns,
                self.after.as_deref(),
                self.batch_size,
                self.schema,
            )
            .await?;
            if chunk.rows.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }
            let n = chunk.rows.len();
            self.after = chunk.last_pk;
            if n < self.batch_size {
                self.exhausted = true;
            }
            Ok(Some(chunk.rows))
        }
    }

    let chunks = PgKeysetChunks {
        client,
        table_name,
        pk_columns: &pk_columns,
        after: None,
        batch_size,
        schema,
        cancel,
        exhausted: false,
    };
    let mut driver = RowChunkDriver::new(chunks);
    let transformer = Arc::new(pipeline.clone());
    let runtime_opts = SourceRuntimeOpts::new();
    run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;
    Ok(driver.sunk_count() as usize)
}

#[allow(clippy::too_many_arguments)]
async fn migrate_relation_streaming<S: SurrealSink>(
    client: &Client,
    surreal: &S,
    table_name: &str,
    pk_columns: &[String],
    batch_size: usize,
    dry_run: bool,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
    in_fk: sync_core::ForeignKeyDefinition,
    out_fk: sync_core::ForeignKeyDefinition,
) -> Result<usize> {
    use async_trait::async_trait;
    use std::sync::Arc;
    use surreal_sync_postgresql::{read_offset_relation_chunk, read_relation_chunk};
    use sync_core::{UniversalRelation, UniversalValue};
    use sync_transform::{
        run_source_runtime_with, RelationChunkDriver, RelationChunkSource, SourceRuntimeOpts,
    };

    if dry_run {
        let mut total = 0usize;
        if pk_columns.is_empty() {
            let mut offset = 0usize;
            loop {
                if cancel.is_cancelled() {
                    return Ok(total);
                }
                let rels = read_offset_relation_chunk(
                    client,
                    table_name,
                    offset,
                    batch_size,
                    &in_fk,
                    &out_fk,
                )
                .await?;
                if rels.is_empty() {
                    break;
                }
                let n = rels.len();
                total += n;
                offset += n;
                if n < batch_size {
                    break;
                }
            }
        } else {
            let mut after: Option<Vec<UniversalValue>> = None;
            let mut row_index_base = 0u64;
            loop {
                if cancel.is_cancelled() {
                    return Ok(total);
                }
                let chunk = read_relation_chunk(
                    client,
                    table_name,
                    pk_columns,
                    after.as_deref(),
                    batch_size,
                    &in_fk,
                    &out_fk,
                    row_index_base,
                )
                .await?;
                if chunk.relations.is_empty() {
                    break;
                }
                let n = chunk.relations.len();
                total += n;
                row_index_base += n as u64;
                after = chunk.last_pk;
                if n < batch_size {
                    break;
                }
            }
        }
        return Ok(total);
    }

    if pk_columns.is_empty() {
        tracing::warn!(
            "Relation table '{table_name}' has no primary key; streaming via OFFSET/LIMIT"
        );
        struct OffsetRelChunks<'a> {
            client: &'a Client,
            table_name: &'a str,
            batch_size: usize,
            offset: usize,
            in_fk: &'a sync_core::ForeignKeyDefinition,
            out_fk: &'a sync_core::ForeignKeyDefinition,
            cancel: &'a tokio_util::sync::CancellationToken,
            exhausted: bool,
        }
        #[async_trait]
        impl RelationChunkSource for OffsetRelChunks<'_> {
            async fn next_chunk(&mut self) -> Result<Option<Vec<UniversalRelation>>> {
                if self.exhausted || self.cancel.is_cancelled() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let rels = read_offset_relation_chunk(
                    self.client,
                    self.table_name,
                    self.offset,
                    self.batch_size,
                    self.in_fk,
                    self.out_fk,
                )
                .await?;
                if rels.is_empty() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let n = rels.len();
                self.offset += n;
                if n < self.batch_size {
                    self.exhausted = true;
                }
                Ok(Some(rels))
            }
        }
        let mut driver = RelationChunkDriver::new(OffsetRelChunks {
            client,
            table_name,
            batch_size,
            offset: 0,
            in_fk: &in_fk,
            out_fk: &out_fk,
            cancel,
            exhausted: false,
        });
        let transformer = Arc::new(pipeline.clone());
        run_source_runtime_with(
            &mut driver,
            surreal,
            transformer,
            apply_opts,
            &SourceRuntimeOpts::new(),
        )
        .await?;
        return Ok(driver.sunk_count() as usize);
    }

    struct KeysetRelChunks<'a> {
        client: &'a Client,
        table_name: &'a str,
        pk_columns: &'a [String],
        after: Option<Vec<UniversalValue>>,
        batch_size: usize,
        in_fk: &'a sync_core::ForeignKeyDefinition,
        out_fk: &'a sync_core::ForeignKeyDefinition,
        row_index_base: u64,
        cancel: &'a tokio_util::sync::CancellationToken,
        exhausted: bool,
    }
    #[async_trait]
    impl RelationChunkSource for KeysetRelChunks<'_> {
        async fn next_chunk(&mut self) -> Result<Option<Vec<UniversalRelation>>> {
            if self.exhausted || self.cancel.is_cancelled() {
                self.exhausted = true;
                return Ok(None);
            }
            let chunk = read_relation_chunk(
                self.client,
                self.table_name,
                self.pk_columns,
                self.after.as_deref(),
                self.batch_size,
                self.in_fk,
                self.out_fk,
                self.row_index_base,
            )
            .await?;
            if chunk.relations.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }
            let n = chunk.relations.len();
            self.row_index_base += n as u64;
            self.after = chunk.last_pk;
            if n < self.batch_size {
                self.exhausted = true;
            }
            Ok(Some(chunk.relations))
        }
    }
    let mut driver = RelationChunkDriver::new(KeysetRelChunks {
        client,
        table_name,
        pk_columns,
        after: None,
        batch_size,
        in_fk: &in_fk,
        out_fk: &out_fk,
        row_index_base: 0,
        cancel,
        exhausted: false,
    });
    let transformer = Arc::new(pipeline.clone());
    run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &SourceRuntimeOpts::new(),
    )
    .await?;
    Ok(driver.sunk_count() as usize)
}

#[allow(clippy::too_many_arguments)]
async fn migrate_offset_rows_streaming<S: SurrealSink>(
    client: &Client,
    surreal: &S,
    table_name: &str,
    batch_size: usize,
    schema: Option<&sync_core::DatabaseSchema>,
    dry_run: bool,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<usize> {
    use async_trait::async_trait;
    use std::sync::Arc;
    use surreal_sync_postgresql::read_offset_table_chunk;
    use sync_core::UniversalRow;
    use sync_transform::{
        run_source_runtime_with, RowChunkDriver, RowChunkSource, SourceRuntimeOpts,
    };

    if dry_run {
        let mut total = 0usize;
        let mut offset = 0usize;
        loop {
            if cancel.is_cancelled() {
                return Ok(total);
            }
            let (rows, _) =
                read_offset_table_chunk(client, table_name, offset, batch_size, schema, &[])
                    .await?;
            if rows.is_empty() {
                break;
            }
            let n = rows.len();
            total += n;
            offset += n;
            if n < batch_size {
                break;
            }
        }
        return Ok(total);
    }

    struct OffsetRowChunks<'a> {
        client: &'a Client,
        table_name: &'a str,
        batch_size: usize,
        offset: usize,
        schema: Option<&'a sync_core::DatabaseSchema>,
        cancel: &'a tokio_util::sync::CancellationToken,
        exhausted: bool,
    }
    #[async_trait]
    impl RowChunkSource for OffsetRowChunks<'_> {
        async fn next_chunk(&mut self) -> Result<Option<Vec<UniversalRow>>> {
            if self.exhausted || self.cancel.is_cancelled() {
                self.exhausted = true;
                return Ok(None);
            }
            let (rows, _) = read_offset_table_chunk(
                self.client,
                self.table_name,
                self.offset,
                self.batch_size,
                self.schema,
                &[],
            )
            .await?;
            if rows.is_empty() {
                self.exhausted = true;
                return Ok(None);
            }
            let n = rows.len();
            self.offset += n;
            if n < self.batch_size {
                self.exhausted = true;
            }
            Ok(Some(rows))
        }
    }
    let mut driver = RowChunkDriver::new(OffsetRowChunks {
        client,
        table_name,
        batch_size,
        offset: 0,
        schema,
        cancel,
        exhausted: false,
    });
    let transformer = Arc::new(pipeline.clone());
    run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &SourceRuntimeOpts::new(),
    )
    .await?;
    Ok(driver.sunk_count() as usize)
}

// End of streaming helpers for relation / no-PK full sync.

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
pub async fn capture_head_checkpoint(from_opts: &SourceOpts) -> Result<PgoutputCheckpoint> {
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

    Ok(PgoutputCheckpoint {
        lsn,
        timestamp: chrono::Utc::now(),
    })
}

async fn capture_wal_checkpoint(sql: &Arc<Mutex<Client>>) -> Result<PgoutputCheckpoint> {
    let client = sql.lock().await;
    let lsn = get_current_wal_lsn(&client).await?;
    Ok(PgoutputCheckpoint {
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
