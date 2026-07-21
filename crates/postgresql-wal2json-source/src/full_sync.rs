//! PostgreSQL logical replication full sync implementation
//!
//! This module provides functionality to perform full database migration from PostgreSQL
//! to SurrealDB using logical replication infrastructure, with checkpoint emission support
//! for coordinated incremental sync.

use anyhow::Result;
use checkpoint::{CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::SyncOpts;
use sync_transform::{ApplyOpts, Pipeline};
use tokio_postgres::NoTls;
use tracing::{debug, info};

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
    /// Tables to force-classify as relation (join) tables for SurrealDB RELATE.
    /// When empty (default), auto-detection is used based on FK/PK heuristics.
    pub relation_tables: Vec<String>,
}

/// Run full sync from PostgreSQL to SurrealDB with checkpoint support (identity transforms).
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
///
/// Rows are converted with existing PostgreSQL FK helpers first, then applied
/// via [`write_rows`] / [`write_relations`].
pub async fn run_full_sync_with_transforms<S: SurrealSink, CS: CheckpointStore>(
    surreal: &S,
    from_opts: SourceOpts,
    sync_opts: SyncOpts,
    sync_manager: Option<&SyncManager<CS>>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!("Starting PostgreSQL logical replication full sync to SurrealDB");
    if pipeline.is_identity() {
        debug!("Full sync using identity transform pipeline");
    } else {
        info!(
            stages = pipeline.len(),
            "Full sync using transform pipeline"
        );
    }

    let (client, connection) = tokio_postgres::connect(&from_opts.connection_string, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let pg_client = crate::Client::new(client, from_opts.tables.clone());
    pg_client.create_slot(&from_opts.slot_name).await?;

    if let Some(manager) = sync_manager {
        let checkpoint = pg_client.get_current_wal_lsn_checkpoint().await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.lsn
        );
    }

    let tables = if from_opts.tables.is_empty() {
        surreal_sync_postgresql::get_user_tables(pg_client.pg_client(), &from_opts.schema).await?
    } else {
        from_opts.tables.clone()
    };

    info!("Found {} tables to migrate", tables.len());

    let db_schema =
        surreal_sync_postgresql::schema::collect_database_schema_with_fks(pg_client.pg_client())
            .await?;

    let mut total_migrated = 0;

    for table_name in &tables {
        info!("Migrating table: {}", table_name);

        let count = migrate_one_table_keyset(
            pg_client.pg_client(),
            surreal,
            table_name,
            &sync_opts,
            Some(&db_schema),
            &from_opts.relation_tables,
            pipeline,
            apply_opts,
        )
        .await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    if let Some(manager) = sync_manager {
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

#[allow(clippy::too_many_arguments)]
async fn migrate_one_table_keyset<S: surreal_sink::SurrealSink>(
    client: &tokio_postgres::Client,
    surreal: &S,
    table_name: &str,
    sync_opts: &surreal_sync_postgresql::SyncOpts,
    schema: Option<&sync_core::DatabaseSchema>,
    relation_overrides: &[String],
    pipeline: &sync_transform::Pipeline,
    apply_opts: &sync_transform::ApplyOpts,
) -> anyhow::Result<usize> {
    use async_trait::async_trait;
    use std::sync::Arc;
    use surreal_sync_postgresql::{convert_table, get_primary_key_columns, read_table_chunk};
    use sync_core::{classify_table, TableKind, UniversalRow};
    use sync_transform::{
        run_source_runtime_with, write_relations, write_rows, RowChunkDriver, RowChunkSource,
        SourceRuntimeOpts,
    };

    let pk_columns = get_primary_key_columns(client, table_name).await?;
    let table_kind = schema
        .and_then(|s| s.get_table(table_name))
        .map(|td| classify_table(td, relation_overrides));
    let is_relation = matches!(table_kind, Some(TableKind::Relation { .. }));
    let batch_size = sync_opts.batch_size.max(1);

    if is_relation || pk_columns.is_empty() {
        if pk_columns.is_empty() && !is_relation {
            tracing::warn!(
                "Table '{table_name}' has no primary key; falling back to full SELECT * (loads table into memory)"
            );
        }
        let (rows, relations) =
            convert_table(client, table_name, schema, relation_overrides).await?;
        let mut total = 0usize;
        for chunk in rows.chunks(batch_size) {
            if !sync_opts.dry_run {
                write_rows(surreal, pipeline, chunk.to_vec(), apply_opts).await?;
            }
            total += chunk.len();
        }
        for chunk in relations.chunks(batch_size) {
            if !sync_opts.dry_run {
                write_relations(surreal, pipeline, chunk.to_vec(), apply_opts).await?;
            }
            total += chunk.len();
        }
        return Ok(total);
    }

    if sync_opts.dry_run {
        let mut total = 0usize;
        let mut after: Option<Vec<sync_core::UniversalValue>> = None;
        loop {
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
            total += n;
            after = chunk.last_pk;
            if n < batch_size {
                break;
            }
        }
        return Ok(total);
    }

    struct PgKeysetChunks<'a> {
        client: &'a tokio_postgres::Client,
        table_name: &'a str,
        pk_columns: &'a [String],
        after: Option<Vec<sync_core::UniversalValue>>,
        batch_size: usize,
        schema: Option<&'a sync_core::DatabaseSchema>,
        exhausted: bool,
    }

    #[async_trait]
    impl RowChunkSource for PgKeysetChunks<'_> {
        async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<UniversalRow>>> {
            if self.exhausted {
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

