//! Sequential SNAPSHOT-isolation dump (CDC tail is started by the embed orchestrator).

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{
    Checkpoint, CheckpointStore, DatabaseSchema, Row, SyncManager, SyncPhase, TableDefinition,
    TableKind, Value,
};
use surreal_sync_runtime::{
    run_source_runtime_with, write_relations, ApplyOpts, Pipeline, RowChunkDriver, RowChunkSource,
    SourceRuntimeOpts,
};
use tracing::info;

use crate::from_mssql::catalog::{collect_database_schema, MssqlTableMeta, TableSyncKind};
use crate::from_mssql::cdc;
use crate::from_mssql::checkpoint::MssqlCheckpoint;
use crate::from_mssql::client::MssqlClient;
use crate::from_mssql::regular;
use crate::from_mssql::schema::{database_schema, schemafull_extras, temporal_targets};
use crate::from_mssql::temporal;
use crate::from_mssql::{SourceOpts, SyncOpts};

/// Copy tables in one SNAPSHOT-isolation transaction and return the snapshot LSN.
///
/// Writers are not locked (SNAPSHOT isolation is MVCC). Requires
/// `ALLOW_SNAPSHOT_ISOLATION`. There is no signal table on this path.
/// `--snapshot-mode initial` starts CDC from the returned checkpoint;
/// `--snapshot-mode only` stops after this dump.
#[allow(clippy::too_many_arguments)]
pub async fn run_sequential_snapshot_with_transforms<S, St>(
    sink: &S,
    from_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    chunk_size: usize,
    cancel: &tokio_util::sync::CancellationToken,
    manager: Option<&SyncManager<St>>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<MssqlCheckpoint>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let client = crate::from_mssql::client::connect(&from_opts.connection_string).await?;
    ensure_snapshot_isolation(&client).await?;

    cdc::ensure_cdc_enabled(&client).await?;
    let metas = collect_database_schema(&client, &from_opts.tables).await?;
    for meta in &metas {
        cdc::ensure_table_cdc(&client, &meta.source).await?;
    }

    let snapshot_lsn = loop {
        if let Some(lsn) = cdc::max_lsn(&client).await? {
            break lsn;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if cancel.is_cancelled() {
            anyhow::bail!("SNAPSHOT dump cancelled before CDC reported a max LSN");
        }
    };
    let start = MssqlCheckpoint::new(snapshot_lsn);
    if let Some(manager) = manager {
        manager
            .emit_checkpoint(&start, SyncPhase::FullSyncStart)
            .await?;
        info!(
            "SNAPSHOT isolation dump starting at LSN {}",
            start.to_cli_string()
        );
    }

    let db_schema = database_schema(&metas);
    let extras = schemafull_extras(&metas, from_opts.relation_tables.clone());
    surreal_sync_core::maybe_emit_schemafull(
        sink,
        &db_schema,
        &extras,
        sync_opts.schemafull,
        sync_opts.dry_run,
    )
    .await?;

    client
        .simple_query("SET TRANSACTION ISOLATION LEVEL SNAPSHOT; BEGIN TRANSACTION;")
        .await?;

    let temporal = temporal_targets(&metas);
    let dump_result = dump_tables(
        &client,
        sink,
        &metas,
        &db_schema,
        &from_opts.relation_tables,
        &temporal,
        chunk_size,
        sync_opts,
        cancel,
        pipeline,
        apply_opts,
    )
    .await;

    let commit = client.simple_query("COMMIT TRANSACTION;").await;
    dump_result?;
    commit?;

    if let Some(manager) = manager {
        manager
            .emit_checkpoint(&start, SyncPhase::FullSyncEnd)
            .await?;
    }

    Ok(start)
}

async fn ensure_snapshot_isolation(client: &MssqlClient) -> Result<()> {
    let rows = client
        .query(
            "SELECT snapshot_isolation_state, DB_NAME() FROM sys.databases WHERE database_id = DB_ID()",
            &[],
        )
        .await?;
    let state: u8 = rows
        .first()
        .and_then(|r| r.try_get::<u8, _>(0).ok().flatten())
        .or_else(|| {
            rows.first()
                .and_then(|r| r.try_get::<i32, _>(0).ok().flatten())
                .map(|v| v as u8)
        })
        .unwrap_or(0);
    let db: String = rows
        .first()
        .and_then(|r| r.try_get::<&str, _>(1).ok().flatten())
        .unwrap_or("the current database")
        .to_string();
    if state == 1 {
        return Ok(());
    }
    Err(anyhow!(
        "SNAPSHOT isolation is not enabled for database `{db}`. A DBA must run:\n\
         ALTER DATABASE [{db}] SET ALLOW_SNAPSHOT_ISOLATION ON"
    ))
}

#[allow(clippy::too_many_arguments)]
async fn dump_tables<S: SurrealSink>(
    client: &MssqlClient,
    sink: &S,
    metas: &[MssqlTableMeta],
    db_schema: &DatabaseSchema,
    relation_tables: &[String],
    temporal: &std::collections::HashSet<String>,
    chunk_size: usize,
    sync_opts: &SyncOpts,
    cancel: &tokio_util::sync::CancellationToken,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    for meta in metas {
        if cancel.is_cancelled() {
            info!(
                "Cancellation requested during SNAPSHOT dump; stopping before `{}`",
                meta.source
            );
            return Ok(());
        }
        info!("Copying table {}", meta.source);
        if sync_opts.dry_run {
            continue;
        }
        let td = db_schema.get_table(&meta.target);
        if meta.kind == TableSyncKind::Regular {
            if let Some(td) = td {
                if let TableKind::Relation { .. } = regular::classify(td, relation_tables, temporal)
                {
                    dump_relations(
                        client,
                        sink,
                        meta,
                        td,
                        relation_tables,
                        temporal,
                        chunk_size,
                        pipeline,
                        apply_opts,
                    )
                    .await?;
                    continue;
                }
            }
        }
        let source = KeysetChunkSource {
            client: client.clone(),
            meta: meta.clone(),
            after: None,
            chunk_size,
            table_def: td.cloned(),
            relation_tables: relation_tables.to_vec(),
            temporal: temporal.clone(),
            done: false,
        };
        let mut driver = RowChunkDriver::new(source);
        let transformer = Arc::new(pipeline.clone());
        run_source_runtime_with(
            &mut driver,
            sink,
            transformer,
            apply_opts,
            &SourceRuntimeOpts::new(),
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dump_relations<S: SurrealSink>(
    client: &MssqlClient,
    sink: &S,
    meta: &MssqlTableMeta,
    td: &TableDefinition,
    relation_tables: &[String],
    temporal: &std::collections::HashSet<String>,
    chunk_size: usize,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    let mut after: Option<Vec<Value>> = None;
    loop {
        let maps = regular::read_chunk(client, meta, after.as_deref(), chunk_size).await?;
        if maps.is_empty() {
            break;
        }
        after = maps.last().map(|m| {
            meta.pk_columns
                .iter()
                .map(|c| m.get(c).cloned().unwrap_or(Value::Null))
                .collect()
        });
        let (_, rels) = regular::snapshot_items(meta, maps, 0, Some(td), relation_tables, temporal);
        if rels.is_empty() {
            break;
        }
        write_relations(sink, pipeline, rels, apply_opts).await?;
    }
    Ok(())
}

struct KeysetChunkSource {
    client: MssqlClient,
    meta: MssqlTableMeta,
    after: Option<Vec<Value>>,
    chunk_size: usize,
    table_def: Option<TableDefinition>,
    relation_tables: Vec<String>,
    temporal: std::collections::HashSet<String>,
    done: bool,
}

#[async_trait]
impl RowChunkSource for KeysetChunkSource {
    async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>> {
        if self.done {
            return Ok(None);
        }
        let maps = match self.meta.kind {
            TableSyncKind::Regular => {
                regular::read_chunk(
                    &self.client,
                    &self.meta,
                    self.after.as_deref(),
                    self.chunk_size,
                )
                .await?
            }
            TableSyncKind::Temporal => {
                temporal::read_chunk(
                    &self.client,
                    &self.meta,
                    self.after.as_deref(),
                    self.chunk_size,
                )
                .await?
            }
        };
        if maps.is_empty() {
            self.done = true;
            return Ok(None);
        }
        let rows = match self.meta.kind {
            TableSyncKind::Regular => {
                let (rows, _) = regular::snapshot_items(
                    &self.meta,
                    maps,
                    0,
                    self.table_def.as_ref(),
                    &self.relation_tables,
                    &self.temporal,
                );
                if let Some(last) = rows.last() {
                    self.after = Some(
                        self.meta
                            .pk_columns
                            .iter()
                            .map(|c| last.fields.get(c).cloned().unwrap_or(Value::Null))
                            .collect(),
                    );
                }
                rows
            }
            TableSyncKind::Temporal => {
                let rows = temporal::rows_from_maps(&self.meta, maps, 0);
                if let Some(last) = rows.last() {
                    self.after = Some(temporal::chunk_after_from_row(&self.meta, last));
                }
                rows
            }
        };
        if rows.is_empty() {
            self.done = true;
            return Ok(None);
        }
        Ok(Some(rows))
    }
}
