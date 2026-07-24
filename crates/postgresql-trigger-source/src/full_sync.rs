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
use sync_transform::{ApplyOpts, Pipeline};
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
    run_full_sync_with_transforms(
        surreal,
        from_opts,
        sync_opts,
        sync_manager,
        &pipeline,
        &apply_opts,
    )
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

    for table_name in &tables {
        info!("Migrating table: {}", table_name);
        let count = migrate_one_table_keyset(
            &client,
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
    use surreal_sync_postgresql::{
        get_primary_key_columns, read_offset_relation_chunk, read_offset_table_chunk,
        read_relation_chunk, read_table_chunk,
    };
    use sync_core::{classify_table, Relation, Row, TableKind, Value};
    use sync_transform::{
        run_source_runtime_with, RelationChunkDriver, RelationChunkSource, RowChunkDriver,
        RowChunkSource, SourceRuntimeOpts,
    };

    let pk_columns = get_primary_key_columns(client, table_name).await?;
    let table_kind = schema
        .and_then(|s| s.get_table(table_name))
        .map(|td| classify_table(td, relation_overrides));
    let batch_size = sync_opts.batch_size.max(1);

    if let Some(TableKind::Relation { in_fk, out_fk }) = table_kind.clone() {
        if sync_opts.dry_run {
            let mut total = 0usize;
            if pk_columns.is_empty() {
                let mut offset = 0usize;
                loop {
                    let rels = read_offset_relation_chunk(
                        client, table_name, offset, batch_size, &in_fk, &out_fk,
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
                let mut after: Option<Vec<Value>> = None;
                let mut base = 0u64;
                loop {
                    let chunk = read_relation_chunk(
                        client,
                        table_name,
                        &pk_columns,
                        after.as_deref(),
                        batch_size,
                        &in_fk,
                        &out_fk,
                        base,
                    )
                    .await?;
                    if chunk.relations.is_empty() {
                        break;
                    }
                    let n = chunk.relations.len();
                    total += n;
                    base += n as u64;
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
                "Relation table '{table_name}' has no primary key; streaming via OFFSET/LIMIT \
                 (ORDER BY ctid). Unsafe under concurrent source writes — prefer a PK \
                 or interleaved-snapshot"
            );
            struct OffsetRel<'a> {
                client: &'a tokio_postgres::Client,
                table_name: &'a str,
                batch_size: usize,
                offset: usize,
                in_fk: &'a sync_core::ForeignKeyDefinition,
                out_fk: &'a sync_core::ForeignKeyDefinition,
                exhausted: bool,
            }
            #[async_trait]
            impl RelationChunkSource for OffsetRel<'_> {
                async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Relation>>> {
                    if self.exhausted {
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
            let mut driver = RelationChunkDriver::new(OffsetRel {
                client,
                table_name,
                batch_size,
                offset: 0,
                in_fk: &in_fk,
                out_fk: &out_fk,
                exhausted: false,
            });
            run_source_runtime_with(
                &mut driver,
                surreal,
                Arc::new(pipeline.clone()),
                apply_opts,
                &SourceRuntimeOpts::new(),
            )
            .await?;
            return Ok(driver.sunk_count() as usize);
        }

        struct KeysetRel<'a> {
            client: &'a tokio_postgres::Client,
            table_name: &'a str,
            pk_columns: &'a [String],
            after: Option<Vec<Value>>,
            batch_size: usize,
            in_fk: &'a sync_core::ForeignKeyDefinition,
            out_fk: &'a sync_core::ForeignKeyDefinition,
            base: u64,
            exhausted: bool,
        }
        #[async_trait]
        impl RelationChunkSource for KeysetRel<'_> {
            async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Relation>>> {
                if self.exhausted {
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
                    self.base,
                )
                .await?;
                if chunk.relations.is_empty() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let n = chunk.relations.len();
                self.base += n as u64;
                self.after = chunk.last_pk;
                if n < self.batch_size {
                    self.exhausted = true;
                }
                Ok(Some(chunk.relations))
            }
        }
        let mut driver = RelationChunkDriver::new(KeysetRel {
            client,
            table_name,
            pk_columns: &pk_columns,
            after: None,
            batch_size,
            in_fk: &in_fk,
            out_fk: &out_fk,
            base: 0,
            exhausted: false,
        });
        run_source_runtime_with(
            &mut driver,
            surreal,
            Arc::new(pipeline.clone()),
            apply_opts,
            &SourceRuntimeOpts::new(),
        )
        .await?;
        return Ok(driver.sunk_count() as usize);
    }

    if pk_columns.is_empty() {
        tracing::warn!(
            "Table '{table_name}' has no primary key; streaming via OFFSET/LIMIT chunks \
             (ORDER BY ctid). Unsafe under concurrent source writes — prefer a PK \
             or interleaved-snapshot"
        );
        if sync_opts.dry_run {
            let mut total = 0usize;
            let mut offset = 0usize;
            loop {
                let (rows, _) = read_offset_table_chunk(
                    client,
                    table_name,
                    offset,
                    batch_size,
                    schema,
                    relation_overrides,
                )
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
        struct OffsetRows<'a> {
            client: &'a tokio_postgres::Client,
            table_name: &'a str,
            batch_size: usize,
            offset: usize,
            schema: Option<&'a sync_core::DatabaseSchema>,
            overrides: &'a [String],
            exhausted: bool,
        }
        #[async_trait]
        impl RowChunkSource for OffsetRows<'_> {
            async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Row>>> {
                if self.exhausted {
                    return Ok(None);
                }
                let (rows, _) = read_offset_table_chunk(
                    self.client,
                    self.table_name,
                    self.offset,
                    self.batch_size,
                    self.schema,
                    self.overrides,
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
        let mut driver = RowChunkDriver::new(OffsetRows {
            client,
            table_name,
            batch_size,
            offset: 0,
            schema,
            overrides: relation_overrides,
            exhausted: false,
        });
        run_source_runtime_with(
            &mut driver,
            surreal,
            Arc::new(pipeline.clone()),
            apply_opts,
            &SourceRuntimeOpts::new(),
        )
        .await?;
        return Ok(driver.sunk_count() as usize);
    }

    if sync_opts.dry_run {
        let mut total = 0usize;
        let mut after: Option<Vec<sync_core::Value>> = None;
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
        after: Option<Vec<sync_core::Value>>,
        batch_size: usize,
        schema: Option<&'a sync_core::DatabaseSchema>,
        exhausted: bool,
    }

    #[async_trait]
    impl RowChunkSource for PgKeysetChunks<'_> {
        async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<Row>>> {
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
    run_source_runtime_with(&mut driver, surreal, transformer, apply_opts, &runtime_opts).await?;
    Ok(driver.sunk_count() as usize)
}
