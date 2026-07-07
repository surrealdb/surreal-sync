//! PostgreSQL pgoutput replication tail (post-handoff steady CDC).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::{DateTime, Utc};
use pgoutput_protocol::{PgWalClient, RelationMeta, StreamEvent};
use surreal_sink::SurrealSink;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::catch_up::{
    effective_sync_tables, emit_catch_up_progress, read_catch_up_progress, CatchUpProgress,
    CoverageKind,
};
use crate::change::cdc_change_to_universal;
use crate::checkpoint::{get_current_checkpoint, WalCheckpoint};
use crate::client::{
    connect_wal_client, ensure_publication_for_source, new_sql_client, resolve_schema,
    start_wal_from_checkpoint,
};
use crate::ddl::{
    detect_relation_rename_from_filter, detect_relation_renames, truncate_affects_synced,
};
use crate::schema::{collect_postgresql_database_schema, get_table_column_names_ordinal};
use crate::signal::{acknowledge_execute_snapshot_signal, read_pending_execute_snapshot_signals};
use crate::watermark_source::{
    run_adhoc_snapshots_for_tables, write_catch_up_for_tables, WalWatermarkSource,
};
use crate::SourceOpts;

/// Default chunk size for ad-hoc snapshots during steady-state streaming.
pub const DEFAULT_STREAM_CHUNK_SIZE: usize = 1024;

/// Default `next_events` batch size in the replication tail loop.
pub const DEFAULT_WAL_EVENT_BATCH_SIZE: usize = 32;

/// Default sleep when a replication tail poll returns no events.
pub const DEFAULT_REPLICATION_TAIL_IDLE_SLEEP: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    /// Optional wall-clock stop for the stream phase.
    pub deadline: Option<DateTime<Utc>>,
    /// Optional exact WAL stop bound for the stream phase.
    pub until: Option<WalCheckpoint>,
    pub checkpoint_interval: Duration,
    /// Rows read per keyset chunk when handling ad-hoc snapshot signals.
    pub chunk_size: usize,
    /// Max events requested per `next_events` call in the replication tail loop.
    pub event_batch_size: usize,
    /// Sleep when a poll returns no events before retrying.
    pub idle_sleep: Duration,
    /// Cooperative cancellation signal. When cancelled, the sync loop flushes a
    /// final resumable checkpoint and returns cleanly.
    pub cancel: tokio_util::sync::CancellationToken,
}

impl ReplicationTailOptions {
    pub fn stream(deadline: Option<DateTime<Utc>>, until: Option<WalCheckpoint>) -> Self {
        Self {
            deadline,
            until,
            checkpoint_interval: Duration::from_secs(10),
            chunk_size: DEFAULT_STREAM_CHUNK_SIZE,
            event_batch_size: DEFAULT_WAL_EVENT_BATCH_SIZE,
            idle_sleep: DEFAULT_REPLICATION_TAIL_IDLE_SLEEP,
            cancel: tokio_util::sync::CancellationToken::new(),
        }
    }

    pub fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
}

pub async fn run_replication_tail<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: WalCheckpoint,
) -> Result<()> {
    run_replication_tail_with_checkpoints::<S, checkpoint::NullStore>(
        surreal,
        from_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(None, None),
        None,
    )
    .await
}

pub async fn run_replication_tail_with_checkpoints<S, St>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: WalCheckpoint,
    options: ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
) -> Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    info!(
        "Starting PostgreSQL WAL incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    let sql = new_sql_client(&from_opts.connection_string).await?;
    let schema = resolve_schema(&from_opts).await;
    {
        let client = sql.lock().await;
        ensure_publication_for_source(&client, &from_opts, &schema).await?;
    }

    let mut db_schema = {
        let client = sql.lock().await;
        collect_postgresql_database_schema(&client).await?
    };

    let mut column_names_cache: HashMap<String, Vec<String>> = HashMap::new();
    let mut prev_relations: HashMap<u32, RelationMeta> = HashMap::new();

    let catch_up = if from_opts.tables.is_empty() {
        None
    } else if let Some(manager) = checkpoint_manager {
        read_catch_up_progress(manager).await?
    } else {
        None
    };
    let effective_tables = effective_sync_tables(&from_opts.tables, catch_up.as_ref());
    let mut table_filter = effective_tables.clone();

    let mut client = connect_wal_client(&from_opts)
        .await?
        .with_cancel(options.cancel.clone());
    client.ensure_replication_slot().await?;
    start_wal_from_checkpoint(&mut client, &from_checkpoint).await?;

    if let (Some(manager), Some(ref effective)) = (checkpoint_manager, &effective_tables) {
        if let Some(mut progress) = catch_up {
            let before = progress.covered_tables.len();
            progress.prune_to_requested(effective);
            if progress.covered_tables.len() != before {
                emit_catch_up_progress(manager, &progress).await?;
            }
        }
    }

    let mut total_changes = 0u64;
    let mut last_persisted_checkpoint: Option<WalCheckpoint> = None;
    let mut last_checkpoint_emit = std::time::Instant::now();
    let mut ddl_refresh_pending = false;

    loop {
        if options.cancel.is_cancelled() {
            persist_checkpoint(
                checkpoint_manager,
                &client,
                &mut last_persisted_checkpoint,
                true,
            )
            .await?;
            info!("Cancellation requested, stopping incremental sync (checkpoint flushed)");
            break;
        }

        if let Some(deadline) = options.deadline {
            if Utc::now() >= deadline {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Deadline reached, stopping incremental sync");
                break;
            }
        }

        if let Some(target) = options.until.as_ref() {
            if client.current_position() >= target.lsn {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Reached target checkpoint, stopping incremental sync");
                break;
            }
        }

        if should_emit_checkpoint(checkpoint_manager.is_some(), &options, last_checkpoint_emit) {
            persist_checkpoint(
                checkpoint_manager,
                &client,
                &mut last_persisted_checkpoint,
                false,
            )
            .await?;
            last_checkpoint_emit = std::time::Instant::now();
        }

        client = handle_snapshot_signals(
            surreal,
            &sql,
            &schema,
            &from_opts,
            client,
            &mut table_filter,
            &options,
            checkpoint_manager,
        )
        .await?;

        let events = tokio::select! {
            result = client.next_events(options.event_batch_size) => {
                result.map_err(|e| anyhow::anyhow!("wal read failed: {e}"))?
            }
            _ = options.cancel.cancelled() => {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Cancellation received, stopping incremental sync (checkpoint flushed)");
                break;
            }
            _ = async {
                if let Some(deadline) = options.deadline {
                    let wait = deadline.signed_duration_since(Utc::now());
                    if wait.num_milliseconds() > 0 {
                        tokio::time::sleep(wait.to_std().unwrap_or(Duration::from_secs(1))).await;
                    }
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                persist_checkpoint(
                    checkpoint_manager,
                    &client,
                    &mut last_persisted_checkpoint,
                    true,
                )
                .await?;
                info!("Deadline reached, stopping incremental sync");
                break;
            }
        };

        if events.is_empty() {
            tokio::time::sleep(options.idle_sleep).await;
            continue;
        }

        let had_events = !events.is_empty();
        for event in events {
            match event {
                StreamEvent::Relation(meta) => {
                    let mut renames = detect_relation_renames(&prev_relations, &meta);
                    if renames.is_empty() {
                        if let Some(rename) =
                            detect_relation_rename_from_filter(&table_filter, &meta)
                        {
                            renames.push(rename);
                        }
                    }
                    prev_relations.insert(meta.relation_oid, meta.clone());
                    if !renames.is_empty() {
                        apply_renames_to_filter(&mut table_filter, &renames);
                        info!(
                            "Refreshing PostgreSQL schema metadata after Relation rename for '{}'",
                            meta.table
                        );
                        let sql_client = sql.lock().await;
                        db_schema = collect_postgresql_database_schema(&sql_client).await?;
                        column_names_cache.clear();
                    } else {
                        ddl_refresh_pending = true;
                    }
                }
                StreamEvent::Type {
                    schema: type_schema,
                    name,
                    ..
                } => {
                    if type_schema == schema {
                        ddl_refresh_pending = true;
                        info!(
                            "PostgreSQL Type event for '{}.{}'; schema refresh deferred to commit",
                            type_schema, name
                        );
                    }
                }
                StreamEvent::Truncate { tables } => {
                    if truncate_affects_filtered(&tables, &table_filter) {
                        warn!(
                            "TRUNCATE on synced table(s) {:?}; target rows were removed at source",
                            tables
                        );
                    }
                }
                StreamEvent::Change(change) => {
                    let rename_from_filter = table_filter.as_ref().and_then(|tables| {
                        if !tables.contains(&change.table) && tables.len() == 1 {
                            Some(crate::ddl::TableRename {
                                old: tables[0].clone(),
                                new: change.table.clone(),
                            })
                        } else {
                            None
                        }
                    });
                    if let Some(rename) = rename_from_filter {
                        apply_renames_to_filter(&mut table_filter, &[rename]);
                    }
                    let relation = client
                        .relation_cache()
                        .get(&change.relation_oid)
                        .cloned()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "pgoutput change for relation_oid {} has no preceding Relation; \
                                 refusing to silently drop the change",
                                change.relation_oid
                            )
                        })?;
                    if relation.schema != schema {
                        continue;
                    }
                    if let Some(ref tables) = table_filter {
                        if !tables.contains(&change.table) {
                            continue;
                        }
                    }

                    let position = client.current_position();
                    if let Some(target) = options.until.as_ref() {
                        if position >= target.lsn {
                            persist_checkpoint(
                                checkpoint_manager,
                                &client,
                                &mut last_persisted_checkpoint,
                                true,
                            )
                            .await?;
                            info!("Reached target checkpoint, stopping incremental sync");
                            return Ok(());
                        }
                    }

                    let column_names = if let Some(names) = column_names_cache.get(&change.table) {
                        names.clone()
                    } else {
                        let sql_client = sql.lock().await;
                        let names =
                            get_table_column_names_ordinal(&sql_client, &schema, &change.table)
                                .await?;
                        column_names_cache.insert(change.table.clone(), names.clone());
                        names
                    };

                    let universal =
                        cdc_change_to_universal(&change, &relation, &column_names, &db_schema)?;

                    surreal.apply_universal_change(&universal).await?;
                    client.commit(position);
                    total_changes += 1;
                    persist_checkpoint(
                        checkpoint_manager,
                        &client,
                        &mut last_persisted_checkpoint,
                        false,
                    )
                    .await?;
                    last_checkpoint_emit = std::time::Instant::now();

                    if total_changes.is_multiple_of(100) {
                        debug!("Processed {total_changes} WAL changes");
                    }
                }
                StreamEvent::Control => {}
                StreamEvent::Commit => {
                    if ddl_refresh_pending {
                        info!("Refreshing PostgreSQL schema metadata after DDL commit");
                        let sql_client = sql.lock().await;
                        db_schema = collect_postgresql_database_schema(&sql_client).await?;
                        column_names_cache.clear();
                        ddl_refresh_pending = false;
                    }
                }
            }
        }
        if had_events {
            client.commit(client.current_position());
        }
    }

    persist_checkpoint(
        checkpoint_manager,
        &client,
        &mut last_persisted_checkpoint,
        true,
    )
    .await?;
    info!("PostgreSQL WAL incremental sync completed: {total_changes} changes applied");
    Ok(())
}

fn truncate_affects_filtered(tables: &[String], filter: &Option<Vec<String>>) -> bool {
    match filter {
        None => !tables.is_empty(),
        Some(synced) => truncate_affects_synced(tables, synced),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_snapshot_signals<S, St>(
    surreal: &S,
    sql: &std::sync::Arc<Mutex<tokio_postgres::Client>>,
    schema: &str,
    from_opts: &SourceOpts,
    client: PgWalClient,
    table_filter: &mut Option<Vec<String>>,
    options: &ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
) -> Result<PgWalClient>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let signals = {
        let sql_client = sql.lock().await;
        read_pending_execute_snapshot_signals(&sql_client).await?
    };
    if signals.is_empty() {
        return Ok(client);
    }

    let mut wm = WalWatermarkSource::wrap_active_wal_client(
        sql.clone(),
        schema.to_string(),
        from_opts,
        client,
        options.cancel.clone(),
    )
    .await?;

    for signal in signals {
        info!(
            "ad-hoc snapshot signal {} requesting tables {:?}",
            signal.id, signal.tables
        );
        if let Some(ref mut tables) = table_filter {
            for name in &signal.tables {
                if !tables.contains(name) {
                    tables.push(name.clone());
                }
            }
        }
        run_adhoc_snapshots_for_tables(
            &mut wm,
            surreal,
            &signal.tables,
            options.chunk_size,
            checkpoint_manager,
        )
        .await?;

        {
            let sql_client = sql.lock().await;
            acknowledge_execute_snapshot_signal(&sql_client, &signal.id).await?;
        }

        if let Some(manager) = checkpoint_manager {
            let checkpoint = wm.current_checkpoint()?;
            let existing = read_catch_up_progress(manager).await?;
            write_catch_up_for_tables(
                manager,
                signal.tables.clone(),
                CoverageKind::AdHoc,
                &checkpoint,
                existing,
            )
            .await?;
        }
    }

    Ok(wm.into_wal_client())
}

fn apply_renames_to_filter(filter: &mut Option<Vec<String>>, renames: &[crate::ddl::TableRename]) {
    let Some(tables) = filter.as_mut() else {
        return;
    };
    for rename in renames {
        if tables.iter().any(|t| t == &rename.old) {
            tables.retain(|t| t != &rename.old);
            if !tables.iter().any(|t| t == &rename.new) {
                tables.push(rename.new.clone());
            }
            tracing::info!(
                "Relation rename '{}' -> '{}': tracking the renamed table under its new name",
                rename.old,
                rename.new
            );
        }
    }
}

fn should_emit_checkpoint(
    enabled: bool,
    options: &ReplicationTailOptions,
    last_emit: std::time::Instant,
) -> bool {
    enabled
        && options.checkpoint_interval > Duration::ZERO
        && last_emit.elapsed() >= options.checkpoint_interval
}

async fn persist_checkpoint<St: CheckpointStore>(
    manager: Option<&SyncManager<St>>,
    client: &PgWalClient,
    last: &mut Option<WalCheckpoint>,
    force: bool,
) -> Result<()> {
    let Some(manager) = manager else {
        return Ok(());
    };
    let checkpoint = get_current_checkpoint(client)?;
    if !force && last.as_ref().is_some_and(|prev| prev.lsn == checkpoint.lsn) {
        return Ok(());
    }
    let existing = read_catch_up_progress(manager).await?;
    let mut progress = existing.unwrap_or_else(|| CatchUpProgress::new(checkpoint.clone()));
    progress.update_position(checkpoint.clone());
    manager
        .emit_checkpoint(&progress, SyncPhase::CatchUpProgress)
        .await?;
    *last = Some(checkpoint);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::TableRename;

    #[test]
    fn rename_updates_synced_table_filter() {
        let mut filter = Some(vec!["orders".to_string(), "users".to_string()]);
        apply_renames_to_filter(
            &mut filter,
            &[TableRename {
                old: "orders".into(),
                new: "orders_v2".into(),
            }],
        );
        let tables = filter.unwrap();
        assert!(tables.contains(&"orders_v2".to_string()));
        assert!(!tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"users".to_string()));
    }

    #[test]
    fn rename_of_unsynced_table_is_ignored() {
        let mut filter = Some(vec!["orders".to_string()]);
        apply_renames_to_filter(
            &mut filter,
            &[TableRename {
                old: "audit".into(),
                new: "audit_v2".into(),
            }],
        );
        assert_eq!(filter.unwrap(), vec!["orders".to_string()]);
    }

    #[test]
    fn rename_with_no_filter_is_noop() {
        let mut filter: Option<Vec<String>> = None;
        apply_renames_to_filter(
            &mut filter,
            &[TableRename {
                old: "orders".into(),
                new: "orders_v2".into(),
            }],
        );
        assert!(filter.is_none());
    }
}
