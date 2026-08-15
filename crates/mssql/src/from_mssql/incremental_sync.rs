//! SQL Server CDC replication tail (after snapshot handoff).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Checkpoint, CheckpointStore, DatabaseSchema, SyncManager, SyncPhase};
use surreal_sync_runtime::{
    ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver, SourceRuntimeOpts,
    StopReason,
};
use tracing::info;

use crate::from_mssql::catalog::{MssqlTableMeta, TableSyncKind};
use crate::from_mssql::cdc::{self, CdcChange, CdcOperation};
use crate::from_mssql::checkpoint::{MssqlCheckpoint, MssqlLsn};
use crate::from_mssql::client::MssqlClient;
use crate::from_mssql::regular::{self, RegularCdc};
use crate::from_mssql::schema::temporal_targets;
use crate::from_mssql::temporal;
use crate::from_mssql::SourceOpts;

const DEFAULT_IDLE_SLEEP: Duration = Duration::from_millis(100);
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);

/// Options for the CDC replication tail.
#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    pub deadline: Option<DateTime<Utc>>,
    pub until: Option<MssqlLsn>,
    pub checkpoint_interval: Duration,
    pub idle_sleep: Duration,
    pub cancel: tokio_util::sync::CancellationToken,
    pub event_batch_size: usize,
}

impl ReplicationTailOptions {
    pub fn stream(deadline: Option<DateTime<Utc>>, until: Option<MssqlLsn>) -> Self {
        Self {
            deadline,
            until,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            idle_sleep: DEFAULT_IDLE_SLEEP,
            cancel: tokio_util::sync::CancellationToken::new(),
            event_batch_size: 32,
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
    from_checkpoint: MssqlCheckpoint,
) -> Result<()> {
    run_replication_tail_with_checkpoints::<S, surreal_sync_core::NullStore>(
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
    from_checkpoint: MssqlCheckpoint,
    options: ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
) -> Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_replication_tail_with_transforms(
        surreal,
        from_opts,
        from_checkpoint,
        options,
        checkpoint_manager,
        &pipeline,
        &apply_opts,
    )
    .await
}

pub async fn run_replication_tail_with_transforms<S, St>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: MssqlCheckpoint,
    options: ReplicationTailOptions,
    checkpoint_manager: Option<&SyncManager<St>>,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    info!(
        "Starting SQL Server CDC incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    let client = crate::from_mssql::client::connect(&from_opts.connection_string).await?;
    let metas =
        crate::from_mssql::catalog::collect_database_schema(&client, &from_opts.tables).await?;
    let db_schema = crate::from_mssql::schema::database_schema(&metas);
    let mut driver = MssqlSourceDriver {
        client,
        metas,
        db_schema,
        relation_tables: from_opts.relation_tables.clone(),
        from_lsn: Some(from_checkpoint.lsn.clone()),
        last_sunk: Some(from_checkpoint.clone()),
        options: options.clone(),
        checkpoint_manager,
        until_reached: false,
        cancel_seen: false,
        total_changes: 0,
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = surreal_sync_runtime::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Cancelled) => {
            info!("Cancellation requested, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Deadline) => {
            info!("Deadline reached, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Until) => {
            info!("Reached target LSN, stopping incremental sync");
        }
        surreal_sync_runtime::RuntimeExit::Stopped(StopReason::Finished) => {
            info!("SQL Server CDC source finished");
        }
    }

    if let Some(cp) = driver.last_sunk.clone() {
        if let Some(manager) = checkpoint_manager {
            manager
                .emit_checkpoint(&cp, SyncPhase::CatchUpProgress)
                .await?;
        }
    }
    info!(
        "SQL Server CDC incremental sync completed: {} changes sunk",
        driver.total_changes
    );
    Ok(())
}

struct MssqlSourceDriver<'a, St: CheckpointStore> {
    client: MssqlClient,
    metas: Vec<MssqlTableMeta>,
    db_schema: DatabaseSchema,
    relation_tables: Vec<String>,
    from_lsn: Option<MssqlLsn>,
    last_sunk: Option<MssqlCheckpoint>,
    options: ReplicationTailOptions,
    checkpoint_manager: Option<&'a SyncManager<St>>,
    until_reached: bool,
    cancel_seen: bool,
    total_changes: u64,
}

impl<St> MssqlSourceDriver<'_, St>
where
    St: CheckpointStore,
{
    async fn poll_all(&mut self) -> Result<Vec<(CdcChange, MssqlTableMeta)>> {
        let Some(to) = cdc::max_lsn(&self.client).await? else {
            return Ok(Vec::new());
        };
        if let Some(until) = &self.options.until {
            if to >= *until {
                self.until_reached = true;
            }
        }
        let mut batch = Vec::new();
        for meta in &self.metas {
            let changes =
                cdc::poll_changes(&self.client, meta, self.from_lsn.as_ref(), &to).await?;
            for ch in changes {
                batch.push((ch, meta.clone()));
            }
        }
        Ok(batch)
    }
}

#[async_trait::async_trait]
impl<St> SourceDriver for MssqlSourceDriver<'_, St>
where
    St: CheckpointStore,
{
    type Position = MssqlCheckpoint;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.options.cancel.is_cancelled() {
            self.cancel_seen = true;
            return Ok(Vec::new());
        }
        if let Some(deadline) = self.options.deadline {
            if Utc::now() >= deadline {
                return Ok(Vec::new());
            }
        }

        let batch = self.poll_all().await?;
        if batch.is_empty() {
            tokio::time::sleep(self.options.idle_sleep).await;
            return Ok(Vec::new());
        }

        let temporal = temporal_targets(&self.metas);
        let mut events = Vec::new();
        for (change, meta) in batch {
            let pos = MssqlCheckpoint::new(change.start_lsn.clone());
            match meta.kind {
                TableSyncKind::Regular => {
                    if change.operation == CdcOperation::UpdateBefore {
                        continue;
                    }
                    let td = self.db_schema.get_table(&meta.target);
                    match regular::apply_change(
                        &meta,
                        &change,
                        td,
                        &self.relation_tables,
                        &temporal,
                    )? {
                        RegularCdc::Row(c) => events.push(PositionedEvent::change(c, pos)),
                        RegularCdc::Relation(r) => {
                            events.push(PositionedEvent::relation_change(r, pos))
                        }
                    }
                }
                TableSyncKind::Temporal => {
                    let td = self.db_schema.get_table(&meta.target);
                    let action = temporal::apply_change(&meta, &change, td, &temporal)?;
                    if let Some(c) = action.new_version {
                        events.push(PositionedEvent::change(c, pos));
                    }
                }
            }
        }
        Ok(events)
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()> {
        self.from_lsn = Some(position.lsn.clone());
        self.last_sunk = Some(position);
        self.total_changes += 1;
        Ok(())
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if self.cancel_seen || self.options.cancel.is_cancelled() {
            return Some(StopReason::Cancelled);
        }
        if let Some(deadline) = self.options.deadline {
            if Utc::now() >= deadline {
                return Some(StopReason::Deadline);
            }
        }
        if self.until_reached {
            return Some(StopReason::Until);
        }
        None
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::IntervalWhenDrained {
            interval: self.options.checkpoint_interval,
        }
    }

    async fn persist_checkpoint(&mut self, position: Self::Position) -> Result<()> {
        if let Some(manager) = self.checkpoint_manager {
            manager
                .emit_checkpoint(&position, SyncPhase::CatchUpProgress)
                .await?;
        }
        Ok(())
    }
}
