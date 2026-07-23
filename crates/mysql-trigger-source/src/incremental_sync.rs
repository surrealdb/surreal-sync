//! MySQL incremental sync implementation for surreal-sync
//!
//! This module provides functionality to perform incremental database migration from MySQL
//! to SurrealDB.

use super::checkpoint::MySQLCheckpoint;
use super::source::IncrementalSource;
use crate::SourceOpts;
use anyhow::Result;
use checkpoint::Checkpoint;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use surreal_sink::SurrealSink;
use sync_transform::{
    ApplyOpts, CheckpointPolicy, Pipeline, PositionedEvent, SourceDriver, SourceRuntimeOpts,
    StopReason,
};
use tracing::{debug, info, warn};

/// Options for the MySQL trigger replication tail.
#[derive(Clone, Debug)]
pub struct ReplicationTailOptions {
    pub deadline: DateTime<Utc>,
    pub until: Option<MySQLCheckpoint>,
}

impl ReplicationTailOptions {
    pub fn stream(deadline: DateTime<Utc>, until: Option<MySQLCheckpoint>) -> Self {
        Self { deadline, until }
    }
}

/// Run incremental sync from MySQL to SurrealDB (identity transforms).
pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: MySQLCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    target_checkpoint: Option<MySQLCheckpoint>,
) -> Result<()> {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_incremental_sync_with_transforms(
        surreal,
        from_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(deadline, target_checkpoint),
        &pipeline,
        &apply_opts,
    )
    .await
}

/// Incremental sync via `SourceDriver` + `run_source_runtime` (shared apply window).
pub async fn run_incremental_sync_with_transforms<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: MySQLCheckpoint,
    options: ReplicationTailOptions,
    pipeline: &Pipeline,
    apply_opts: &ApplyOpts,
) -> Result<()> {
    info!(
        "Starting MySQL incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    let sequence_id = from_checkpoint.sequence_id;
    let pool =
        super::client::new_mysql_pool_with_ssl(&from_opts.source_uri, &from_opts.ssl).await?;
    let mut source = super::source::MySQLIncrementalSource::with_id_column_overrides(
        pool,
        sequence_id,
        from_opts.id_column_overrides,
    );
    source.initialize().await?;

    let stream = source.get_changes().await?;
    info!("Starting to consume MySQL change stream...");

    let mut driver = MysqlTriggerSourceDriver {
        stream,
        options: &options,
        until_reached: false,
        finished: false,
        total_changes: 0,
    };

    let runtime_opts = SourceRuntimeOpts::new();
    let transformer = Arc::new(pipeline.clone());
    let exit = sync_transform::run_source_runtime_with(
        &mut driver,
        surreal,
        transformer,
        apply_opts,
        &runtime_opts,
    )
    .await?;

    match exit {
        sync_transform::RuntimeExit::Stopped(StopReason::Deadline) => {
            info!("Reached deadline, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Until) => {
            info!("Reached target checkpoint, stopping incremental sync");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Finished) => {
            info!("MySQL trigger source finished");
        }
        sync_transform::RuntimeExit::Stopped(StopReason::Cancelled) => {
            info!("Cancellation requested, stopping incremental sync");
        }
    }

    info!(
        "MySQL incremental sync completed. Processed {} changes",
        driver.total_changes
    );

    source.cleanup().await?;
    Ok(())
}

struct MysqlTriggerSourceDriver<'a> {
    stream: Box<dyn super::source::ChangeStream>,
    options: &'a ReplicationTailOptions,
    until_reached: bool,
    finished: bool,
    total_changes: u64,
}

#[async_trait::async_trait]
impl SourceDriver for MysqlTriggerSourceDriver<'_> {
    type Position = i64;

    async fn poll_work(&mut self) -> Result<Vec<PositionedEvent<Self::Position>>> {
        if self.stop_reason().is_some() || self.finished {
            return Ok(Vec::new());
        }

        match self.stream.next_with_sequence_id().await {
            None => {
                self.finished = true;
                Ok(Vec::new())
            }
            Some(Err(e)) => {
                warn!("Error reading change stream: {}", e);
                self.finished = true;
                Ok(Vec::new())
            }
            Some(Ok((sequence_id, change))) => {
                debug!("Received change: {:?}", change);
                if let Some(ref target) = self.options.until {
                    if sequence_id >= target.sequence_id {
                        info!(
                            "Reached target checkpoint: {}, stopping incremental sync",
                            target.to_cli_string()
                        );
                        self.until_reached = true;
                        return Ok(Vec::new());
                    }
                }
                Ok(vec![PositionedEvent::change(change, sequence_id)])
            }
        }
    }

    async fn advance_watermark(&mut self, position: Self::Position) -> Result<()> {
        self.stream.commit_sunk(position);
        Ok(())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn checkpoint_policy(&self) -> CheckpointPolicy {
        CheckpointPolicy::AdvanceOnly
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if Utc::now() >= self.options.deadline {
            return Some(StopReason::Deadline);
        }
        if self.until_reached {
            return Some(StopReason::Until);
        }
        None
    }

    fn note_sunk_events(&mut self, count: u64) {
        self.total_changes = self.total_changes.saturating_add(count);
        if self.total_changes.is_multiple_of(100) {
            info!("Processed {} changes", self.total_changes);
        }
    }
}
