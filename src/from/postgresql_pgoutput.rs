//! PostgreSQL pgoutput WAL CDC sync handlers.
//!
//! Source crate: crates/postgresql-pgoutput-source/
//! CLI commands:
//! - `from postgresql-pgoutput sync` — snapshot and/or stream (see `--snapshot-mode`)
//! - `from postgresql-pgoutput snapshot` — ad-hoc execute-snapshot signal insert

use anyhow::Context;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager};
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::SnapshotTransforms;
use surreal_sync_postgresql_pgoutput_source::{
    request_snapshot, run_full_sync_cancellable_with_transforms,
    run_initial_interleaved_snapshot_with_transforms,
    run_interleaved_snapshot_full_sync_with_transforms, run_replication_tail_with_transforms,
    InterleavedFullSyncOptions, PgoutputCheckpoint, ReplicationTailOptions, SourceOpts, SyncOpts,
};
use sync_transform::{ApplyOpts, Pipeline};
use tokio_util::sync::CancellationToken;

use super::transforms::load_transforms_from_args;
use super::{
    get_sdk_version, make_surreal2_sink, make_surreal3_sink, parse_duration_to_secs, SdkVersion,
};
use crate::{
    BinlogSnapshotModeArg, PostgreSQLPgoutputSnapshotArgs, PostgreSQLPgoutputSyncArgs, SyncStrategy,
};

fn install_shutdown_token() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("Shutdown signal received; requesting graceful stop");
        child.cancel();
    });
    token
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn wal_source_opts(args: &PostgreSQLPgoutputSyncArgs) -> SourceOpts {
    SourceOpts {
        connection_string: args.connection_string.clone(),
        schema: args.schema.clone(),
        tables: args.tables.clone(),
        slot_name: args.slot.clone(),
        publication_name: args.publication.clone(),
    }
}

fn wal_sync_opts(batch_size: usize, dry_run: bool) -> SyncOpts {
    SyncOpts {
        batch_size,
        dry_run,
    }
}

fn parse_stop_after_deadline(
    raw: Option<&str>,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    Ok(match raw {
        Some(raw) => {
            let seconds = parse_duration_to_secs(raw)?;
            Some(chrono::Utc::now() + chrono::Duration::seconds(seconds))
        }
        None => None,
    })
}

fn wal_stream_options(args: &PostgreSQLPgoutputSyncArgs) -> anyhow::Result<ReplicationTailOptions> {
    let until_checkpoint = args
        .stop_at
        .as_ref()
        .map(|s| PgoutputCheckpoint::from_cli_string(s))
        .transpose()?;
    let deadline = parse_stop_after_deadline(args.stop_after.as_deref())?;
    let mut options = ReplicationTailOptions::stream(deadline, until_checkpoint);
    options.checkpoint_interval = std::time::Duration::from_secs(args.checkpoint_interval);
    options.chunk_size = args.chunk_size;
    options.idle_sleep = std::time::Duration::from_millis(args.idle_sleep_ms);
    options.event_batch_size = args.wal_event_batch_size;
    Ok(options)
}

async fn read_latest_replication_checkpoint<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> anyhow::Result<PgoutputCheckpoint> {
    use checkpoint::SyncPhase;
    use surreal_sync_postgresql_pgoutput_source::{max_pgoutput_checkpoint, CatchUpProgress};

    let catch_up = manager
        .read_checkpoint::<CatchUpProgress>(SyncPhase::CatchUpProgress)
        .await
        .ok();
    let full_sync_end = manager
        .read_checkpoint::<PgoutputCheckpoint>(SyncPhase::FullSyncEnd)
        .await
        .ok();

    match (catch_up, full_sync_end) {
        (Some(progress), Some(end)) => Ok(max_pgoutput_checkpoint(&[progress.position, end])),
        (Some(progress), None) => Ok(progress.position),
        (None, Some(end)) => Ok(end),
        (None, None) => manager
            .read_checkpoint(SyncPhase::FullSyncStart)
            .await
            .with_context(|| "No CatchUpProgress, FullSyncEnd, or FullSyncStart checkpoint found"),
    }
}

fn is_start_at_head(explicit: &Option<String>) -> bool {
    matches!(explicit.as_deref().map(str::trim), Some(s) if s.eq_ignore_ascii_case("head") || s.is_empty())
}

async fn checkpoint_from_arg_or_store<St: CheckpointStore>(
    explicit: &Option<String>,
    manager: Option<&SyncManager<St>>,
    source_opts: &SourceOpts,
) -> anyhow::Result<PgoutputCheckpoint> {
    if is_start_at_head(explicit) {
        tracing::info!("Starting stream at current WAL head");
        return surreal_sync_postgresql_pgoutput_source::capture_head_checkpoint(source_opts).await;
    }

    match (explicit, manager) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            PgoutputCheckpoint::from_cli_string(s)
        }
        (None, Some(manager)) => {
            tracing::info!("Reading latest checkpoint from configured checkpoint store");
            match read_latest_replication_checkpoint(manager).await {
                Ok(checkpoint) => Ok(checkpoint),
                Err(read_err) => {
                    tracing::info!(
                        "No checkpoint found in store ({read_err}); starting at current WAL head"
                    );
                    surreal_sync_postgresql_pgoutput_source::capture_head_checkpoint(source_opts)
                        .await
                }
            }
        }
        (None, None) => {
            anyhow::bail!("--from, --checkpoint-dir, or --checkpoints-surreal-table is required")
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn wal_snapshot_full<S, St>(
    sink: &S,
    source_opts: &SourceOpts,
    strategy: SyncStrategy,
    chunk_size: usize,
    cancel: CancellationToken,
    sync_opts: &SyncOpts,
    manager: Option<&SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> anyhow::Result<Option<surreal_sync_postgresql_pgoutput_source::InterleavedFullSyncOutcome>>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    match strategy {
        SyncStrategy::InterleavedSnapshot => {
            let outcome = run_interleaved_snapshot_full_sync_with_transforms(
                sink,
                source_opts,
                chunk_size,
                cancel,
                manager,
                InterleavedFullSyncOptions::default(),
                transforms,
            )
            .await?;
            if outcome.cancelled {
                tracing::info!(
                    "WAL watermark snapshot cancelled; resume from FullSyncStart: {}",
                    outcome.start.to_cli_string()
                );
                return Ok(None);
            }
            tracing::info!(
                "WAL watermark snapshot completed (final checkpoint: {})",
                outcome.end.to_cli_string()
            );
            Ok(Some(outcome))
        }
        SyncStrategy::SequentialSnapshot => {
            run_full_sync_cancellable_with_transforms(
                sink,
                source_opts,
                sync_opts,
                manager,
                &cancel,
                &transforms.pipeline,
                &transforms.apply_opts,
            )
            .await?;
            tracing::info!("Sequential PostgreSQL WAL full sync completed");
            Ok(None)
        }
    }
}

/// Run `from postgresql-pgoutput sync`.
pub async fn run_sync(args: PostgreSQLPgoutputSyncArgs) -> anyhow::Result<()> {
    // Fail-fast on bad transforms config / worker spawn before connecting.
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;
    let cancel = install_shutdown_token();
    match sdk_version {
        SdkVersion::V2 => run_sync_v2(args, cancel, pipeline, apply_opts).await,
        SdkVersion::V3 => run_sync_v3(args, cancel, pipeline, apply_opts).await,
    }
}

async fn run_sync_v2(
    args: PostgreSQLPgoutputSyncArgs,
    cancel: CancellationToken,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting PostgreSQL WAL sync to SurrealDB (SDK v2)");
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = make_surreal2_sink(surreal, args.surreal.zero_temporal);
    let checkpoint_dir = args.checkpoint_dir.clone();
    let checkpoints_surreal_table = args.checkpoints_surreal_table.clone();
    match (checkpoint_dir, checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(&dir));
            wal_orchestrate(&sink, args, cancel, Some(&manager), pipeline, apply_opts).await
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let manager =
                SyncManager::new(checkpoint::Surreal2Store::new(checkpoint_surreal, table));
            wal_orchestrate(&sink, args, cancel, Some(&manager), pipeline, apply_opts).await
        }
        (None, None) => {
            wal_orchestrate::<_, checkpoint::NullStore>(
                &sink, args, cancel, None, pipeline, apply_opts,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn run_sync_v3(
    args: PostgreSQLPgoutputSyncArgs,
    cancel: CancellationToken,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting PostgreSQL WAL sync to SurrealDB (SDK v3)");
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = make_surreal3_sink(surreal.clone(), args.surreal.zero_temporal);
    let checkpoint_dir = args.checkpoint_dir.clone();
    let checkpoints_surreal_table = args.checkpoints_surreal_table.clone();
    match (checkpoint_dir, checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(&dir));
            wal_orchestrate(&sink, args, cancel, Some(&manager), pipeline, apply_opts).await
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(checkpoint_surreal3::Surreal3Store::new(surreal, table));
            wal_orchestrate(&sink, args, cancel, Some(&manager), pipeline, apply_opts).await
        }
        (None, None) => {
            wal_orchestrate::<_, checkpoint::NullStore>(
                &sink, args, cancel, None, pipeline, apply_opts,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn wal_orchestrate<S, St>(
    sink: &S,
    args: PostgreSQLPgoutputSyncArgs,
    cancel: CancellationToken,
    checkpoint_manager: Option<&SyncManager<St>>,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let transforms = SnapshotTransforms {
        pipeline,
        apply_opts,
    };

    let snapshot_mode = args.snapshot_mode;
    let strategy = args.strategy;
    let chunk_size = args.chunk_size;
    let from_explicit = args.from.clone();
    let stream_options = wal_stream_options(&args)?.with_cancel(cancel.clone());

    let source_opts = wal_source_opts(&args);
    let sync_opts = wal_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match snapshot_mode {
        BinlogSnapshotModeArg::Only => {
            wal_snapshot_full(
                sink,
                &source_opts,
                strategy,
                chunk_size,
                cancel,
                &sync_opts,
                checkpoint_manager,
                &transforms,
            )
            .await?;
            Ok(())
        }
        BinlogSnapshotModeArg::Never => {
            let from_checkpoint =
                checkpoint_from_arg_or_store(&from_explicit, checkpoint_manager, &source_opts)
                    .await?;
            run_replication_tail_with_transforms(
                sink,
                source_opts,
                from_checkpoint,
                stream_options,
                checkpoint_manager,
                &transforms.pipeline,
                &transforms.apply_opts,
            )
            .await
        }
        BinlogSnapshotModeArg::Initial => {
            let interleaved_outcome = match strategy {
                SyncStrategy::InterleavedSnapshot => {
                    let initial = run_initial_interleaved_snapshot_with_transforms(
                        sink,
                        &source_opts,
                        chunk_size,
                        cancel.clone(),
                        checkpoint_manager,
                        &transforms,
                    )
                    .await?;
                    initial.sync_outcome
                }
                SyncStrategy::SequentialSnapshot => {
                    wal_snapshot_full(
                        sink,
                        &source_opts,
                        strategy,
                        chunk_size,
                        cancel.clone(),
                        &sync_opts,
                        checkpoint_manager,
                        &transforms,
                    )
                    .await?;
                    None
                }
            };

            if let Some(outcome) = interleaved_outcome {
                if outcome.cancelled {
                    tracing::info!(
                        "Sync cancelled during snapshot; not handing off to streaming. \
                         Resume re-snapshots and streams from FullSyncStart: {}",
                        outcome.start.to_cli_string()
                    );
                    return Ok(());
                }
                run_replication_tail_with_transforms(
                    sink,
                    source_opts,
                    outcome.end,
                    stream_options,
                    checkpoint_manager,
                    &transforms.pipeline,
                    &transforms.apply_opts,
                )
                .await
            } else if strategy == SyncStrategy::SequentialSnapshot {
                let from_checkpoint = match checkpoint_manager {
                    Some(manager) => read_latest_replication_checkpoint(manager).await?,
                    None => {
                        checkpoint_from_arg_or_store::<checkpoint::NullStore>(
                            &from_explicit,
                            None,
                            &source_opts,
                        )
                        .await?
                    }
                };
                run_replication_tail_with_transforms(
                    sink,
                    source_opts,
                    from_checkpoint,
                    stream_options,
                    checkpoint_manager,
                    &transforms.pipeline,
                    &transforms.apply_opts,
                )
                .await
            } else {
                Ok(())
            }
        }
    }
}

/// Emit an ad-hoc execute-snapshot signal so a running `sync` snapshots the
/// requested tables.
pub async fn run_snapshot_signal(args: PostgreSQLPgoutputSnapshotArgs) -> anyhow::Result<()> {
    let source_opts = SourceOpts {
        connection_string: args.connection_string,
        schema: "public".to_string(),
        tables: args.tables.clone(),
        slot_name: "surreal_sync_slot".to_string(),
        publication_name: "surreal_sync_pub".to_string(),
    };
    request_snapshot(&source_opts, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}
