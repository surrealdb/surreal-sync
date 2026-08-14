//! SQL Server CDC — library entrypoint for stock CLI and embedders.
//!
//! Pick `surreal-sync-surreal` with feature `v3` (or `v2`), define transforms,
//! and call [`run`]:
//!
//! ```ignore
//! use surreal_sync_mssql::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     run::<Surreal3Sink>([
//!         Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
//!     ]).await
//! }
//! ```
//!
//! `run` parses source-shaped argv (`sync` + the same flags as
//! `surreal-sync from mssql`).

mod args;

pub use args::{Commands, SnapshotModeArg, SyncArgs, SyncStrategy, DEFAULT_CHUNK_SIZE};

use anyhow::Context;
use clap::Parser;
use surreal_sync_core::{Checkpoint, CheckpointStore, NullStore, SurrealSink, SyncManager};
use surreal_sync_runtime::checkpoint_fs::FilesystemStore;
use surreal_sync_runtime::{init, merge_inplace_boxed, parse_duration_to_secs};
use surreal_sync_runtime::{ApplyOpts, SnapshotTransforms};
use tokio_util::sync::CancellationToken;

use crate::from_mssql::{
    run_initial_interleaved_snapshot_with_transforms,
    run_interleaved_snapshot_full_sync_with_transforms, run_replication_tail_with_transforms,
    run_sequential_snapshot_with_transforms, InterleavedFullSyncOptions, MssqlCheckpoint,
    ReplicationTailOptions, SourceOpts, SyncOpts,
};

pub use surreal_sync_core::Value;
pub use surreal_sync_runtime::SinkWithCheckpoints;
pub use surreal_sync_runtime::{FlattenId, InPlaceTransform, Pipeline};

/// Create a cancellation token that fires on SIGINT/SIGTERM.
pub fn install_shutdown_token() -> CancellationToken {
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

fn source_opts(args: &SyncArgs) -> SourceOpts {
    SourceOpts {
        connection_string: args.connection_string.clone(),
        tables: args.tables.clone(),
        relation_tables: args.relation_tables.clone(),
        schemafull: args.schemafull,
        dry_run: args.surreal.dry_run,
    }
}

fn sync_opts(args: &SyncArgs) -> SyncOpts {
    SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schemafull: args.schemafull,
    }
}

fn parse_timeout(raw: Option<&str>) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    Ok(match raw {
        Some(raw) => {
            let seconds = parse_duration_to_secs(raw)?;
            Some(chrono::Utc::now() + chrono::Duration::seconds(seconds))
        }
        None => None,
    })
}

fn tail_options(
    args: &SyncArgs,
    cancel: CancellationToken,
) -> anyhow::Result<ReplicationTailOptions> {
    let deadline = parse_timeout(args.timeout.as_deref())?;
    Ok(ReplicationTailOptions::stream(deadline, None).with_cancel(cancel))
}

async fn read_latest_replication_checkpoint<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> anyhow::Result<MssqlCheckpoint> {
    use surreal_sync_core::SyncPhase;
    if let Ok(end) = manager
        .read_checkpoint::<MssqlCheckpoint>(SyncPhase::FullSyncEnd)
        .await
    {
        return Ok(end);
    }
    manager
        .read_checkpoint(SyncPhase::FullSyncStart)
        .await
        .with_context(|| "No FullSyncEnd or FullSyncStart checkpoint found")
}

/// Run sync with a pre-built sink; checkpoint backend is selected from args.
pub async fn run_sync<S: SinkWithCheckpoints>(
    args: SyncArgs,
    sink: &S,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    if args.checkpoint_dir.is_some() && args.checkpoints_surreal_table.is_some() {
        anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table");
    }

    let cancel = install_shutdown_token();

    if let Some(dir) = args.checkpoint_dir.clone() {
        let manager = SyncManager::new(FilesystemStore::new(&dir));
        orchestrate(sink, args, cancel, Some(&manager), pipeline, apply_opts).await
    } else if let Some(table) = args.checkpoints_surreal_table.clone() {
        let store = sink.table_checkpoints(table);
        let manager = SyncManager::new(store);
        orchestrate(sink, args, cancel, Some(&manager), pipeline, apply_opts).await
    } else {
        orchestrate::<_, NullStore>(sink, args, cancel, None, pipeline, apply_opts).await
    }
}

/// Load `--transforms-config` (if any), append Rust [`InPlaceTransform`] stages,
/// connect `S`, then [`run_sync`].
pub async fn run_sync_with_extra_transforms<S: SinkWithCheckpoints>(
    args: SyncArgs,
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = merge_inplace_boxed(args.transforms_config.as_deref(), extra)?;
    let config = args
        .surreal
        .to_config(args.to_namespace.clone(), args.to_database.clone());
    let sink = S::connect(&config).await?;
    run_sync(args, &sink, pipeline, apply_opts).await
}

/// Run with parsed [`SyncArgs`] and only TOML transforms.
pub async fn run_args_with_sink<S: SurrealSink>(args: SyncArgs, sink: &S) -> anyhow::Result<()> {
    let (pipeline, apply_opts) =
        surreal_sync_runtime::load_transforms_from_args(args.transforms_config.as_deref())?;
    let cancel = install_shutdown_token();
    orchestrate::<_, NullStore>(sink, args, cancel, None, pipeline, apply_opts).await
}

#[derive(Parser)]
#[command(
    name = "surreal-sync-mssql",
    about = "Embeddable SQL Server sync (same flags as `surreal-sync from mssql`)"
)]
struct EmbedCli {
    #[command(subcommand)]
    command: Commands,
}

/// Parses CLI args, connects the sink type you pass, and sets up checkpoints.
pub async fn run<S: SinkWithCheckpoints>(
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    init();
    let cli = EmbedCli::parse();
    match cli.command {
        Commands::Sync(args) => run_sync_with_extra_transforms::<S>(*args, extra).await,
    }
}

async fn orchestrate<S, St>(
    sink: &S,
    args: SyncArgs,
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
    let source_opts = source_opts(&args);
    let sync_opts = sync_opts(&args);
    let stream_options = tail_options(&args, cancel.clone())?;

    match snapshot_mode {
        SnapshotModeArg::Only => {
            snapshot_full(
                sink,
                &source_opts,
                &sync_opts,
                strategy,
                chunk_size,
                cancel,
                checkpoint_manager,
                &transforms,
            )
            .await?;
            Ok(())
        }
        SnapshotModeArg::Never => {
            let from_checkpoint = match checkpoint_manager {
                Some(manager) => read_latest_replication_checkpoint(manager).await?,
                None => anyhow::bail!(
                    "--checkpoint-dir or --checkpoints-surreal-table is required with --snapshot-mode never"
                ),
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
        }
        SnapshotModeArg::Initial => {
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
                    let checkpoint = run_sequential_snapshot_with_transforms(
                        sink,
                        &source_opts,
                        &sync_opts,
                        chunk_size,
                        &cancel,
                        checkpoint_manager,
                        &transforms.pipeline,
                        &transforms.apply_opts,
                    )
                    .await?;
                    if cancel.is_cancelled() {
                        tracing::info!(
                            "Sync cancelled during SNAPSHOT dump; not handing off to streaming. \
                             Resume from FullSyncStart: {}",
                            checkpoint.to_cli_string()
                        );
                        return Ok(());
                    }
                    return run_replication_tail_with_transforms(
                        sink,
                        source_opts,
                        checkpoint,
                        stream_options,
                        checkpoint_manager,
                        &transforms.pipeline,
                        &transforms.apply_opts,
                    )
                    .await;
                }
            };

            if let Some(outcome) = interleaved_outcome {
                if outcome.cancelled {
                    tracing::info!(
                        "Sync cancelled during snapshot; not handing off to streaming. \
                         Resume from FullSyncStart: {}",
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
            } else {
                Ok(())
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn snapshot_full<S, St>(
    sink: &S,
    source_opts: &SourceOpts,
    sync_opts: &SyncOpts,
    strategy: SyncStrategy,
    chunk_size: usize,
    cancel: CancellationToken,
    manager: Option<&SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> anyhow::Result<Option<crate::from_mssql::InterleavedFullSyncOutcome>>
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
                    "SQL Server watermark snapshot cancelled; resume from FullSyncStart: {}",
                    outcome.start.to_cli_string()
                );
                return Ok(None);
            }
            tracing::info!(
                "SQL Server watermark snapshot completed (final checkpoint: {})",
                outcome.end.to_cli_string()
            );
            Ok(Some(outcome))
        }
        SyncStrategy::SequentialSnapshot => {
            let checkpoint = run_sequential_snapshot_with_transforms(
                sink,
                source_opts,
                sync_opts,
                chunk_size,
                &cancel,
                manager,
                &transforms.pipeline,
                &transforms.apply_opts,
            )
            .await?;
            tracing::info!(
                "Sequential SNAPSHOT-isolation dump completed (LSN {})",
                checkpoint.to_cli_string()
            );
            Ok(None)
        }
    }
}
