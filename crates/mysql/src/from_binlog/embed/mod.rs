//! MySQL/MariaDB binlog CDC — library entrypoint for stock CLI and embedders.
//!
//! # Embedder usage
//!
//! Pick `surreal-sync-surreal` with feature `v3` (or `v2`),
//! define transforms, and call [`run`]:
//!
//! ```ignore
//! use surreal_sync_mysql::from_binlog::{run, FlattenId, InPlaceTransform, Value};
//! use surreal_sync_surreal::Surreal3Sink;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     run::<Surreal3Sink>([
//!         Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
//!         // …
//!     ]).await
//! }
//! ```
//!
//! `run` parses source-shaped argv (`sync` | `snapshot` + the same flags as
//! `surreal-sync from mysql-binlog`), connects the sink type, and chooses where
//! to store checkpoints from `--checkpoint-dir` / `--checkpoints-surreal-table`
//! (MySQL binlog CDC only).
//!
//! For embedding, use only `run`, `FlattenId`, `InPlaceTransform`, and `Value`
//! (re-exported at `surreal_sync_mysql::from_binlog`). Clap args / `run_sync`
//! stay crate-internal (`from_binlog::cli` for the stock CLI). See
//! `examples/from-mysql-binlog`.

mod args;

pub use args::{
    Commands, FlavorArg, MariaDbGtidStrictModeArg, SnapshotArgs, SnapshotModeArg, SyncArgs,
    SyncStrategy, TlsArgs, DEFAULT_CHUNK_SIZE,
};

use anyhow::Context;
use clap::Parser;
use surreal_sync_core::{Checkpoint, CheckpointStore, NullStore, SurrealSink, SyncManager};
use surreal_sync_runtime::checkpoint_fs::FilesystemStore;
use surreal_sync_runtime::{init, merge_inplace_boxed, parse_duration_to_secs};
use surreal_sync_runtime::{ApplyOpts, SnapshotTransforms};
use tokio_util::sync::CancellationToken;

use crate::from_binlog::{
    new_mysql_pool_with_ssl, request_snapshot, run_full_sync_cancellable_with_transforms,
    run_initial_interleaved_snapshot_with_transforms,
    run_interleaved_snapshot_full_sync_with_transforms, run_replication_tail_with_transforms,
    BinlogCheckpoint, InterleavedFullSyncOptions, ReplicationTailOptions, SourceOpts, SyncOpts,
};

pub use surreal_sync_core::Value;
pub use surreal_sync_runtime::SinkWithCheckpoints;
pub use surreal_sync_runtime::{FlattenId, InPlaceTransform, Pipeline};

/// Create a cancellation token that fires on SIGINT/SIGTERM so snapshot and
/// stream phases can stop gracefully and flush a resumable checkpoint.
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

fn binlog_source_opts(args: &SyncArgs) -> SourceOpts {
    binlog_source_opts_from(BinlogSourceOptsInput {
        connection_string: args.connection_string.clone(),
        database: args.database.clone(),
        tables: args.tables.clone(),
        server_id: args.server_id,
        flavor: args.flavor,
        mariadb_gtid_strict_mode: args.mariadb_gtid_strict_mode,
        ssl: args.tls.ssl_mode(),
    })
}

struct BinlogSourceOptsInput {
    connection_string: String,
    database: Option<String>,
    tables: Vec<String>,
    server_id: Option<u32>,
    flavor: Option<FlavorArg>,
    mariadb_gtid_strict_mode: MariaDbGtidStrictModeArg,
    ssl: crate::from_binlog::SslMode,
}

fn binlog_source_opts_from(input: BinlogSourceOptsInput) -> SourceOpts {
    SourceOpts {
        connection_string: input.connection_string,
        database: input.database,
        tables: input.tables,
        server_id: input.server_id,
        flavor: input.flavor.map(Into::into),
        ssl: input.ssl,
        mariadb_gtid_strict_mode: input.mariadb_gtid_strict_mode.into(),
    }
}

fn binlog_sync_opts(batch_size: usize, dry_run: bool) -> SyncOpts {
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

fn binlog_stream_options(args: &SyncArgs) -> anyhow::Result<ReplicationTailOptions> {
    let until_checkpoint = args
        .stop_at
        .as_ref()
        .map(|s| BinlogCheckpoint::from_cli_string(s))
        .transpose()?;
    let deadline = parse_stop_after_deadline(args.stop_after.as_deref())?;
    let mut options = ReplicationTailOptions::stream(deadline, until_checkpoint);
    options.checkpoint_interval = std::time::Duration::from_secs(args.checkpoint_interval);
    options.chunk_size = args.chunk_size;
    options.binlog_poll_timeout = std::time::Duration::from_millis(args.binlog_poll_timeout_ms);
    options.idle_sleep = std::time::Duration::from_millis(args.idle_sleep_ms);
    options.event_batch_size = args.binlog_event_batch_size;
    Ok(options)
}

async fn read_latest_replication_checkpoint<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> anyhow::Result<BinlogCheckpoint> {
    use crate::from_binlog::{max_binlog_checkpoint, CatchUpProgress};
    use surreal_sync_core::SyncPhase;

    let catch_up = manager
        .read_checkpoint::<CatchUpProgress>(SyncPhase::CatchUpProgress)
        .await
        .ok();
    let full_sync_end = manager
        .read_checkpoint::<BinlogCheckpoint>(SyncPhase::FullSyncEnd)
        .await
        .ok();

    match (catch_up, full_sync_end) {
        (Some(progress), Some(end)) => Ok(max_binlog_checkpoint(&[progress.position, end])),
        (Some(progress), None) => Ok(progress.position),
        (None, Some(end)) => Ok(end),
        (None, None) => manager
            .read_checkpoint(SyncPhase::FullSyncStart)
            .await
            .with_context(|| "No CatchUpProgress, FullSyncEnd, or FullSyncStart checkpoint found"),
    }
}

/// True when `--from` explicitly requests starting at the current master head.
fn is_start_at_head(explicit: &Option<String>) -> bool {
    matches!(explicit.as_deref().map(str::trim), Some(s) if s.eq_ignore_ascii_case("head") || s.is_empty())
}

async fn checkpoint_from_arg_or_store<St: CheckpointStore>(
    explicit: &Option<String>,
    manager: Option<&SyncManager<St>>,
    source_opts: &SourceOpts,
) -> anyhow::Result<BinlogCheckpoint> {
    if is_start_at_head(explicit) {
        tracing::info!("Starting stream at current master head");
        return crate::from_binlog::capture_head_checkpoint(source_opts).await;
    }

    match (explicit, manager) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            BinlogCheckpoint::from_cli_string(s)
        }
        (None, Some(manager)) => {
            tracing::info!("Reading latest checkpoint from configured checkpoint store");
            match read_latest_replication_checkpoint(manager).await {
                Ok(checkpoint) => Ok(checkpoint),
                Err(read_err) => {
                    tracing::info!(
                        "No checkpoint found in store ({read_err}); starting at current master head"
                    );
                    crate::from_binlog::capture_head_checkpoint(source_opts).await
                }
            }
        }
        (None, None) => {
            anyhow::bail!("--from, --checkpoint-dir, or --checkpoints-surreal-table is required")
        }
    }
}

async fn resolve_mysql_database(
    pool: &mysql_async::Pool,
    explicit: &Option<String>,
) -> anyhow::Result<String> {
    if let Some(db) = explicit {
        return Ok(db.clone());
    }
    let mut conn = pool.get_conn().await?;
    let current: Option<String> =
        mysql_async::prelude::Queryable::query_first(&mut conn, "SELECT DATABASE()")
            .await?
            .flatten();
    current.ok_or_else(|| anyhow::anyhow!("No MySQL database selected; pass --database"))
}

#[allow(clippy::too_many_arguments)]
async fn binlog_snapshot_full<S, St>(
    sink: &S,
    source_opts: &SourceOpts,
    strategy: SyncStrategy,
    chunk_size: usize,
    cancel: CancellationToken,
    sync_opts: &SyncOpts,
    manager: Option<&SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> anyhow::Result<Option<crate::from_binlog::InterleavedFullSyncOutcome>>
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
                    "Binlog watermark snapshot cancelled; resume from FullSyncStart: {}",
                    outcome.start.to_cli_string()
                );
                return Ok(None);
            }
            tracing::info!(
                "Binlog watermark snapshot completed (final checkpoint: {})",
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
            tracing::info!("Sequential binlog full sync completed");
            Ok(None)
        }
    }
}

/// Run sync with a pre-built sink; checkpoint backend is selected from args.
///
/// - `--checkpoint-dir` → [`FilesystemStore`]
/// - `--checkpoints-surreal-table` → `S::CheckpointStore` via [`SinkWithCheckpoints`]
/// - neither → no durable store (`None` manager)
///
/// Stock CLI and embed [`run`] share this path after connecting a concrete sink.
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
        binlog_orchestrate(sink, args, cancel, Some(&manager), pipeline, apply_opts).await
    } else if let Some(table) = args.checkpoints_surreal_table.clone() {
        let store = sink.table_checkpoints(table);
        let manager = SyncManager::new(store);
        binlog_orchestrate(sink, args, cancel, Some(&manager), pipeline, apply_opts).await
    } else {
        binlog_orchestrate::<_, NullStore>(sink, args, cancel, None, pipeline, apply_opts).await
    }
}

/// Run with an explicit checkpoint manager (custom stores, tests).
///
/// Prefer [`run_sync`] / [`run`] when flags should select the backend.
#[allow(dead_code)]
pub async fn run_sync_with_manager<S: SurrealSink, St: CheckpointStore>(
    args: SyncArgs,
    sink: &S,
    checkpoint_manager: Option<&SyncManager<St>>,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    let cancel = install_shutdown_token();
    binlog_orchestrate(sink, args, cancel, checkpoint_manager, pipeline, apply_opts).await
}

/// Load `--transforms-config` (if any), append Rust [`InPlaceTransform`] stages,
/// apply the ApplyOpts upgrade rule, connect `S`, then [`run_sync`].
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

/// Top-level clap root for source-shaped argv (`sync|snapshot …`).
#[derive(Parser)]
#[command(
    name = "surreal-sync-mysql-binlog",
    about = "Embeddable MySQL/MariaDB binlog sync (same flags as `surreal-sync from mysql-binlog`)"
)]
struct EmbedCli {
    #[command(subcommand)]
    command: Commands,
}

/// Parses CLI args, connects the sink type you pass (e.g. `Surreal3Sink`), and
/// sets up checkpoints from flags.
///
/// Pass one sink type: `run::<Surreal3Sink>(…)` or `run::<Surreal2Sink>(…)`.
/// Does not auto-detect SurrealDB major version (the CLI binary does).
pub async fn run<S: SinkWithCheckpoints>(
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<()> {
    init();
    let cli = EmbedCli::parse();
    match cli.command {
        Commands::Sync(args) => run_sync_with_extra_transforms::<S>(*args, extra).await,
        Commands::Snapshot(args) => run_snapshot_signal(args).await,
    }
}

async fn binlog_orchestrate<S, St>(
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
    let from_explicit = args.from.clone();
    let stream_options = binlog_stream_options(&args)?.with_cancel(cancel.clone());

    let source_opts = binlog_source_opts(&args);
    let sync_opts = binlog_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match snapshot_mode {
        SnapshotModeArg::Only => {
            binlog_snapshot_full(
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
        SnapshotModeArg::Never => {
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
                    binlog_snapshot_full(
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
                        checkpoint_from_arg_or_store::<surreal_sync_core::NullStore>(
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
pub async fn run_snapshot_signal(args: SnapshotArgs) -> anyhow::Result<()> {
    let pool = new_mysql_pool_with_ssl(&args.connection_string, &args.tls.ssl_mode()).await?;
    let database = resolve_mysql_database(&pool, &args.database).await?;
    request_snapshot(&pool, &database, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}
