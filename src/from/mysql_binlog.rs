//! MySQL/MariaDB binlog CDC sync handlers.
//!
//! Source crate: crates/mysql-binlog-source/
//! CLI commands:
//! - `from mysql-binlog sync` — snapshot and/or stream (see `--snapshot-mode`)
//! - `from mysql-binlog snapshot` — ad-hoc execute-snapshot signal insert

use anyhow::Context;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager};
use surreal_sink::SurrealSink;
use surreal_sync_mysql_binlog_source::{
    request_snapshot, run_full_sync_cancellable, run_initial_interleaved_snapshot,
    run_interleaved_snapshot_full_sync, run_replication_tail_with_checkpoints, BinlogCheckpoint,
    InterleavedFullSyncOptions, ReplicationTailOptions, SourceOpts, SyncOpts,
};
use tokio_util::sync::CancellationToken;

use super::{get_sdk_version, parse_duration_to_secs, SdkVersion};
use crate::{
    BinlogSnapshotModeArg, MariaDbGtidStrictModeArg, MySQLBinlogFlavorArg, MySQLBinlogSnapshotArgs,
    MySQLBinlogSyncArgs, SyncStrategy,
};

/// Create a cancellation token that fires on SIGINT/SIGTERM so snapshot and
/// stream phases can stop gracefully and flush a resumable checkpoint.
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

fn binlog_source_opts(args: &MySQLBinlogSyncArgs) -> SourceOpts {
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
    flavor: Option<MySQLBinlogFlavorArg>,
    mariadb_gtid_strict_mode: MariaDbGtidStrictModeArg,
    ssl: surreal_sync_mysql_binlog_source::SslMode,
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

fn binlog_stream_options(args: &MySQLBinlogSyncArgs) -> anyhow::Result<ReplicationTailOptions> {
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
    use checkpoint::SyncPhase;
    use surreal_sync_mysql_binlog_source::{max_binlog_checkpoint, CatchUpProgress};

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
        return surreal_sync_mysql_binlog_source::capture_head_checkpoint(source_opts).await;
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
                    surreal_sync_mysql_binlog_source::capture_head_checkpoint(source_opts).await
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

async fn binlog_snapshot_full<S, St>(
    sink: &S,
    source_opts: &SourceOpts,
    strategy: SyncStrategy,
    chunk_size: usize,
    cancel: CancellationToken,
    sync_opts: &SyncOpts,
    manager: Option<&SyncManager<St>>,
) -> anyhow::Result<Option<surreal_sync_mysql_binlog_source::InterleavedFullSyncOutcome>>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    match strategy {
        SyncStrategy::InterleavedSnapshot => {
            let outcome = run_interleaved_snapshot_full_sync(
                sink,
                source_opts,
                chunk_size,
                cancel,
                manager,
                InterleavedFullSyncOptions::default(),
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
            run_full_sync_cancellable(sink, source_opts, sync_opts, manager, &cancel).await?;
            tracing::info!("Sequential binlog full sync completed");
            Ok(None)
        }
    }
}

/// Run `from mysql-binlog sync`.
pub async fn run_sync(args: MySQLBinlogSyncArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;
    let cancel = install_shutdown_token();
    match sdk_version {
        SdkVersion::V2 => run_sync_v2(args, cancel).await,
        SdkVersion::V3 => run_sync_v3(args, cancel).await,
    }
}

async fn run_sync_v2(args: MySQLBinlogSyncArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("Starting MySQL binlog sync to SurrealDB (SDK v2)");
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);
    let checkpoint_dir = args.checkpoint_dir.clone();
    let checkpoints_surreal_table = args.checkpoints_surreal_table.clone();
    match (checkpoint_dir, checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(&dir));
            binlog_orchestrate(&sink, args, cancel, Some(&manager)).await
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
            binlog_orchestrate(&sink, args, cancel, Some(&manager)).await
        }
        (None, None) => {
            binlog_orchestrate::<_, checkpoint::NullStore>(&sink, args, cancel, None).await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn run_sync_v3(args: MySQLBinlogSyncArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("Starting MySQL binlog sync to SurrealDB (SDK v3)");
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());
    let checkpoint_dir = args.checkpoint_dir.clone();
    let checkpoints_surreal_table = args.checkpoints_surreal_table.clone();
    match (checkpoint_dir, checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(&dir));
            binlog_orchestrate(&sink, args, cancel, Some(&manager)).await
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(checkpoint_surreal3::Surreal3Store::new(surreal, table));
            binlog_orchestrate(&sink, args, cancel, Some(&manager)).await
        }
        (None, None) => {
            binlog_orchestrate::<_, checkpoint::NullStore>(&sink, args, cancel, None).await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn binlog_orchestrate<S, St>(
    sink: &S,
    args: MySQLBinlogSyncArgs,
    cancel: CancellationToken,
    checkpoint_manager: Option<&SyncManager<St>>,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let snapshot_mode = args.snapshot_mode;
    let strategy = args.strategy;
    let chunk_size = args.chunk_size;
    let from_explicit = args.from.clone();
    let stream_options = binlog_stream_options(&args)?.with_cancel(cancel.clone());

    let source_opts = binlog_source_opts(&args);
    let sync_opts = binlog_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match snapshot_mode {
        BinlogSnapshotModeArg::Only => {
            binlog_snapshot_full(
                sink,
                &source_opts,
                strategy,
                chunk_size,
                cancel,
                &sync_opts,
                checkpoint_manager,
            )
            .await?;
            Ok(())
        }
        BinlogSnapshotModeArg::Never => {
            let from_checkpoint =
                checkpoint_from_arg_or_store(&from_explicit, checkpoint_manager, &source_opts)
                    .await?;
            run_replication_tail_with_checkpoints(
                sink,
                source_opts,
                from_checkpoint,
                stream_options,
                checkpoint_manager,
            )
            .await
        }
        BinlogSnapshotModeArg::Initial => {
            let interleaved_outcome = match strategy {
                SyncStrategy::InterleavedSnapshot => {
                    let initial = run_initial_interleaved_snapshot(
                        sink,
                        &source_opts,
                        chunk_size,
                        cancel.clone(),
                        checkpoint_manager,
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
                run_replication_tail_with_checkpoints(
                    sink,
                    source_opts,
                    outcome.end,
                    stream_options,
                    checkpoint_manager,
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
                run_replication_tail_with_checkpoints(
                    sink,
                    source_opts,
                    from_checkpoint,
                    stream_options,
                    checkpoint_manager,
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
pub async fn run_snapshot_signal(args: MySQLBinlogSnapshotArgs) -> anyhow::Result<()> {
    let pool = mysql_async::Pool::from_url(&args.connection_string)?;
    let database = resolve_mysql_database(&pool, &args.database).await?;
    request_snapshot(&pool, &database, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}
