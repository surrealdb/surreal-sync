//! MySQL/MariaDB binlog CDC sync handlers.
//!
//! Source crate: crates/mysql-binlog-source/
//! CLI commands:
//! - Full sync: `from mysql-binlog full --connection-string ... --tables ...`
//! - Incremental sync: `from mysql-binlog incremental --incremental-from mysql-binlog:...`
//! - Combined snapshot+stream: `from mysql-binlog sync ...`
//! - Ad-hoc snapshot signal: `from mysql-binlog snapshot ...`

use anyhow::Context;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sink::SurrealSink;
use surreal_sync_mysql_binlog_source::{
    request_snapshot, run_full_sync_cancellable, run_incremental_sync_with_checkpoints,
    run_interleaved_snapshot_full_sync, BinlogCheckpoint, IncrementalSyncOptions, SourceOpts,
    SyncOpts,
};
use tokio_util::sync::CancellationToken;

use super::{get_sdk_version, SdkVersion};
use crate::{
    MariaDbGtidStrictModeArg, MySQLBinlogFlavorArg, MySQLBinlogFullArgs,
    MySQLBinlogIncrementalArgs, MySQLBinlogSnapshotArgs, MySQLBinlogSyncArgs, SyncStrategy,
};

/// Create a cancellation token that fires on SIGINT/SIGTERM so full,
/// incremental, and interleaved sync can all stop gracefully and flush a
/// resumable checkpoint on the way out.
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

fn binlog_source_opts(args: &MySQLBinlogFullArgs) -> SourceOpts {
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

fn parse_timeout_deadline(
    raw: Option<&str>,
    default_seconds: Option<i64>,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    let seconds = match raw {
        Some(raw) => Some(
            raw.parse::<i64>()
                .with_context(|| format!("Invalid timeout format: {raw}"))?,
        ),
        None => default_seconds,
    };
    Ok(seconds.map(|seconds| chrono::Utc::now() + chrono::Duration::seconds(seconds)))
}

fn binlog_incremental_options(
    follow: bool,
    timeout: Option<&str>,
    until: Option<BinlogCheckpoint>,
    checkpoint_interval: u64,
) -> anyhow::Result<IncrementalSyncOptions> {
    let mut options = if follow {
        IncrementalSyncOptions::follow(parse_timeout_deadline(timeout, None)?)
    } else {
        IncrementalSyncOptions::batch(parse_timeout_deadline(timeout, Some(3600))?, until)
    };
    options.checkpoint_interval = std::time::Duration::from_secs(checkpoint_interval);
    Ok(options)
}

fn binlog_sync_stream_options(
    batch: bool,
    timeout: Option<&str>,
    until: Option<BinlogCheckpoint>,
    checkpoint_interval: u64,
) -> anyhow::Result<IncrementalSyncOptions> {
    let mut options = if batch {
        IncrementalSyncOptions::batch(parse_timeout_deadline(timeout, None)?, until)
    } else {
        IncrementalSyncOptions::follow(parse_timeout_deadline(timeout, None)?)
    };
    options.checkpoint_interval = std::time::Duration::from_secs(checkpoint_interval);
    Ok(options)
}

async fn read_latest_binlog_checkpoint<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> anyhow::Result<BinlogCheckpoint> {
    match manager.read_checkpoint(SyncPhase::FullSyncEnd).await {
        Ok(checkpoint) => Ok(checkpoint),
        Err(end_err) => manager
            .read_checkpoint(SyncPhase::FullSyncStart)
            .await
            .with_context(|| {
                format!(
                    "Failed to read FullSyncEnd checkpoint ({end_err}); also failed to read FullSyncStart"
                )
            }),
    }
}

/// True when `--incremental-from` explicitly requests starting at the current
/// master head (`head`, or an empty string meaning "start fresh from here").
fn is_start_at_head(explicit: &Option<String>) -> bool {
    matches!(explicit.as_deref().map(str::trim), Some(s) if s.eq_ignore_ascii_case("head") || s.is_empty())
}

async fn checkpoint_from_arg_or_store<St: CheckpointStore>(
    explicit: &Option<String>,
    manager: Option<&SyncManager<St>>,
    source_opts: &SourceOpts,
) -> anyhow::Result<BinlogCheckpoint> {
    // Explicit "start at head": resolve the server's current position so the
    // stream begins after everything already committed (a real, resumable
    // checkpoint — not a placeholder).
    if is_start_at_head(explicit) {
        tracing::info!("Starting incremental sync at current master head");
        return surreal_sync_mysql_binlog_source::capture_head_checkpoint(source_opts).await;
    }

    match (explicit, manager) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            BinlogCheckpoint::from_cli_string(s)
        }
        (None, Some(manager)) => {
            tracing::info!("Reading latest checkpoint from configured checkpoint store");
            // An empty/absent checkpoint store falls back to starting at head so
            // a first-ever run does not fail; a populated store resumes from it.
            match read_latest_binlog_checkpoint(manager).await {
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
            anyhow::bail!(
                "--incremental-from, --checkpoint-dir, or --checkpoints-surreal-table is required"
            )
        }
    }
}

/// Run MySQL binlog full sync, dispatching by strategy then SDK version.
pub async fn run_full(args: MySQLBinlogFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    let cancel = install_shutdown_token();
    match (args.strategy, sdk_version) {
        (SyncStrategy::SequentialSnapshot, SdkVersion::V2) => run_full_v2(args, cancel).await,
        (SyncStrategy::SequentialSnapshot, SdkVersion::V3) => run_full_v3(args, cancel).await,
        (SyncStrategy::InterleavedSnapshot, SdkVersion::V2) => {
            run_full_interleaved_snapshot_v2(args, cancel).await
        }
        (SyncStrategy::InterleavedSnapshot, SdkVersion::V3) => {
            run_full_interleaved_snapshot_v3(args, cancel).await
        }
    }
}

async fn run_full_v2(args: MySQLBinlogFullArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL binlog to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_opts = binlog_source_opts(&args);
    let sync_opts = binlog_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync_cancellable(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
                &cancel,
            )
            .await?;
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync_cancellable(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
                &cancel,
            )
            .await?;
        }
        (None, None) => {
            run_full_sync_cancellable::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
                &cancel,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_full_v3(args: MySQLBinlogFullArgs, cancel: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MySQL binlog to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

    let source_opts = binlog_source_opts(&args);
    let sync_opts = binlog_sync_opts(args.surreal.batch_size, args.surreal.dry_run);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync_cancellable(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
                &cancel,
            )
            .await?;
        }
        (None, Some(table)) => {
            let store = checkpoint_surreal3::Surreal3Store::new(surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            run_full_sync_cancellable(
                &sink,
                &source_opts,
                &sync_opts,
                Some(&sync_manager),
                &cancel,
            )
            .await?;
        }
        (None, None) => {
            run_full_sync_cancellable::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                &sync_opts,
                None,
                &cancel,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

/// Run MySQL binlog incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: MySQLBinlogIncrementalArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    let cancel = install_shutdown_token();
    match sdk_version {
        SdkVersion::V2 => run_incremental_v2(args, cancel).await,
        SdkVersion::V3 => run_incremental_v3(args, cancel).await,
    }
}

async fn run_incremental_v2(
    args: MySQLBinlogIncrementalArgs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL binlog to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let until_arg = args.until.as_ref().or(args.incremental_to.as_ref());
    if let Some(to) = until_arg {
        tracing::info!("Will stop at checkpoint: {}", to);
    }
    let until_checkpoint = until_arg
        .map(|s| BinlogCheckpoint::from_cli_string(s))
        .transpose()?;
    let options = binlog_incremental_options(
        args.follow,
        args.timeout.as_deref(),
        until_checkpoint,
        args.checkpoint_interval,
    )?
    .with_cancel(cancel);

    let source_opts = binlog_source_opts_from(BinlogSourceOptsInput {
        connection_string: args.connection_string.clone(),
        database: args.database.clone(),
        tables: args.tables.clone(),
        server_id: args.server_id,
        flavor: args.flavor,
        mariadb_gtid_strict_mode: args.mariadb_gtid_strict_mode,
        ssl: args.tls.ssl_mode(),
    });

    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            let from_checkpoint =
                checkpoint_from_arg_or_store(&args.incremental_from, Some(&manager), &source_opts)
                    .await?;
            run_incremental_sync_with_checkpoints(
                &sink,
                source_opts,
                from_checkpoint,
                options,
                Some(&manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let manager = SyncManager::new(checkpoint::Surreal2Store::new(
                checkpoint_surreal,
                table.clone(),
            ));
            let from_checkpoint =
                checkpoint_from_arg_or_store(&args.incremental_from, Some(&manager), &source_opts)
                    .await?;
            run_incremental_sync_with_checkpoints(
                &sink,
                source_opts,
                from_checkpoint,
                options,
                Some(&manager),
            )
            .await?;
        }
        (None, None) => {
            let from_checkpoint = checkpoint_from_arg_or_store::<checkpoint::NullStore>(
                &args.incremental_from,
                None,
                &source_opts,
            )
            .await?;
            run_incremental_sync_with_checkpoints::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                from_checkpoint,
                options,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(
    args: MySQLBinlogIncrementalArgs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MySQL binlog to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let until_arg = args.until.as_ref().or(args.incremental_to.as_ref());
    if let Some(to) = until_arg {
        tracing::info!("Will stop at checkpoint: {}", to);
    }
    let until_checkpoint = until_arg
        .map(|s| BinlogCheckpoint::from_cli_string(s))
        .transpose()?;
    let options = binlog_incremental_options(
        args.follow,
        args.timeout.as_deref(),
        until_checkpoint,
        args.checkpoint_interval,
    )?
    .with_cancel(cancel);

    let source_opts = binlog_source_opts_from(BinlogSourceOptsInput {
        connection_string: args.connection_string.clone(),
        database: args.database.clone(),
        tables: args.tables.clone(),
        server_id: args.server_id,
        flavor: args.flavor,
        mariadb_gtid_strict_mode: args.mariadb_gtid_strict_mode,
        ssl: args.tls.ssl_mode(),
    });

    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            let from_checkpoint =
                checkpoint_from_arg_or_store(&args.incremental_from, Some(&manager), &source_opts)
                    .await?;
            run_incremental_sync_with_checkpoints(
                &sink,
                source_opts,
                from_checkpoint,
                options,
                Some(&manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(checkpoint_surreal3::Surreal3Store::new(
                surreal,
                table.clone(),
            ));
            let from_checkpoint =
                checkpoint_from_arg_or_store(&args.incremental_from, Some(&manager), &source_opts)
                    .await?;
            run_incremental_sync_with_checkpoints(
                &sink,
                source_opts,
                from_checkpoint,
                options,
                Some(&manager),
            )
            .await?;
        }
        (None, None) => {
            let from_checkpoint = checkpoint_from_arg_or_store::<checkpoint::NullStore>(
                &args.incremental_from,
                None,
                &source_opts,
            )
            .await?;
            run_incremental_sync_with_checkpoints::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                from_checkpoint,
                options,
                None,
            )
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

// =============================================================================
// Interleaved snapshot strategy
// =============================================================================

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
    chunk_size: usize,
    cancel: CancellationToken,
    manager: Option<&SyncManager<St>>,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    // `run_interleaved_snapshot_full_sync` persists FullSyncStart (the streaming
    // lower bound) BEFORE the snapshot and FullSyncEnd on completion.
    let outcome =
        run_interleaved_snapshot_full_sync(sink, source_opts, chunk_size, cancel, manager).await?;
    if outcome.cancelled {
        tracing::info!(
            "Binlog watermark snapshot cancelled; resume from FullSyncStart: {}",
            outcome.start.to_cli_string()
        );
        return Ok(());
    }
    tracing::info!(
        "Binlog watermark snapshot full sync completed (final checkpoint: {})",
        outcome.end.to_cli_string()
    );
    Ok(())
}

async fn run_full_interleaved_snapshot_v2(
    args: MySQLBinlogFullArgs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot full sync from MySQL binlog to SurrealDB (SDK v2)"
    );
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);
    let source_opts = binlog_source_opts(&args);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, cancel, Some(&manager)).await
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let manager = SyncManager::new(checkpoint::Surreal2Store::new(
                checkpoint_surreal,
                table.clone(),
            ));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, cancel, Some(&manager)).await
        }
        (None, None) => {
            binlog_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                args.chunk_size,
                cancel,
                None,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn run_full_interleaved_snapshot_v3(
    args: MySQLBinlogFullArgs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot full sync from MySQL binlog to SurrealDB (SDK v3)"
    );
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());
    let source_opts = binlog_source_opts(&args);

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, cancel, Some(&manager)).await
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(checkpoint_surreal3::Surreal3Store::new(
                surreal,
                table.clone(),
            ));
            binlog_snapshot_full(&sink, &source_opts, args.chunk_size, cancel, Some(&manager)).await
        }
        (None, None) => {
            binlog_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                &source_opts,
                args.chunk_size,
                cancel,
                None,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

/// Run the combined `from mysql-binlog sync` orchestrator.
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
    tracing::info!("Starting interleaved snapshot sync from MySQL binlog to SurrealDB (SDK v2)");
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
    tracing::info!("Starting interleaved snapshot sync from MySQL binlog to SurrealDB (SDK v3)");
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
    let until_checkpoint = args
        .until
        .as_ref()
        .map(|s| BinlogCheckpoint::from_cli_string(s))
        .transpose()?;
    let stream_options = binlog_sync_stream_options(
        args.batch,
        args.timeout.as_deref(),
        until_checkpoint,
        args.checkpoint_interval,
    )?;
    let flavor = args.flavor;
    let server_id = args.server_id;
    let mariadb_gtid_strict_mode = args.mariadb_gtid_strict_mode;
    let ssl = args.tls.ssl_mode();
    let snapshot_opts = binlog_source_opts_from(BinlogSourceOptsInput {
        connection_string: args.connection_string.clone(),
        database: args.database.clone(),
        tables: args.tables.clone(),
        server_id,
        flavor,
        mariadb_gtid_strict_mode,
        ssl: ssl.clone(),
    });
    let incremental_opts = binlog_source_opts_from(BinlogSourceOptsInput {
        connection_string: args.connection_string,
        database: args.database,
        tables: args.tables,
        server_id,
        flavor,
        mariadb_gtid_strict_mode,
        ssl,
    });
    let chunk_size = args.chunk_size;

    // Snapshot phase: persists FullSyncStart (streaming lower bound) before any
    // rows and FullSyncEnd on completion, honoring cancellation throughout.
    let outcome = run_interleaved_snapshot_full_sync(
        sink,
        &snapshot_opts,
        chunk_size,
        cancel.clone(),
        checkpoint_manager,
    )
    .await?;

    if outcome.cancelled {
        tracing::info!(
            "Sync cancelled during snapshot; not handing off to streaming. \
             Resume re-snapshots and streams from FullSyncStart: {}",
            outcome.start.to_cli_string()
        );
        return Ok(());
    }

    // Streaming phase: resume from the snapshot's consistent end position, with
    // the same cancellation token so SIGINT/SIGTERM stops streaming gracefully.
    let stream_options = stream_options.with_cancel(cancel);
    run_incremental_sync_with_checkpoints(
        sink,
        incremental_opts,
        outcome.end,
        stream_options,
        checkpoint_manager,
    )
    .await
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
