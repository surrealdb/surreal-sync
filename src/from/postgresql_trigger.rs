//! PostgreSQL trigger-based CDC sync handlers.
//!
//! Source crate: crates/postgresql-trigger-source/
//! CLI commands:
//! - Full sync: `from postgresql-trigger full --connection-string ... --tables ... --checkpoints-surreal-table ...`
//! - Incremental sync: `from postgresql-trigger incremental --connection-string ... --tables ... --checkpoints-surreal-table ...`
//!
//! Both commands support `-c` / `--config-file` to load settings from a TOML file:
//! - Full sync: `from postgresql-trigger full -c surreal-sync.toml`
//! - Incremental sync: `from postgresql-trigger incremental -c surreal-sync.toml`
//!
//! CLI flags take precedence over config file values when both are provided.

use anyhow::Context;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use std::path::PathBuf;
use surreal_sink::SurrealSink;
use surreal_sync::{orchestrate_snapshot_then_incremental, SurrealOpts};
use surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint;
use surreal_sync_snapshot_stream::{NoopCheckpointer, SnapshotStreamConfig};

use super::{extract_postgresql_database, get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::config::load_config;
use crate::{
    PostgreSQLTriggerFullArgs, PostgreSQLTriggerIncrementalArgs, PostgreSQLTriggerSnapshotArgs,
    PostgreSQLTriggerSyncArgs, SyncStrategy,
};
use surreal_sync_postgresql_trigger_source::toml_config::{
    TriggerFullSource, TriggerIncrementalSource,
};

// ---- Resolved args (all required fields are non-optional) ----

struct ResolvedTriggerFullArgs {
    connection_string: String,
    tables: Vec<String>,
    to_namespace: String,
    to_database: String,
    checkpoint_dir: Option<String>,
    checkpoints_surreal_table: Option<String>,
    schema_file: Option<PathBuf>,
    strategy: SyncStrategy,
    chunk_size: usize,
    surreal: SurrealOpts,
}

struct ResolvedTriggerIncrementalArgs {
    connection_string: String,
    tables: Vec<String>,
    to_namespace: String,
    to_database: String,
    incremental_from: Option<String>,
    checkpoints_surreal_table: Option<String>,
    incremental_to: Option<String>,
    timeout: String,
    schema_file: Option<PathBuf>,
    surreal: SurrealOpts,
}

fn resolve_full_args(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<ResolvedTriggerFullArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<TriggerFullSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedTriggerFullArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            tables: if args.tables.is_empty() {
                pg.tables
            } else {
                args.tables
            },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            checkpoint_dir: args.checkpoint_dir.or(pg.checkpoint_dir),
            checkpoints_surreal_table: args
                .checkpoints_surreal_table
                .or(pg.checkpoints_surreal_table),
            schema_file: args.schema_file.or(pg.schema_file),
            strategy: args.strategy,
            chunk_size: args.chunk_size,
            surreal: SurrealOpts {
                surreal_endpoint: sink.endpoint,
                surreal_username: sink.username,
                surreal_password: sink.password,
                batch_size: sink.batch_size,
                dry_run: sink.dry_run,
                surreal_sdk_version: args.surreal.surreal_sdk_version.or(sink.sdk_version),
            },
        })
    } else {
        Ok(ResolvedTriggerFullArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            tables: args.tables,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            checkpoint_dir: args.checkpoint_dir,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            schema_file: args.schema_file,
            strategy: args.strategy,
            chunk_size: args.chunk_size,
            surreal: args.surreal,
        })
    }
}

fn resolve_incremental_args(
    args: PostgreSQLTriggerIncrementalArgs,
) -> anyhow::Result<ResolvedTriggerIncrementalArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<TriggerIncrementalSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedTriggerIncrementalArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            tables: if args.tables.is_empty() {
                pg.tables
            } else {
                args.tables
            },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            incremental_from: args.incremental_from.or(pg.incremental_from),
            checkpoints_surreal_table: args
                .checkpoints_surreal_table
                .or(pg.checkpoints_surreal_table),
            incremental_to: args.incremental_to.or(pg.incremental_to),
            timeout: if args.timeout != "3600" {
                args.timeout
            } else {
                pg.timeout.to_string()
            },
            schema_file: args.schema_file.or(pg.schema_file),
            surreal: SurrealOpts {
                surreal_endpoint: sink.endpoint,
                surreal_username: sink.username,
                surreal_password: sink.password,
                batch_size: sink.batch_size,
                dry_run: sink.dry_run,
                surreal_sdk_version: args.surreal.surreal_sdk_version.or(sink.sdk_version),
            },
        })
    } else {
        Ok(ResolvedTriggerIncrementalArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            tables: args.tables,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            incremental_from: args.incremental_from,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            incremental_to: args.incremental_to,
            timeout: args.timeout,
            schema_file: args.schema_file,
            surreal: args.surreal,
        })
    }
}

// ---- Public entry points ----

/// Run PostgreSQL trigger-based full sync, dispatching by strategy then SDK version.
pub async fn run_full(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    let args = resolve_full_args(args)?;
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match (args.strategy, sdk_version) {
        (SyncStrategy::Bulk, SdkVersion::V2) => run_full_v2(args).await,
        (SyncStrategy::Bulk, SdkVersion::V3) => run_full_v3(args).await,
        (SyncStrategy::SnapshotStream, SdkVersion::V2) => run_full_snapshot_stream_v2(args).await,
        (SyncStrategy::SnapshotStream, SdkVersion::V3) => run_full_snapshot_stream_v3(args).await,
    }
}

async fn run_full_v2(args: ResolvedTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
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
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
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

async fn run_full_v3(args: ResolvedTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = checkpoint::FilesystemStore::new(dir);
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal3_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint_surreal3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            surreal_sync_postgresql_trigger_source::run_full_sync(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
            )
            .await?;
        }
        (None, None) => {
            surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                sync_opts,
                None,
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

// =============================================================================
// Watermark snapshot+stream strategy
// =============================================================================

fn trigger_source_opts(
    connection_string: &str,
    tables: Vec<String>,
) -> surreal_sync_postgresql_trigger_source::SourceOpts {
    let source_database = extract_postgresql_database(connection_string);
    surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: connection_string.to_string(),
        source_database,
        tables,
        relation_tables: vec![],
    }
}

/// Run a PostgreSQL trigger watermark snapshot+stream full sync, emitting the
/// handoff position as a checkpoint (when checkpoint storage is configured) so
/// a later `incremental` run can resume from the consistent end position.
async fn pg_trigger_snapshot_full<S, St>(
    sink: &S,
    source_opts: surreal_sync_postgresql_trigger_source::SourceOpts,
    chunk_size: usize,
    manager: Option<&SyncManager<St>>,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let config = SnapshotStreamConfig { chunk_size };
    let mut checkpointer = NoopCheckpointer;
    let final_seq = surreal_sync_postgresql_trigger_source::run_snapshot_stream_full_sync(
        sink,
        &source_opts,
        &config,
        &mut checkpointer,
    )
    .await?;

    if let Some(manager) = manager {
        let checkpoint = PostgreSQLCheckpoint {
            sequence_id: final_seq,
            timestamp: chrono::Utc::now(),
        };
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;
    }
    tracing::info!("Watermark snapshot full sync completed (final sequence_id: {final_seq})");
    Ok(())
}

async fn run_full_snapshot_stream_v2(args: ResolvedTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting snapshot-stream full sync from PostgreSQL (trigger) to SurrealDB (SDK v2)"
    );
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);
    let source_opts = trigger_source_opts(&args.connection_string, args.tables.clone());

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            pg_trigger_snapshot_full(&sink, source_opts, args.chunk_size, Some(&manager)).await
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
            pg_trigger_snapshot_full(&sink, source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, None) => {
            pg_trigger_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                args.chunk_size,
                None,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn run_full_snapshot_stream_v3(args: ResolvedTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting snapshot-stream full sync from PostgreSQL (trigger) to SurrealDB (SDK v3)"
    );
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal.clone());
    let source_opts = trigger_source_opts(&args.connection_string, args.tables.clone());

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(checkpoint::FilesystemStore::new(dir));
            pg_trigger_snapshot_full(&sink, source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(checkpoint_surreal3::Surreal3Store::new(
                surreal,
                table.clone(),
            ));
            pg_trigger_snapshot_full(&sink, source_opts, args.chunk_size, Some(&manager)).await
        }
        (None, None) => {
            pg_trigger_snapshot_full::<_, checkpoint::NullStore>(
                &sink,
                source_opts,
                args.chunk_size,
                None,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

/// Run the combined `from postgresql-trigger sync` orchestrator.
pub async fn run_sync(args: PostgreSQLTriggerSyncArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;
    match sdk_version {
        SdkVersion::V2 => run_sync_v2(args).await,
        SdkVersion::V3 => run_sync_v3(args).await,
    }
}

async fn run_sync_v2(args: PostgreSQLTriggerSyncArgs) -> anyhow::Result<()> {
    tracing::info!("Starting snapshot+stream sync from PostgreSQL (trigger) to SurrealDB (SDK v2)");
    let _schema = load_schema_if_provided(&args.schema_file)?;
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);
    pg_trigger_orchestrate(&sink, args).await
}

async fn run_sync_v3(args: PostgreSQLTriggerSyncArgs) -> anyhow::Result<()> {
    tracing::info!("Starting snapshot+stream sync from PostgreSQL (trigger) to SurrealDB (SDK v3)");
    let _schema = load_schema_if_provided(&args.schema_file)?;
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);
    pg_trigger_orchestrate(&sink, args).await
}

async fn pg_trigger_orchestrate<S: SurrealSink>(
    sink: &S,
    args: PostgreSQLTriggerSyncArgs,
) -> anyhow::Result<()> {
    let config = SnapshotStreamConfig {
        chunk_size: args.chunk_size,
    };
    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_opts = trigger_source_opts(&args.connection_string, args.tables.clone());
    let snapshot_opts = source_opts.clone();

    orchestrate_snapshot_then_incremental(
        async move {
            let mut checkpointer = NoopCheckpointer;
            surreal_sync_postgresql_trigger_source::run_snapshot_stream_full_sync(
                sink,
                &snapshot_opts,
                &config,
                &mut checkpointer,
            )
            .await
        },
        |sequence_id| PostgreSQLCheckpoint {
            sequence_id,
            timestamp: chrono::Utc::now(),
        },
        |from_checkpoint| {
            surreal_sync_postgresql_trigger_source::run_incremental_sync(
                sink,
                source_opts,
                from_checkpoint,
                deadline,
                None,
            )
        },
    )
    .await
}

/// Emit an ad-hoc `execute-snapshot` signal so a running `sync` snapshots the
/// requested tables.
pub async fn run_snapshot_signal(args: PostgreSQLTriggerSnapshotArgs) -> anyhow::Result<()> {
    let source_opts = trigger_source_opts(&args.connection_string, vec![]);
    surreal_sync_postgresql_trigger_source::request_snapshot(&source_opts, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}

/// Run PostgreSQL trigger-based incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: PostgreSQLTriggerIncrementalArgs) -> anyhow::Result<()> {
    let args = resolve_incremental_args(args)?;
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_incremental_v2(args).await,
        SdkVersion::V3 => run_incremental_v3(args).await,
    }
}

async fn run_incremental_v2(args: ResolvedTriggerIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            tracing::info!("Reading checkpoint from SurrealDB table: {}", table);
            let checkpoint_surreal = surreal2_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
        relation_tables: vec![],
    };

    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(args: ResolvedTriggerIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            tracing::info!("Starting from checkpoint: {}", s);
            surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            tracing::info!("Reading checkpoint from SurrealDB table: {}", table);
            let checkpoint_surreal = surreal3_sink::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store = checkpoint_surreal3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = checkpoint::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint>(
                    checkpoint::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
        tables: args.tables,
        relation_tables: vec![],
    };

    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        from_checkpoint,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}
