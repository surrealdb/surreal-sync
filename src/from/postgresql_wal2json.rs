//! PostgreSQL WAL-based logical replication sync handlers.
//!
//! Source crate: crates/postgresql/ (from_wal2json)
//! CLI commands:
//! - Full sync: `from postgresql full --connection-string ... --slot ... --tables ... --to-namespace ... --to-database ...`
//! - Incremental sync: `from postgresql incremental --connection-string ... --slot ... --tables ... --checkpoints-surreal-table ...`
//!
//! Both commands support `-c` / `--config-file` to load settings from a TOML file:
//! - Full sync: `from postgresql full -c surreal-sync.toml`
//! - Incremental sync: `from postgresql incremental -c surreal-sync.toml`
//!
//! CLI flags take precedence over config file values when both are provided.

use anyhow::Context;
use std::path::PathBuf;
use surreal_sync::orchestrate_snapshot_then_incremental;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use surreal_sync_postgresql::from_wal2json::{PostgreSQLLogicalCheckpoint, ReplicationTailOptions};
use surreal_sync_runtime::SnapshotTransforms;
use surreal_sync_runtime::SurrealCliOpts as SurrealOpts;
use surreal_sync_runtime::{ApplyOpts, Pipeline};

use super::transforms::load_transforms_from_args;
use super::{
    get_sdk_version, load_schema_if_provided, make_surreal2_sink, make_surreal3_sink, SdkVersion,
};
use crate::config::load_config;
use crate::{
    PostgreSQLLogicalFullArgs, PostgreSQLLogicalIncrementalArgs, PostgreSQLLogicalSnapshotArgs,
    PostgreSQLLogicalSyncArgs, SyncStrategy,
};
use surreal_sync_postgresql::from_wal2json::toml_config::{
    Wal2jsonFullSource, Wal2jsonIncrementalSource,
};

// ---- Resolved args (all required fields are non-optional) ----

struct ResolvedWal2jsonFullArgs {
    connection_string: String,
    slot: String,
    tables: Vec<String>,
    schema: String,
    to_namespace: String,
    to_database: String,
    schema_file: Option<PathBuf>,
    checkpoint_dir: Option<String>,
    checkpoints_surreal_table: Option<String>,
    strategy: SyncStrategy,
    chunk_size: usize,
    transforms_config: Option<PathBuf>,
    surreal: SurrealOpts,
}

struct ResolvedWal2jsonIncrementalArgs {
    connection_string: String,
    slot: String,
    tables: Vec<String>,
    schema: String,
    to_namespace: String,
    to_database: String,
    schema_file: Option<PathBuf>,
    incremental_from: Option<String>,
    checkpoints_surreal_table: Option<String>,
    incremental_to: Option<String>,
    timeout: String,
    transforms_config: Option<PathBuf>,
    surreal: SurrealOpts,
}

fn resolve_full_args(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<ResolvedWal2jsonFullArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<Wal2jsonFullSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedWal2jsonFullArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            slot: if args.slot != "surreal_sync_slot" {
                args.slot
            } else {
                pg.slot
            },
            tables: if args.tables.is_empty() {
                pg.tables
            } else {
                args.tables
            },
            schema: if args.schema != "public" {
                args.schema
            } else {
                pg.schema
            },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            schema_file: args.schema_file.or(pg.schema_file),
            checkpoint_dir: args.checkpoint_dir.or(pg.checkpoint_dir),
            checkpoints_surreal_table: args
                .checkpoints_surreal_table
                .or(pg.checkpoints_surreal_table),
            strategy: args.strategy,
            chunk_size: args.chunk_size,
            transforms_config: args.transforms_config,
            surreal: SurrealOpts {
                surreal_endpoint: sink.endpoint,
                surreal_username: sink.username,
                surreal_password: sink.password,
                batch_size: sink.batch_size,
                dry_run: sink.dry_run,
                surreal_sdk_version: args.surreal.surreal_sdk_version.or(sink.sdk_version),
                zero_temporal: sink.zero_temporal,
            },
        })
    } else {
        Ok(ResolvedWal2jsonFullArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            slot: args.slot,
            tables: args.tables,
            schema: args.schema,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            schema_file: args.schema_file,
            checkpoint_dir: args.checkpoint_dir,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            strategy: args.strategy,
            chunk_size: args.chunk_size,
            transforms_config: args.transforms_config,
            surreal: args.surreal,
        })
    }
}

fn resolve_incremental_args(
    args: PostgreSQLLogicalIncrementalArgs,
) -> anyhow::Result<ResolvedWal2jsonIncrementalArgs> {
    if let Some(ref config_path) = args.config_file {
        let cfg = load_config::<Wal2jsonIncrementalSource>(config_path)?;
        let pg = cfg.source.postgresql;
        let sink = cfg.sink.surrealdb;
        Ok(ResolvedWal2jsonIncrementalArgs {
            connection_string: args.connection_string.unwrap_or(pg.connection_string),
            slot: if args.slot != "surreal_sync_slot" {
                args.slot
            } else {
                pg.slot
            },
            tables: if args.tables.is_empty() {
                pg.tables
            } else {
                args.tables
            },
            schema: if args.schema != "public" {
                args.schema
            } else {
                pg.schema
            },
            to_namespace: args.to_namespace.unwrap_or(sink.namespace),
            to_database: args.to_database.unwrap_or(sink.database),
            schema_file: args.schema_file.or(pg.schema_file),
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
            transforms_config: args.transforms_config,
            surreal: SurrealOpts {
                surreal_endpoint: sink.endpoint,
                surreal_username: sink.username,
                surreal_password: sink.password,
                batch_size: sink.batch_size,
                dry_run: sink.dry_run,
                surreal_sdk_version: args.surreal.surreal_sdk_version.or(sink.sdk_version),
                zero_temporal: sink.zero_temporal,
            },
        })
    } else {
        Ok(ResolvedWal2jsonIncrementalArgs {
            connection_string: args
                .connection_string
                .ok_or_else(|| anyhow::anyhow!("--connection-string is required"))?,
            slot: args.slot,
            tables: args.tables,
            schema: args.schema,
            to_namespace: args
                .to_namespace
                .ok_or_else(|| anyhow::anyhow!("--to-namespace is required"))?,
            to_database: args
                .to_database
                .ok_or_else(|| anyhow::anyhow!("--to-database is required"))?,
            schema_file: args.schema_file,
            incremental_from: args.incremental_from,
            checkpoints_surreal_table: args.checkpoints_surreal_table,
            incremental_to: args.incremental_to,
            timeout: args.timeout,
            transforms_config: args.transforms_config,
            surreal: args.surreal,
        })
    }
}

// ---- Public entry points ----

/// Run PostgreSQL WAL-based full sync, dispatching by strategy then SDK version.
pub async fn run_full(args: PostgreSQLLogicalFullArgs) -> anyhow::Result<()> {
    let args = resolve_full_args(args)?;
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match (args.strategy, sdk_version) {
        (SyncStrategy::SequentialSnapshot, SdkVersion::V2) => run_full_v2(args).await,
        (SyncStrategy::SequentialSnapshot, SdkVersion::V3) => run_full_v3(args).await,
        (SyncStrategy::InterleavedSnapshot, SdkVersion::V2) => {
            run_full_interleaved_snapshot_v2(args).await
        }
        (SyncStrategy::InterleavedSnapshot, SdkVersion::V3) => {
            run_full_interleaved_snapshot_v3(args).await
        }
    }
}

async fn run_full_v2(args: ResolvedWal2jsonFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal_sync_surreal::v2::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::v2::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal2_sink(surreal, args.surreal.zero_temporal);

    let source_opts = surreal_sync_postgresql::from_wal2json::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = surreal_sync_runtime::checkpoint_fs::FilesystemStore::new(dir);
            let sync_manager = surreal_sync_core::SyncManager::new(store);
            surreal_sync_postgresql::from_wal2json::run_full_sync_with_transforms(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
                &pipeline,
                &apply_opts,
            )
            .await?;
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal_sync_surreal::v2::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store =
                surreal_sync_surreal::v2::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = surreal_sync_core::SyncManager::new(store);
            surreal_sync_postgresql::from_wal2json::run_full_sync_with_transforms(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
                &pipeline,
                &apply_opts,
            )
            .await?;
        }
        (None, None) => {
            surreal_sync_postgresql::from_wal2json::run_full_sync_with_transforms::<
                _,
                surreal_sync_core::NullStore,
            >(&sink, source_opts, sync_opts, None, &pipeline, &apply_opts)
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    Ok(())
}

async fn run_full_v3(args: ResolvedWal2jsonFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting full sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal_sync_surreal::v3::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::v3::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal3_sink(surreal.clone(), args.surreal.zero_temporal);

    let source_opts = surreal_sync_postgresql::from_wal2json::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let store = surreal_sync_runtime::checkpoint_fs::FilesystemStore::new(dir);
            let sync_manager = surreal_sync_core::SyncManager::new(store);
            surreal_sync_postgresql::from_wal2json::run_full_sync_with_transforms(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
                &pipeline,
                &apply_opts,
            )
            .await?;
        }
        (None, Some(table)) => {
            let store = surreal_sync_surreal::v3::Surreal3Store::new(surreal, table.clone());
            let sync_manager = surreal_sync_core::SyncManager::new(store);
            surreal_sync_postgresql::from_wal2json::run_full_sync_with_transforms(
                &sink,
                source_opts,
                sync_opts,
                Some(&sync_manager),
                &pipeline,
                &apply_opts,
            )
            .await?;
        }
        (None, None) => {
            surreal_sync_postgresql::from_wal2json::run_full_sync_with_transforms::<
                _,
                surreal_sync_core::NullStore,
            >(&sink, source_opts, sync_opts, None, &pipeline, &apply_opts)
            .await?;
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }

    Ok(())
}

/// Run PostgreSQL WAL-based incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: PostgreSQLLogicalIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: ResolvedWal2jsonIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal_sync_surreal::v2::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            surreal_sync_postgresql::from_wal2json::PostgreSQLLogicalCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal_sync_surreal::v2::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store =
                surreal_sync_surreal::v2::Surreal2Store::new(checkpoint_surreal, table.clone());
            let sync_manager = surreal_sync_core::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_postgresql::from_wal2json::PostgreSQLLogicalCheckpoint>(
                    surreal_sync_core::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    let to_checkpoint = args
        .incremental_to
        .as_ref()
        .map(|s| {
            surreal_sync_postgresql::from_wal2json::PostgreSQLLogicalCheckpoint::from_cli_string(s)
        })
        .transpose()?;

    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    let surreal = surreal_sync_surreal::v2::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal2_sink(surreal, args.surreal.zero_temporal);

    let source_opts = surreal_sync_postgresql::from_wal2json::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
    };

    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    surreal_sync_postgresql::from_wal2json::run_incremental_sync_with_transforms(
        &sink,
        source_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(deadline, to_checkpoint),
        &pipeline,
        &apply_opts,
    )
    .await?;

    Ok(())
}

async fn run_incremental_v3(args: ResolvedWal2jsonIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal_sync_surreal::v3::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };

    let from_checkpoint = match (&args.incremental_from, &args.checkpoints_surreal_table) {
        (Some(s), _) => {
            surreal_sync_postgresql::from_wal2json::PostgreSQLLogicalCheckpoint::from_cli_string(s)?
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal_sync_surreal::v3::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let store =
                surreal_sync_surreal::v3::Surreal3Store::new(checkpoint_surreal, table.clone());
            let sync_manager = surreal_sync_core::SyncManager::new(store);
            sync_manager
                .read_checkpoint::<surreal_sync_postgresql::from_wal2json::PostgreSQLLogicalCheckpoint>(
                    surreal_sync_core::SyncPhase::FullSyncStart,
                )
                .await
                .with_context(|| "Failed to read t1 checkpoint from SurrealDB")?
        }
        (None, None) => {
            anyhow::bail!("--incremental-from or --checkpoints-surreal-table is required")
        }
    };

    let to_checkpoint = args
        .incremental_to
        .as_ref()
        .map(|s| {
            surreal_sync_postgresql::from_wal2json::PostgreSQLLogicalCheckpoint::from_cli_string(s)
        })
        .transpose()?;

    let timeout_secs: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);

    let surreal = surreal_sync_surreal::v3::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal3_sink(surreal, args.surreal.zero_temporal);

    let source_opts = surreal_sync_postgresql::from_wal2json::SourceOpts {
        connection_string: args.connection_string,
        slot_name: args.slot,
        tables: args.tables,
        schema: args.schema,
        relation_tables: vec![],
    };

    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    surreal_sync_postgresql::from_wal2json::run_incremental_sync_with_transforms(
        &sink,
        source_opts,
        from_checkpoint,
        ReplicationTailOptions::stream(deadline, to_checkpoint),
        &pipeline,
        &apply_opts,
    )
    .await?;

    Ok(())
}

// =============================================================================
// Interleaved snapshot strategy
// =============================================================================

fn wal2json_source_opts(
    connection_string: &str,
    slot: &str,
    tables: Vec<String>,
    schema: &str,
) -> surreal_sync_postgresql::from_wal2json::SourceOpts {
    surreal_sync_postgresql::from_wal2json::SourceOpts {
        connection_string: connection_string.to_string(),
        slot_name: slot.to_string(),
        tables,
        schema: schema.to_string(),
        relation_tables: vec![],
    }
}

/// Run a wal2json interleaved snapshot full sync, emitting the handoff LSN
/// as a checkpoint (when checkpoint storage is configured) so a later
/// `incremental` run can resume from the consistent end position.
async fn wal2json_snapshot_full<S, St>(
    sink: &S,
    source_opts: surreal_sync_postgresql::from_wal2json::SourceOpts,
    chunk_size: usize,
    manager: Option<&SyncManager<St>>,
    transforms: &SnapshotTransforms,
) -> anyhow::Result<()>
where
    S: SurrealSink,
    St: CheckpointStore,
{
    let final_lsn =
        surreal_sync_postgresql::from_wal2json::run_interleaved_snapshot_full_sync_with_transforms(
            sink,
            source_opts,
            chunk_size,
            transforms,
        )
        .await?;

    if let Some(manager) = manager {
        let checkpoint = PostgreSQLLogicalCheckpoint {
            lsn: final_lsn.to_pg_string(),
            timestamp: chrono::Utc::now(),
        };
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncStart)
            .await?;
        manager
            .emit_checkpoint(&checkpoint, SyncPhase::FullSyncEnd)
            .await?;
    }
    tracing::info!("Watermark snapshot full sync completed (final LSN: {final_lsn})");
    Ok(())
}

async fn run_full_interleaved_snapshot_v2(args: ResolvedWal2jsonFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot full sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal_sync_surreal::v2::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::v2::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal2_sink(surreal, args.surreal.zero_temporal);
    let source_opts = wal2json_source_opts(
        &args.connection_string,
        &args.slot,
        args.tables.clone(),
        &args.schema,
    );

    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    let transforms = SnapshotTransforms {
        pipeline,
        apply_opts,
    };

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(
                surreal_sync_runtime::checkpoint_fs::FilesystemStore::new(dir),
            );
            wal2json_snapshot_full(
                &sink,
                source_opts,
                args.chunk_size,
                Some(&manager),
                &transforms,
            )
            .await
        }
        (None, Some(table)) => {
            let checkpoint_surreal = surreal_sync_surreal::v2::surreal_connect(
                &surreal_opts,
                &args.to_namespace,
                &args.to_database,
            )
            .await?;
            let manager = SyncManager::new(surreal_sync_surreal::v2::Surreal2Store::new(
                checkpoint_surreal,
                table.clone(),
            ));
            wal2json_snapshot_full(
                &sink,
                source_opts,
                args.chunk_size,
                Some(&manager),
                &transforms,
            )
            .await
        }
        (None, None) => {
            wal2json_snapshot_full::<_, surreal_sync_core::NullStore>(
                &sink,
                source_opts,
                args.chunk_size,
                None,
                &transforms,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

async fn run_full_interleaved_snapshot_v3(args: ResolvedWal2jsonFullArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot full sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    let _schema = load_schema_if_provided(&args.schema_file)?;

    let surreal_opts = surreal_sync_surreal::v3::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::v3::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal3_sink(surreal.clone(), args.surreal.zero_temporal);
    let source_opts = wal2json_source_opts(
        &args.connection_string,
        &args.slot,
        args.tables.clone(),
        &args.schema,
    );

    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    let transforms = SnapshotTransforms {
        pipeline,
        apply_opts,
    };

    match (&args.checkpoint_dir, &args.checkpoints_surreal_table) {
        (Some(dir), None) => {
            let manager = SyncManager::new(
                surreal_sync_runtime::checkpoint_fs::FilesystemStore::new(dir),
            );
            wal2json_snapshot_full(
                &sink,
                source_opts,
                args.chunk_size,
                Some(&manager),
                &transforms,
            )
            .await
        }
        (None, Some(table)) => {
            let manager = SyncManager::new(surreal_sync_surreal::v3::Surreal3Store::new(
                surreal,
                table.clone(),
            ));
            wal2json_snapshot_full(
                &sink,
                source_opts,
                args.chunk_size,
                Some(&manager),
                &transforms,
            )
            .await
        }
        (None, None) => {
            wal2json_snapshot_full::<_, surreal_sync_core::NullStore>(
                &sink,
                source_opts,
                args.chunk_size,
                None,
                &transforms,
            )
            .await
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --checkpoint-dir and --checkpoints-surreal-table")
        }
    }
}

/// Run the combined `from postgresql sync` orchestrator.
pub async fn run_sync(args: PostgreSQLLogicalSyncArgs) -> anyhow::Result<()> {
    let (pipeline, apply_opts) = load_transforms_from_args(args.transforms_config.as_deref())?;
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;
    match sdk_version {
        SdkVersion::V2 => run_sync_v2(args, pipeline, apply_opts).await,
        SdkVersion::V3 => run_sync_v3(args, pipeline, apply_opts).await,
    }
}

async fn run_sync_v2(
    args: PostgreSQLLogicalSyncArgs,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot sync from PostgreSQL (logical replication) to SurrealDB (SDK v2)"
    );
    let _schema = load_schema_if_provided(&args.schema_file)?;
    let surreal_opts = surreal_sync_surreal::v2::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::v2::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal2_sink(surreal, args.surreal.zero_temporal);
    wal2json_orchestrate(&sink, args, pipeline, apply_opts).await
}

async fn run_sync_v3(
    args: PostgreSQLLogicalSyncArgs,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    tracing::info!(
        "Starting interleaved snapshot sync from PostgreSQL (logical replication) to SurrealDB (SDK v3)"
    );
    let _schema = load_schema_if_provided(&args.schema_file)?;
    let surreal_opts = surreal_sync_surreal::v3::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint.clone(),
        surreal_username: args.surreal.surreal_username.clone(),
        surreal_password: args.surreal.surreal_password.clone(),
    };
    let surreal = surreal_sync_surreal::v3::surreal_connect(
        &surreal_opts,
        &args.to_namespace,
        &args.to_database,
    )
    .await?;
    let sink = make_surreal3_sink(surreal, args.surreal.zero_temporal);
    wal2json_orchestrate(&sink, args, pipeline, apply_opts).await
}

async fn wal2json_orchestrate<S: SurrealSink>(
    sink: &S,
    args: PostgreSQLLogicalSyncArgs,
    pipeline: Pipeline,
    apply_opts: ApplyOpts,
) -> anyhow::Result<()> {
    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let snapshot_opts = wal2json_source_opts(
        &args.connection_string,
        &args.slot,
        args.tables.clone(),
        &args.schema,
    );
    let incremental_opts = wal2json_source_opts(
        &args.connection_string,
        &args.slot,
        args.tables.clone(),
        &args.schema,
    );
    let chunk_size = args.chunk_size;
    let transforms = SnapshotTransforms {
        pipeline: pipeline.clone(),
        apply_opts: apply_opts.clone(),
    };

    orchestrate_snapshot_then_incremental(
        async move {
            surreal_sync_postgresql::from_wal2json::run_interleaved_snapshot_full_sync_with_transforms(
                sink,
                snapshot_opts,
                chunk_size,
                &transforms,
            )
            .await
        },
        |lsn| PostgreSQLLogicalCheckpoint {
            lsn: lsn.to_pg_string(),
            timestamp: chrono::Utc::now(),
        },
        |from_checkpoint| {
            surreal_sync_postgresql::from_wal2json::run_incremental_sync_with_transforms(
                sink,
                incremental_opts,
                from_checkpoint,
                ReplicationTailOptions::stream(deadline, None),
                &pipeline,
                &apply_opts,
            )
        },
    )
    .await
}

/// Emit an ad-hoc `execute-snapshot` signal so a running `sync` snapshots the
/// requested tables.
pub async fn run_snapshot_signal(args: PostgreSQLLogicalSnapshotArgs) -> anyhow::Result<()> {
    let source_opts =
        wal2json_source_opts(&args.connection_string, &args.slot, vec![], &args.schema);
    surreal_sync_postgresql::from_wal2json::request_snapshot(&source_opts, &args.tables).await?;
    tracing::info!(
        "Requested ad-hoc snapshot of tables {:?} via execute-snapshot signal",
        args.tables
    );
    Ok(())
}
