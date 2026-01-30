//! PostgreSQL trigger-based CDC sync handlers.
//!
//! Source crate: crates/postgresql-trigger-source/
//! CLI commands:
//! - Full sync: `from postgresql-trigger full --connection-string ... --to-namespace ... --to-database ...`
//! - Incremental sync: `from postgresql-trigger incremental --connection-string ... --incremental-from ...`

use anyhow::Context;
use checkpoint::Checkpoint;

use super::{extract_postgresql_database, get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::{PostgreSQLTriggerFullArgs, PostgreSQLTriggerIncrementalArgs};

/// Run PostgreSQL trigger-based full sync, dispatching to appropriate SDK version.
pub async fn run_full(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_full_v2(args).await,
        SdkVersion::V3 => run_full_v3(args).await,
    }
}

async fn run_full_v2(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_postgresql_trigger_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            source_opts,
            sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_full_v3(args: PostgreSQLTriggerFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_postgresql_trigger_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_postgresql_trigger_source::run_full_sync::<_, checkpoint::NullStore>(
            &sink,
            source_opts,
            sync_opts,
            None,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

/// Run PostgreSQL trigger-based incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: PostgreSQLTriggerIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: PostgreSQLTriggerIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v2)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let pg_from = surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal2_sink::Surreal2Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        pg_from,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(args: PostgreSQLTriggerIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!(
        "Starting incremental sync from PostgreSQL (trigger-based) to SurrealDB (SDK v3)"
    );
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let _schema = load_schema_if_provided(&args.schema_file)?;

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let pg_from = surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(
        &args.incremental_from,
    )?;
    let pg_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_database = extract_postgresql_database(&args.connection_string);
    let source_opts = surreal_sync_postgresql_trigger_source::SourceOpts {
        source_uri: args.connection_string,
        source_database,
    };

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = surreal3_sink::Surreal3Sink::new(surreal);

    surreal_sync_postgresql_trigger_source::run_incremental_sync(
        &sink,
        source_opts,
        pg_from,
        deadline,
        pg_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}
