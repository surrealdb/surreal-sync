//! MongoDB full and incremental sync handlers.
//!
//! Source crate: crates/mongodb-changestream-source/
//! CLI commands:
//! - Full sync: `from mongodb full --connection-string ... --database ... --to-namespace ... --to-database ...`
//! - Incremental sync: `from mongodb incremental --connection-string ... --incremental-from ...`

use anyhow::Context;
use checkpoint::Checkpoint;

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::{MongoDBFullArgs, MongoDBIncrementalArgs};

/// Run MongoDB full sync, dispatching to appropriate SDK version.
pub async fn run_full(args: MongoDBFullArgs) -> anyhow::Result<()> {
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

async fn run_full_v2(args: MongoDBFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MongoDB to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Load and convert schema to DatabaseSchema for type-aware conversion
    let schema = load_schema_if_provided(&args.schema_file)?.map(|s| s.to_database_schema());

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

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_mongodb_changestream_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_mongodb_changestream_source::migrate_from_mongodb(
            &sink,
            source_opts,
            sync_opts,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

async fn run_full_v3(args: MongoDBFullArgs) -> anyhow::Result<()> {
    tracing::info!("Starting full sync from MongoDB to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Load and convert schema to DatabaseSchema for type-aware conversion
    let schema = load_schema_if_provided(&args.schema_file)?.map(|s| s.to_database_schema());

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

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    let sync_opts = surreal_sync_mongodb_changestream_source::SyncOpts {
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema,
    };

    if args.emit_checkpoints {
        let store = checkpoint::FilesystemStore::new(&args.checkpoint_dir);
        let sync_manager = checkpoint::SyncManager::new(store);
        surreal_sync_mongodb_changestream_source::run_full_sync(
            &sink,
            source_opts,
            sync_opts,
            Some(&sync_manager),
        )
        .await?;
    } else {
        surreal_sync_mongodb_changestream_source::migrate_from_mongodb(
            &sink,
            source_opts,
            sync_opts,
        )
        .await?;
    }

    tracing::info!("Full sync completed successfully");
    Ok(())
}

/// Run MongoDB incremental sync, dispatching to appropriate SDK version.
pub async fn run_incremental(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
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

async fn run_incremental_v2(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MongoDB to SurrealDB (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

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

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let mongodb_from =
        surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(
            &args.incremental_from,
        )?;
    let mongodb_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    surreal_sync_mongodb_changestream_source::run_incremental_sync(
        &sink,
        source_opts,
        mongodb_from,
        deadline,
        mongodb_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}

async fn run_incremental_v3(args: MongoDBIncrementalArgs) -> anyhow::Result<()> {
    tracing::info!("Starting incremental sync from MongoDB to SurrealDB (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Starting from checkpoint: {}", args.incremental_from);

    if let Some(ref to) = args.incremental_to {
        tracing::info!("Will stop at checkpoint: {}", to);
    }

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

    let timeout_seconds: i64 = args
        .timeout
        .parse()
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_seconds);

    let mongodb_from =
        surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(
            &args.incremental_from,
        )?;
    let mongodb_to = args
        .incremental_to
        .as_ref()
        .map(|s| surreal_sync_mongodb_changestream_source::MongoDBCheckpoint::from_cli_string(s))
        .transpose()?;

    let source_opts = surreal_sync_mongodb_changestream_source::SourceOpts {
        source_uri: args.connection_string,
        source_database: Some(args.database),
    };

    surreal_sync_mongodb_changestream_source::run_incremental_sync(
        &sink,
        source_opts,
        mongodb_from,
        deadline,
        mongodb_to,
    )
    .await?;

    tracing::info!("Incremental sync completed successfully");
    Ok(())
}
