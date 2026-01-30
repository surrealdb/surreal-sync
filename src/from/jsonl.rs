//! JSONL file import handler.
//!
//! Source crate: crates/jsonl-source/ (part of surreal_sync::jsonl)
//! CLI command:
//! - Import: `from jsonl --path ... --to-namespace ... --to-database ...`

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::JsonlArgs;

/// Run JSONL import, dispatching to appropriate SDK version.
pub async fn run(args: JsonlArgs) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => run_v2(args).await,
        SdkVersion::V3 => run_v3(args).await,
    }
}

async fn run_v2(args: JsonlArgs) -> anyhow::Result<()> {
    tracing::info!("Starting JSONL import (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Load and convert schema to DatabaseSchema for type-aware JSONL conversion
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

    // Create config with file source
    let config = surreal_sync::jsonl::Config {
        sources: vec![],
        files: vec![args.path.into()],
        s3_uris: vec![],
        http_uris: vec![],
        id_field: args.id_field,
        conversion_rules: args.conversion_rules,
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::jsonl::sync(&sink, config).await?;

    tracing::info!("JSONL import completed successfully");
    Ok(())
}

async fn run_v3(args: JsonlArgs) -> anyhow::Result<()> {
    tracing::info!("Starting JSONL import (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Load and convert schema to DatabaseSchema for type-aware JSONL conversion
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

    // Create config with file source
    let config = surreal_sync::jsonl::Config {
        sources: vec![],
        files: vec![args.path.into()],
        s3_uris: vec![],
        http_uris: vec![],
        id_field: args.id_field,
        conversion_rules: args.conversion_rules,
        batch_size: args.surreal.batch_size,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::jsonl::sync(&sink, config).await?;

    tracing::info!("JSONL import completed successfully");
    Ok(())
}
