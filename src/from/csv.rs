//! CSV file import handler.
//!
//! Source crate: crates/csv-source/ (part of surreal_sync::csv)
//! CLI command:
//! - Import: `from csv --files ... --table ... --to-namespace ... --to-database ...`

use super::{get_sdk_version, load_schema_if_provided, SdkVersion};
use crate::CsvArgs;

/// Run CSV import, dispatching to appropriate SDK version.
pub async fn run(args: CsvArgs) -> anyhow::Result<()> {
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

async fn run_v2(args: CsvArgs) -> anyhow::Result<()> {
    tracing::info!("Starting CSV import (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

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

    let config = surreal_sync::csv::Config {
        sources: vec![],
        files: args.files,
        s3_uris: args.s3_uris,
        http_uris: args.http_uris,
        table: args.table,
        batch_size: args.surreal.batch_size,
        has_headers: args.has_headers,
        delimiter: args.delimiter as u8,
        id_field: args.id_field,
        column_names: args.column_names,
        emit_metrics: args.emit_metrics,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::csv::sync(&sink, config).await?;

    tracing::info!("CSV import completed successfully");
    Ok(())
}

async fn run_v3(args: CsvArgs) -> anyhow::Result<()> {
    tracing::info!("Starting CSV import (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    let schema = load_schema_if_provided(&args.schema_file)?;

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

    let config = surreal_sync::csv::Config {
        sources: vec![],
        files: args.files,
        s3_uris: args.s3_uris,
        http_uris: args.http_uris,
        table: args.table,
        batch_size: args.surreal.batch_size,
        has_headers: args.has_headers,
        delimiter: args.delimiter as u8,
        id_field: args.id_field,
        column_names: args.column_names,
        emit_metrics: args.emit_metrics,
        dry_run: args.surreal.dry_run,
        schema,
    };
    surreal_sync::csv::sync(&sink, config).await?;

    tracing::info!("CSV import completed successfully");
    Ok(())
}
