//! Kafka streaming sync handler.
//!
//! Source crate: crates/kafka-source/
//! CLI command:
//! - Streaming: `from kafka --brokers ... --topic ... --to-namespace ... --to-database ...`

use anyhow::Context;
use sync_core::Schema;

use super::{get_sdk_version, parse_duration_to_secs, SdkVersion};
use crate::KafkaArgs;

/// Run Kafka streaming sync, dispatching to appropriate SDK version.
pub async fn run(args: KafkaArgs) -> anyhow::Result<()> {
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

async fn run_v2(args: KafkaArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Kafka consumer sync (SDK v2)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Timeout: {}", args.timeout);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Parse timeout duration
    let timeout_secs = parse_duration_to_secs(&args.timeout)
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);
    tracing::info!("Will consume until deadline: {}", deadline);

    // Connect to SurrealDB using v2 SDK
    let surreal_opts = surreal2_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal2_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = std::sync::Arc::new(surreal2_sink::Surreal2Sink::new(surreal));

    let table_schema = if let Some(schema_path) = args.schema_file {
        let schema = Schema::from_file(&schema_path)
            .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?;
        let table_name = args
            .config
            .table_name
            .as_ref()
            .unwrap_or(&args.config.topic);
        schema
            .get_table(table_name)
            .map(|t| t.to_table_definition())
    } else {
        None
    };

    surreal_sync_kafka_source::run_incremental_sync(sink, args.config, deadline, table_schema)
        .await?;

    Ok(())
}

async fn run_v3(args: KafkaArgs) -> anyhow::Result<()> {
    tracing::info!("Starting Kafka consumer sync (SDK v3)");
    tracing::info!("Target: {}/{}", args.to_namespace, args.to_database);
    tracing::info!("Timeout: {}", args.timeout);

    if args.surreal.dry_run {
        tracing::info!("Running in dry-run mode - no data will be written");
    }

    // Parse timeout duration
    let timeout_secs = parse_duration_to_secs(&args.timeout)
        .with_context(|| format!("Invalid timeout format: {}", args.timeout))?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(timeout_secs);
    tracing::info!("Will consume until deadline: {}", deadline);

    // Connect to SurrealDB using v3 SDK
    let surreal_opts = surreal3_sink::SurrealOpts {
        surreal_endpoint: args.surreal.surreal_endpoint,
        surreal_username: args.surreal.surreal_username,
        surreal_password: args.surreal.surreal_password,
    };
    let surreal =
        surreal3_sink::surreal_connect(&surreal_opts, &args.to_namespace, &args.to_database)
            .await?;
    let sink = std::sync::Arc::new(surreal3_sink::Surreal3Sink::new(surreal));

    let table_schema = if let Some(schema_path) = args.schema_file {
        let schema = Schema::from_file(&schema_path)
            .with_context(|| format!("Failed to load sync schema from {schema_path:?}"))?;
        let table_name = args
            .config
            .table_name
            .as_ref()
            .unwrap_or(&args.config.topic);
        schema
            .get_table(table_name)
            .map(|t| t.to_table_definition())
    } else {
        None
    };

    surreal_sync_kafka_source::run_incremental_sync(sink, args.config, deadline, table_schema)
        .await?;

    Ok(())
}
