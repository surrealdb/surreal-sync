//! SQL Server CDC CLI glue (Surreal version auto-detect).
//!
//! Embedders should call `surreal_sync_mssql::run::<Surreal3Sink>` (or
//! `Surreal2Sink`). This module is only for the surreal-sync CLI, which
//! auto-detects SurrealDB v2 vs v3.

use surreal_sync_mssql::from_mssql::cli::{run_sync, Pipeline, SyncArgs};
use surreal_sync_runtime::{load_transforms_from_args, SinkConnect};

pub use surreal_sync_mssql::from_mssql::cli::Commands;

use crate::from::{get_sdk_version, SdkVersion};

/// Stock CLI: auto-detect Surreal major, connect matching sink, run embed orchestration.
pub async fn run_command(command: Commands) -> anyhow::Result<()> {
    match command {
        Commands::Sync(args) => {
            let (pipeline, apply_opts) =
                load_transforms_from_args(args.transforms_config.as_deref())?;
            run_sync_autodetect(*args, pipeline, apply_opts).await
        }
    }
}

async fn run_sync_autodetect(
    args: SyncArgs,
    pipeline: Pipeline,
    apply_opts: surreal_sync_runtime::ApplyOpts,
) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    let config = args
        .surreal
        .to_config(args.to_namespace.clone(), args.to_database.clone());

    match sdk_version {
        SdkVersion::V2 => {
            let sink = surreal_sync_surreal::v2::Surreal2Sink::connect(&config).await?;
            run_sync(args, &sink, pipeline, apply_opts).await
        }
        SdkVersion::V3 => {
            let sink = surreal_sync_surreal::v3::Surreal3Sink::connect(&config).await?;
            run_sync(args, &sink, pipeline, apply_opts).await
        }
    }
}
