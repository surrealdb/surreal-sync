//! Snowflake CLI glue (Surreal version auto-detect).
//!
//! Embedders should use `surreal_sync_snowflake::run` with one sink type. This
//! module is CLI-only and auto-detects SurrealDB v2 vs v3.

use surreal_sync_runtime::SinkConnect;

pub use surreal_sync_snowflake::from_snowflake::cli::Args;

use crate::from::{get_sdk_version, SdkVersion};

/// Stock CLI path: auto-detect Surreal major version.
pub async fn run_args(args: Args) -> anyhow::Result<()> {
    let sdk_version = get_sdk_version(
        &args.surreal.surreal_endpoint,
        args.surreal.surreal_sdk_version.as_deref(),
    )
    .await?;

    match sdk_version {
        SdkVersion::V2 => {
            let config = args
                .surreal
                .to_config(args.to_namespace.clone(), args.to_database.clone());
            let sink = surreal_sync_surreal::v2::Surreal2Sink::connect(&config).await?;
            surreal_sync_snowflake::from_snowflake::cli::run_args_with_sink(args, &sink).await
        }
        SdkVersion::V3 => {
            let config = args
                .surreal
                .to_config(args.to_namespace.clone(), args.to_database.clone());
            let sink = surreal_sync_surreal::v3::Surreal3Sink::connect(&config).await?;
            surreal_sync_snowflake::from_snowflake::cli::run_args_with_sink(args, &sink).await
        }
    }
}
