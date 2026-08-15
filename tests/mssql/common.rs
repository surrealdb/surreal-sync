//! Shared helpers for SQL Server e2e tests.

use surreal_sync::testing::surreal::SurrealConnection;
use surreal_sync::testing::TestConfig;
use surreal_sync_core::ZeroTemporalPolicy;
use surreal_sync_mssql::from_mssql::cli::{SnapshotModeArg, SyncArgs, SyncStrategy};
use surreal_sync_runtime::{SinkConnect, SurrealCliOpts};

pub fn surreal_opts(endpoint: &str) -> SurrealCliOpts {
    SurrealCliOpts {
        surreal_endpoint: endpoint.to_string(),
        surreal_username: "root".into(),
        surreal_password: "root".into(),
        batch_size: 1000,
        dry_run: false,
        surreal_sdk_version: None,
        zero_temporal: ZeroTemporalPolicy::default(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn sync_args(
    connection_string: String,
    tables: Vec<String>,
    config: &TestConfig,
    snapshot_mode: SnapshotModeArg,
    strategy: SyncStrategy,
    checkpoint_dir: Option<String>,
    schemafull: bool,
    timeout: Option<String>,
    relation_tables: Vec<String>,
) -> SyncArgs {
    SyncArgs {
        connection_string,
        tables,
        relation_tables,
        to_namespace: config.surreal_namespace.clone(),
        to_database: config.surreal_database.clone(),
        snapshot_mode,
        timeout,
        strategy,
        chunk_size: 32,
        checkpoint_dir,
        checkpoints_surreal_table: None,
        transforms_config: None,
        schemafull,
        surreal: surreal_opts(&config.surreal_endpoint),
    }
}

pub async fn exec_sql(conn: &str, sql: &str) -> anyhow::Result<()> {
    let client = surreal_sync_mssql::from_mssql::testing::connect(conn).await?;
    client.simple_query(sql).await?;
    Ok(())
}

pub async fn mssql_client(
    conn: &str,
) -> Result<surreal_sync_mssql::from_mssql::testing::MssqlClient, Box<dyn std::error::Error>> {
    Ok(surreal_sync_mssql::from_mssql::testing::connect(conn).await?)
}

pub async fn run_mssql_sync(args: SyncArgs) -> anyhow::Result<()> {
    let (pipeline, apply_opts) =
        surreal_sync_runtime::load_transforms_from_args(args.transforms_config.as_deref())?;
    let http = args
        .surreal
        .surreal_endpoint
        .replace("ws://", "http://")
        .replace("wss://", "https://");
    let detected = surreal_sync_surreal::version::detect_server_version(&http).await?;
    let config = args
        .surreal
        .to_config(args.to_namespace.clone(), args.to_database.clone());
    match detected {
        surreal_sync_surreal::version::SurrealMajorVersion::V2 => {
            let sink = surreal_sync_surreal::v2::Surreal2Sink::connect(&config).await?;
            surreal_sync_mssql::from_mssql::cli::run_sync(args, &sink, pipeline, apply_opts).await
        }
        surreal_sync_surreal::version::SurrealMajorVersion::V3 => {
            let sink = surreal_sync_surreal::v3::Surreal3Sink::connect(&config).await?;
            surreal_sync_mssql::from_mssql::cli::run_sync(args, &sink, pipeline, apply_opts).await
        }
    }
}

pub async fn query_debug(
    conn: &SurrealConnection,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    match conn {
        SurrealConnection::V2(db) => {
            let r = db.query(sql).await?;
            Ok(format!("{r:?}"))
        }
        SurrealConnection::V3(db) => {
            let r = db.query(sql).await?;
            Ok(format!("{r:?}"))
        }
    }
}
