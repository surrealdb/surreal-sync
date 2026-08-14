//! CLI e2e: `from mssql sync --transforms-config` mutate worker.

use std::path::PathBuf;
use std::process::Command;

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};

use crate::common::exec_sql;

fn fixture_worker_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("target");
    p.push("debug");
    p.push("sync-transform-fixture-worker");
    p
}

fn ensure_fixture_worker() -> PathBuf {
    let path = fixture_worker_path();
    if !path.is_file() {
        let status = Command::new("cargo")
            .args([
                "build",
                "-p",
                "surreal-sync-runtime",
                "--features",
                "test-support",
                "--bin",
                "sync-transform-fixture-worker",
            ])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("spawn cargo build fixture worker");
        assert!(
            status.success(),
            "failed to build sync-transform-fixture-worker"
        );
    }
    path
}

#[tokio::test]
async fn test_mssql_stream_cli_transforms_config_mutate() -> Result<(), Box<dyn std::error::Error>>
{
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let container = surreal_sync::testing::shared_containers::shared_mssql().await;
    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-mssql-transforms-cli-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let conn_str =
        surreal_sync::testing::shared_containers::create_mssql_test_db(container, test_id).await?;
    exec_sql(
        &conn_str,
        "CREATE TABLE dbo.people (id INT NOT NULL PRIMARY KEY, name NVARCHAR(64) NOT NULL);",
    )
    .await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&config).await?;
    cleanup_auto(&conn, &["people"]).await?;

    let snapshot_args = [
        "from",
        "mssql",
        "sync",
        "--snapshot-mode",
        "only",
        "--connection-string",
        &conn_str,
        "--tables",
        "dbo.people",
        "--surreal-endpoint",
        &config.surreal_endpoint,
        "--to-namespace",
        &config.surreal_namespace,
        "--to-database",
        &config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--checkpoint-dir",
        &checkpoint_dir,
        "--chunk-size",
        "32",
    ];
    let output = execute_surreal_sync(&snapshot_args)?;
    assert_cli_success(&output, "mssql snapshot phase CLI");

    exec_sql(
        &conn_str,
        "INSERT INTO dbo.people (id, name) VALUES (1, N'alice'), (2, N'bob');",
    )
    .await?;

    let transforms_toml = format!(
        r#"
[pipeline]
failure_policy = "fail"
batch_size = 1
batch_max_wait = "500ms"
timeout = "60s"
max_in_flight = 1

[[transforms]]
type = "command"
mode = "persistent"
command = ["{}", "mutate"]
stdio.framer = "ndjson"
"#,
        worker.display()
    );
    std::fs::create_dir_all(&checkpoint_dir)?;
    let transforms_path = format!("{checkpoint_dir}/transforms-mutate.toml");
    std::fs::write(&transforms_path, transforms_toml)?;

    let stream_args = [
        "from",
        "mssql",
        "sync",
        "--snapshot-mode",
        "never",
        "--connection-string",
        &conn_str,
        "--tables",
        "dbo.people",
        "--surreal-endpoint",
        &config.surreal_endpoint,
        "--to-namespace",
        &config.surreal_namespace,
        "--to-database",
        &config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--timeout",
        "25s",
        "--checkpoint-dir",
        &checkpoint_dir,
        "--transforms-config",
        &transforms_path,
        "--chunk-size",
        "32",
    ];
    let stream_output = execute_surreal_sync(&stream_args)?;
    assert_cli_success(
        &stream_output,
        "mssql stream CLI with --transforms-config mutate",
    );

    #[derive(Debug, serde::Deserialize)]
    struct PeopleRowV2 {
        name: Option<String>,
    }
    use surrealdb3::types::SurrealValue;
    #[derive(SurrealValue, Debug)]
    #[surreal(crate = "surrealdb3::types")]
    struct PeopleRowV3 {
        name: Option<String>,
    }

    let names: Vec<Option<String>> = match &conn {
        SurrealConnection::V2(db) => {
            let mut resp = db.query("SELECT name FROM people").await?;
            let rows: Vec<PeopleRowV2> = resp.take(0)?;
            rows.into_iter().map(|r| r.name).collect()
        }
        SurrealConnection::V3(db) => {
            let mut resp = db.query("SELECT name FROM people").await?;
            let rows: Vec<PeopleRowV3> = resp.take(0)?;
            rows.into_iter().map(|r| r.name).collect()
        }
    };
    assert_eq!(
        names.len(),
        2,
        "expected two people docs after transform stream, got {names:?}"
    );
    for name in &names {
        assert_eq!(
            name.as_deref(),
            Some("mutated"),
            "CLI --transforms-config external mutate should rewrite name; got {names:?}"
        );
    }

    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
