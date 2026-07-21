//! CLI e2e: `from mysql-binlog sync --transforms-config` with an external mutate worker.

use std::path::PathBuf;
use std::process::Command;

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};

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
                "sync-transform",
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
    assert!(
        path.is_file(),
        "fixture worker missing after build: {}",
        path.display()
    );
    path
}

#[tokio::test]
async fn test_mysql_binlog_stream_cli_transforms_config_mutate(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let container = surreal_sync::testing::shared_containers::shared_mysql_binlog().await;

    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-mysql-binlog-transforms-cli-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_conn_str =
        surreal_sync::testing::shared_containers::create_binlog_test_db(container, test_id).await?;
    let test_db_name = format!("test_{test_id}");

    let pool = mysql_async::Pool::from_url(&test_conn_str)?;
    let mut mysql_conn = pool.get_conn().await?;
    mysql_async::prelude::Queryable::query_drop(
        &mut mysql_conn,
        "CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)",
    )
    .await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_auto(&conn, &["people"]).await?;

    let mysql_conn_str = test_conn_str.clone();
    let snapshot_args = [
        "from",
        "mysql-binlog",
        "sync",
        "--snapshot-mode",
        "only",
        "--connection-string",
        &mysql_conn_str,
        "--database",
        &test_db_name,
        "--tables",
        "people",
        "--surreal-endpoint",
        &surreal_config.surreal_endpoint,
        "--to-namespace",
        &surreal_config.surreal_namespace,
        "--to-database",
        &surreal_config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--checkpoint-dir",
        &checkpoint_dir,
    ];
    let output = execute_surreal_sync(&snapshot_args)?;
    assert_cli_success(&output, "MySQL binlog snapshot phase CLI");

    mysql_async::prelude::Queryable::exec_drop(
        &mut mysql_conn,
        "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
        (),
    )
    .await?;

    use checkpoint::{Checkpoint, SyncPhase};
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, SyncPhase::FullSyncStart).await?;
    let binlog_checkpoint: surreal_sync_mysql_binlog_source::BinlogCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = binlog_checkpoint.to_cli_string();

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
    let transforms_path = format!("{checkpoint_dir}/transforms-mutate.toml");
    std::fs::create_dir_all(&checkpoint_dir)?;
    std::fs::write(&transforms_path, transforms_toml)?;

    let stream_args = [
        "from",
        "mysql-binlog",
        "sync",
        "--snapshot-mode",
        "never",
        "--connection-string",
        &mysql_conn_str,
        "--database",
        &test_db_name,
        "--tables",
        "people",
        "--surreal-endpoint",
        &surreal_config.surreal_endpoint,
        "--to-namespace",
        &surreal_config.surreal_namespace,
        "--to-database",
        &surreal_config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--from",
        &checkpoint_string,
        "--stop-after",
        "15s",
        "--checkpoint-dir",
        &checkpoint_dir,
        "--transforms-config",
        &transforms_path,
    ];
    let stream_output = execute_surreal_sync(&stream_args)?;
    assert_cli_success(
        &stream_output,
        "MySQL binlog stream CLI with --transforms-config mutate",
    );

    let catch_up =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, SyncPhase::CatchUpProgress).await;
    assert!(
        catch_up.is_ok(),
        "stream phase with --checkpoint-dir should persist CatchUpProgress; err={catch_up:?}"
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

    mysql_async::prelude::Queryable::query_drop(&mut mysql_conn, "DROP TABLE IF EXISTS people")
        .await
        .ok();
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
