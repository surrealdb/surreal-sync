//! CLI e2e: `from postgresql-pgoutput sync --transforms-config` with an external mutate worker.

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
async fn test_postgresql_pgoutput_stream_cli_transforms_config_mutate(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let container = surreal_sync::testing::shared_containers::shared_postgresql_pgoutput().await;

    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-postgresql-pgoutput-transforms-cli-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgresql_pgoutput_test_db(
            container, test_id,
        )
        .await?;

    let (pg_client, connection) =
        tokio_postgres::connect(&test_conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    pg_client
        .batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)")
        .await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_auto(&conn, &["people"]).await?;

    let slot = format!("transforms_slot_{test_id}");
    let publication = format!("transforms_pub_{test_id}");

    let snapshot_args = [
        "from",
        "postgresql-pgoutput",
        "sync",
        "--snapshot-mode",
        "only",
        "--connection-string",
        &test_conn_str,
        "--tables",
        "people",
        "--slot",
        &slot,
        "--publication",
        &publication,
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
    assert_cli_success(&output, "PostgreSQL pgoutput snapshot phase CLI");

    pg_client
        .execute(
            "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
            &[],
        )
        .await?;

    use checkpoint::{Checkpoint, SyncPhase};
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, SyncPhase::FullSyncStart).await?;
    let wal_checkpoint: surreal_sync_postgresql_pgoutput_source::PgoutputCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = wal_checkpoint.to_cli_string();

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
        "postgresql-pgoutput",
        "sync",
        "--snapshot-mode",
        "never",
        "--connection-string",
        &test_conn_str,
        "--tables",
        "people",
        "--slot",
        &slot,
        "--publication",
        &publication,
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
        "PostgreSQL pgoutput stream CLI with --transforms-config mutate",
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

    let _ = pg_client
        .batch_execute("DROP TABLE IF EXISTS people")
        .await;
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
