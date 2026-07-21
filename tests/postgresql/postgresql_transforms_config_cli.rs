//! CLI e2e: `from postgresql-trigger incremental --transforms-config` with an external mutate worker.

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
async fn test_postgresql_trigger_incremental_cli_transforms_config_mutate(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let container = surreal_sync::testing::shared_containers::shared_postgres().await;

    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-pg-trigger-transforms-cli-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let test_conn_str =
        surreal_sync::testing::shared_containers::create_postgres_test_db(container, test_id)
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

    let full_args = [
        "from",
        "postgresql-trigger",
        "full",
        "--connection-string",
        &test_conn_str,
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
    let output = execute_surreal_sync(&full_args)?;
    assert_cli_success(&output, "PostgreSQL trigger full sync CLI");

    pg_client
        .execute(
            "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
            &[],
        )
        .await?;

    use checkpoint::{Checkpoint, SyncPhase};
    let checkpoint_file =
        checkpoint::get_checkpoint_for_phase(&checkpoint_dir, SyncPhase::FullSyncStart).await?;
    let pg_checkpoint: surreal_sync_postgresql_trigger_source::PostgreSQLCheckpoint =
        checkpoint_file.parse()?;
    let checkpoint_string = pg_checkpoint.to_cli_string();

    let transforms_toml = format!(
        r#"
[[transforms]]
type = "external"
failure_policy = "fail"
batch_size = 1
batch_max_wait = "500ms"
timeout = "60s"
max_in_flight = 1
transport = "stdin"
stdin.mode = "persistent"
stdin.command = ["{}", "mutate"]
stdin.framer = "ndjson"
"#,
        worker.display()
    );
    let transforms_path = format!("{checkpoint_dir}/transforms-mutate.toml");
    std::fs::create_dir_all(&checkpoint_dir)?;
    std::fs::write(&transforms_path, transforms_toml)?;

    let incr_args = [
        "from",
        "postgresql-trigger",
        "incremental",
        "--connection-string",
        &test_conn_str,
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
        "--incremental-from",
        &checkpoint_string,
        "--timeout",
        "20",
        "--transforms-config",
        &transforms_path,
    ];
    let incr_output = execute_surreal_sync(&incr_args)?;
    assert_cli_success(
        &incr_output,
        "PostgreSQL trigger incremental CLI with --transforms-config mutate",
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

    let _ = std::fs::remove_dir_all(&checkpoint_dir);
    Ok(())
}
