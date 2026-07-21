//! CLI e2e: `from jsonl --transforms-config` with an external mutate worker.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};

fn fixture_worker_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("target/debug/sync-transform-fixture-worker");
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
        assert!(status.success());
    }
    path
}

#[tokio::test]
async fn test_jsonl_cli_transforms_config_mutate() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let test_id = generate_test_id();
    let work_dir = format!(".test-jsonl-transforms-cli-{test_id}");
    std::fs::create_dir_all(&work_dir)?;

    let jsonl_path = format!("{work_dir}/people.jsonl");
    let mut f = std::fs::File::create(&jsonl_path)?;
    writeln!(f, r#"{{"id":"1","name":"Alice","value":10}}"#)?;
    writeln!(f, r#"{{"id":"2","name":"Bob","value":20}}"#)?;
    f.flush()?;

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
    let transforms_path = format!("{work_dir}/transforms-mutate.toml");
    std::fs::write(&transforms_path, transforms_toml)?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_auto(&conn, &["people"]).await?;

    let args = [
        "from",
        "jsonl",
        "--path",
        &jsonl_path,
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
        "--transforms-config",
        &transforms_path,
    ];
    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "JSONL CLI with --transforms-config mutate");

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
    assert_eq!(names.len(), 2, "expected two people rows, got {names:?}");
    for name in &names {
        assert_eq!(name.as_deref(), Some("mutated"));
    }

    let _ = std::fs::remove_dir_all(&work_dir);
    Ok(())
}
