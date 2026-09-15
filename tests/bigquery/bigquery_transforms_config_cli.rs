//! CLI e2e: `from bigquery --transforms-config` with an external mutate worker.
//!
//! Required by the porting checklist in `docs/source-ports.md`: every source with
//! CLI e2e coverage gets a `--transforms-config` smoke test, so the shared apply
//! path is exercised with a real transform stage and not only the identity one.

use std::path::PathBuf;
use std::process::Command;

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};

use crate::common::{unique_table, BigQueryTestEnv};

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
                "surreal-sync-runtime",
                "--features",
                "test-support",
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
async fn bigquery_full_sync_cli_transforms_config_mutate() -> Result<(), Box<dyn std::error::Error>>
{
    let worker = ensure_fixture_worker();
    let (env, source_opts) = BigQueryTestEnv::new(Vec::new(), Vec::new()).await;
    let table = unique_table("bq_xf_cli", env.test_id);

    let qualified = env.qualified(&table);
    env.execute(&format!("CREATE TABLE {qualified} (id INT64, name STRING)"))
        .await;
    env.execute(&format!(
        "INSERT INTO {qualified} (id, name) VALUES (1, 'alice'), (2, 'bob')"
    ))
    .await;

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

    let dir = std::env::temp_dir().join(format!("bq-transforms-{}", env.test_id));
    std::fs::create_dir_all(&dir)?;
    let transforms_path = dir.join("transforms-mutate.toml");
    std::fs::write(&transforms_path, transforms_toml)?;
    let transforms_path_str = transforms_path.to_string_lossy().to_string();

    let args = [
        "from",
        "bigquery",
        "--project-id",
        &source_opts.project_id,
        "--dataset",
        &source_opts.dataset,
        "--api-endpoint",
        &env.api_endpoint,
        "--tables",
        &table,
        "--id-columns",
        "id",
        "--surreal-endpoint",
        &env.config.surreal_endpoint,
        "--to-namespace",
        &env.config.surreal_namespace,
        "--to-database",
        &env.config.surreal_database,
        "--surreal-username",
        "root",
        "--surreal-password",
        "root",
        "--transforms-config",
        &transforms_path_str,
    ];
    let output = execute_surreal_sync(&args)?;
    assert_cli_success(&output, "BigQuery CLI with --transforms-config mutate");

    let rows = env.fetch_rows(&table).await;
    assert_eq!(rows.len(), 2, "expected two rows, got {rows:?}");
    for row in &rows {
        assert_eq!(
            row["name"].as_str(),
            Some("mutated"),
            "the external worker should have rewritten every name: {row:?}"
        );
    }

    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}
