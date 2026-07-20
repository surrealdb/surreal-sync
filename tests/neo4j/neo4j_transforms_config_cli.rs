//! CLI e2e: `from neo4j incremental --transforms-config` with an external mutate worker.

use std::path::PathBuf;
use std::process::Command;

use neo4rs::Query;
use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_neo4j_source::testing::container::Neo4jContainer;

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
async fn test_neo4j_incremental_cli_transforms_config_mutate(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let test_id = generate_test_id();
    let checkpoint_dir = format!(".test-neo4j-transforms-cli-checkpoints-{test_id}");
    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;

    let mut container = Neo4jContainer::new(&format!("test-neo4j-xf-cli-{test_id}"));
    container.start()?;
    container.wait_until_ready(60).await?;
    let t1 = chrono::Utc::now();

    let graph_config = neo4rs::ConfigBuilder::default()
        .uri(container.bolt_uri())
        .user(&container.username)
        .password(&container.password)
        .db(&*container.database)
        .build()?;
    let graph = neo4rs::Graph::connect(graph_config)?;
    graph
        .run(Query::new("MATCH (n) DETACH DELETE n".to_string()))
        .await?;

    let surrealdb = surreal_sync::testing::shared_containers::shared_surrealdb();
    let surreal_config = TestConfig::with_surreal_endpoint(test_id, &surrealdb.ws_endpoint());
    let conn = connect_auto(&surreal_config).await?;
    cleanup_auto(&conn, &["person"]).await?;

    let bolt_uri = container.bolt_uri();
    let full_args = [
        "from",
        "neo4j",
        "full",
        "--connection-string",
        &bolt_uri,
        "--database",
        &container.database,
        "--username",
        &container.username,
        "--password",
        &container.password,
        "--tables",
        "Person",
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
        "--timezone",
        "UTC",
        "--allow-empty-tracking-timestamp",
        "--assumed-start-timestamp",
        &t1.to_rfc3339(),
        "--checkpoint-dir",
        &checkpoint_dir,
    ];
    let output = execute_surreal_sync(&full_args)?;
    assert_cli_success(&output, "Neo4j full sync CLI");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    graph
        .run(Query::new(
            "CREATE (a:Person {id: 1, name: 'alice', updated_at: datetime()}), \
             (b:Person {id: 2, name: 'bob', updated_at: datetime()})"
                .to_string(),
        ))
        .await?;

    let checkpoint_string = t1.to_rfc3339();

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
        "neo4j",
        "incremental",
        "--connection-string",
        &bolt_uri,
        "--database",
        &container.database,
        "--username",
        &container.username,
        "--password",
        &container.password,
        "--tables",
        "Person",
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
        "--timezone",
        "UTC",
        "--incremental-from",
        &checkpoint_string,
        "--timeout",
        "30",
        "--transforms-config",
        &transforms_path,
    ];
    let incr_output = execute_surreal_sync(&incr_args)?;
    assert_cli_success(
        &incr_output,
        "Neo4j incremental CLI with --transforms-config mutate",
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
            let mut resp = db.query("SELECT name FROM person").await?;
            let rows: Vec<PeopleRowV2> = resp.take(0)?;
            rows.into_iter().map(|r| r.name).collect()
        }
        SurrealConnection::V3(db) => {
            let mut resp = db.query("SELECT name FROM person").await?;
            let rows: Vec<PeopleRowV3> = resp.take(0)?;
            rows.into_iter().map(|r| r.name).collect()
        }
    };
    assert_eq!(names.len(), 2, "expected two person rows, got {names:?}");
    for name in &names {
        assert_eq!(name.as_deref(), Some("mutated"));
    }

    surreal_sync::testing::checkpoint::cleanup_checkpoint_dir(&checkpoint_dir)?;
    Ok(())
}
