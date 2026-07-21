//! CLI e2e: `from kafka --transforms-config` with an external mutate worker.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use surreal_sync::testing::cli::{assert_cli_success, execute_surreal_sync};
use surreal_sync::testing::surreal::{cleanup_auto, connect_auto, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_kafka_producer::container::KafkaContainer;
use surreal_sync_kafka_producer::{publish_test_users, KafkaTestProducer};
use tokio::time::sleep;

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
async fn test_kafka_cli_transforms_config_mutate() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let test_id = generate_test_id();
    let work_dir = format!(".test-kafka-transforms-cli-{test_id}");
    std::fs::create_dir_all(&work_dir)?;

    let mut kafka = KafkaContainer::new(&format!("test-kafka-xf-cli-{test_id}"));
    kafka.start()?;
    kafka.wait_until_ready(30).await?;
    let kafka_broker = kafka.broker_address.clone();

    let topic = format!("test-users-xf-cli-{test_id}");
    let producer = KafkaTestProducer::new(&kafka_broker).await?;
    producer.create_topic_if_not_exists(&topic, 1).await?;
    sleep(Duration::from_millis(500)).await;

    publish_test_users(&producer, &topic).await?;
    sleep(Duration::from_millis(200)).await;

    let proto_path = format!("{work_dir}/user.proto");
    std::fs::write(
        &proto_path,
        include_str!("../../crates/kafka-producer/proto/user.proto"),
    )?;

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

    let group_id = format!("cli-xf-{test_id}");
    let args = [
        "from",
        "kafka",
        "--brokers",
        &kafka_broker,
        "--group-id",
        &group_id,
        "--topic",
        &topic,
        "--proto-path",
        &proto_path,
        "--message-type",
        "User",
        "--table-name",
        "people",
        "--max-messages",
        "2",
        "--timeout",
        "30s",
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
    assert_cli_success(&output, "Kafka CLI with --transforms-config mutate");

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
