//! Transform pipeline e2e for Kafka source (identity path stays in
//! `incremental_sync_lib`; this file covers external mutate via SourceDriver).

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use surreal_sink::SurrealSink;
use surreal_sync::testing::generate_test_id;
use surreal_sync_kafka_producer::container::KafkaContainer;
use surreal_sync_kafka_producer::{publish_test_users, KafkaTestProducer};
use surreal_sync_kafka_source::Config as KafkaConfig;
use sync_core::{UniversalChange, UniversalRelation, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline};
use tokio::time::sleep;

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, _rows: &[UniversalRow]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[UniversalRelation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        _change: &sync_core::UniversalRelationChange,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

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
            .expect("spawn cargo build");
        assert!(status.success());
    }
    path
}

fn change_name(change: &UniversalChange) -> Option<String> {
    let data = change.data.as_ref()?;
    match data.get("name")? {
        UniversalValue::Text(value) => Some(value.clone()),
        UniversalValue::VarChar { value, .. } => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

#[tokio::test]
async fn kafka_external_mutate_rewrites_name_through_source_driver(
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("surreal_sync_kafka_source=info")
        .try_init()
        .ok();

    let worker = ensure_fixture_worker();
    let test_id = generate_test_id();

    let mut kafka = KafkaContainer::new(&format!("test-kafka-xf-{test_id}"));
    kafka.start()?;
    kafka.wait_until_ready(30).await?;
    let kafka_broker = &kafka.broker_address;

    let topic = format!("test-users-xf-{test_id}");
    let producer = KafkaTestProducer::new(kafka_broker).await?;
    producer.create_topic_if_not_exists(&topic, 1).await?;
    sleep(Duration::from_millis(500)).await;

    publish_test_users(&producer, &topic).await?;
    sleep(Duration::from_millis(200)).await;

    let proto_dir = tempfile::tempdir()?;
    let user_proto_path = proto_dir.path().join("user.proto");
    std::fs::write(
        &user_proto_path,
        include_str!("../../crates/kafka-producer/proto/user.proto"),
    )?;

    let config = KafkaConfig {
        proto_path: user_proto_path.to_string_lossy().to_string(),
        brokers: vec![kafka_broker.to_string()],
        group_id: format!("test-group-xf-{test_id}"),
        topic: topic.clone(),
        message_type: "User".to_string(),
        buffer_size: 1000,
        session_timeout_ms: "6000".to_string(),
        num_consumers: 1,
        kafka_batch_size: 100,
        table_name: Some("people".to_string()),
        use_message_key_as_id: false,
        id_field: "id".to_string(),
        max_messages: Some(2),
        sasl_username: None,
        sasl_password: None,
        sasl_mechanism: None,
        security_protocol: None,
        ssl_ca_location: None,
        ssl_certificate_location: None,
        ssl_key_location: None,
        ssl_key_password: None,
    };

    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            vec![worker.to_string_lossy().to_string(), "mutate".to_string()],
        )
        .expect("spawn mutate worker"),
    );
    let apply_opts = ApplyOpts::identity()
        .with_batch_size(100)
        .with_max_in_flight(2);

    let sink = Arc::new(CaptureSink::new());
    let deadline = Utc::now() + chrono::Duration::seconds(20);
    surreal_sync_kafka_source::run_incremental_sync_with_transforms(
        sink.clone(),
        config,
        deadline,
        None,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(
        changes.len(),
        2,
        "expected two mutated changes, got {changes:?}"
    );
    for change in &changes {
        assert_eq!(change_name(change).as_deref(), Some("mutated"));
    }

    // Distinct record ids within the poll batch (indices are assigned before
    // converting rows to upsert changes for the SourceDriver window).
    let ids: Vec<_> = changes.iter().map(|c| c.id.clone()).collect();
    let unique: std::collections::HashSet<_> = ids.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "message ids must stay unique within a Kafka poll batch; got {ids:?}"
    );

    Ok(())
}
