//! Transform pipeline tests for BigQuery full sync (identity + external mutate).

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;
use surreal_sync_bigquery::from_bigquery::client::QueryResult;
use surreal_sync_bigquery::from_bigquery::full_sync::apply_query_result_with_transforms;
use surreal_sync_bigquery::from_bigquery::{SourceOpts, SyncOpts, DEFAULT_PAGE_SIZE};
use surreal_sync_bigquery::types::FieldSchema;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, Row, Value};
use surreal_sync_runtime::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};

struct CaptureSink {
    rows: Mutex<Vec<Row>>,
    rows_written: Arc<AtomicUsize>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            rows_written: Arc::new(AtomicUsize::new(0)),
        }
    }
}

fn change_to_row(change: &Change, index: u64) -> Row {
    Row::new(
        change.table.clone(),
        index,
        change.id.clone(),
        change.fields.clone().unwrap_or_default(),
    )
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_rows(&self, rows: &[Row]) -> anyhow::Result<()> {
        self.rows_written.fetch_add(rows.len(), Ordering::SeqCst);
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
        Ok(())
    }

    async fn write_relations(&self, _relations: &[Relation]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> anyhow::Result<()> {
        let mut rows = self.rows.lock().expect("lock");
        let index = rows.len() as u64;
        rows.push(change_to_row(change, index));
        self.rows_written.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn apply_relation_change(
        &self,
        _change: &surreal_sync_core::RelationChange,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

fn fixture_worker_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
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
            .current_dir({
                let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                root.pop();
                root.pop();
                root
            })
            .status()
            .expect("spawn cargo build");
        assert!(status.success());
    }
    path
}

fn sample_result() -> QueryResult {
    // Lowercase `name` matches the fixture mutate worker's field key.
    QueryResult {
        schema: vec![
            FieldSchema::new("id", "INTEGER"),
            FieldSchema::new("name", "STRING"),
        ],
        rows: vec![
            vec![json!("1"), json!("Alice")],
            vec![json!("2"), json!("Bob")],
        ],
    }
}

fn sample_opts() -> (SourceOpts, SyncOpts) {
    let source = SourceOpts {
        project_id: "demo".into(),
        dataset: "app".into(),
        billing_project_id: None,
        credentials_json: None,
        location: None,
        api_endpoint: "http://127.0.0.1:9050".into(),
        tables: vec!["people".into()],
        id_columns: vec!["id".into()],
        page_size: DEFAULT_PAGE_SIZE,
    };
    let sync = SyncOpts {
        batch_size: 10,
        dry_run: false,
    };
    (source, sync)
}

fn row_name(row: &Row) -> Option<String> {
    match row.fields.get("name")? {
        Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

#[tokio::test]
async fn identity_apply_writes_rows() {
    let (source_opts, sync_opts) = sample_opts();
    let result = sample_result();
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    let sink = CaptureSink::new();

    let written = apply_query_result_with_transforms(
        &sink,
        "people",
        &result,
        &source_opts,
        &sync_opts,
        &pipeline,
        &apply_opts,
    )
    .await
    .expect("identity apply");

    assert_eq!(written, 2);
    assert_eq!(sink.rows_written.load(Ordering::SeqCst), 2);
    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(row_name(&rows[0]).as_deref(), Some("Alice"));
    assert_eq!(row_name(&rows[1]).as_deref(), Some("Bob"));
    // The id column becomes the record ID and is not duplicated as a field.
    assert_eq!(rows[0].id, Value::Int64(1));
    assert!(!rows[0].fields.contains_key("id"));
}

#[tokio::test]
async fn external_mutate_rewrites_name_through_row_chunk_driver() {
    let worker = ensure_fixture_worker();
    let (source_opts, sync_opts) = sample_opts();
    let result = sample_result();

    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            vec![worker.to_string_lossy().to_string(), "mutate".to_string()],
            FramerKind::Ndjson,
        )
        .expect("spawn mutate worker"),
    );
    let apply_opts = ApplyOpts::identity()
        .with_batch_size(10)
        .with_max_in_flight(2);

    let sink = CaptureSink::new();
    apply_query_result_with_transforms(
        &sink,
        "people",
        &result,
        &source_opts,
        &sync_opts,
        &pipeline,
        &apply_opts,
    )
    .await
    .expect("mutate apply");

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row_name(row).as_deref(), Some("mutated"));
    }
}
