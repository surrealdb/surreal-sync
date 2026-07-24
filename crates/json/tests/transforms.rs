//! Transform pipeline tests for JSONL import (identity + external mutate).
//!
//! Also checks that conversion_rules run before Pipeline stages.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, Row, Value};
use surreal_sync_json::from_jsonl::{sync, sync_with_transforms, Config};
use surreal_sync_runtime::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};
use tempfile::NamedTempFile;

struct CaptureSink {
    rows: Mutex<Vec<Row>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_rows(&self, rows: &[Row]) -> anyhow::Result<()> {
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
        Ok(())
    }

    async fn write_relations(&self, _relations: &[Relation]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> anyhow::Result<()> {
        let mut rows = self.rows.lock().expect("lock");
        let index = rows.len() as u64;
        rows.push(Row::new(
            change.table.clone(),
            index,
            change.id.clone(),
            change.fields.clone().unwrap_or_default(),
        ));
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

fn row_name(row: &Row) -> Option<String> {
    match row.fields.get("name")? {
        Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn row_parent_thing(row: &Row) -> Option<(String, String)> {
    match row.fields.get("parent")? {
        Value::Thing { table, id } => {
            let id_text = match id.as_ref() {
                Value::Text(s) => s.clone(),
                other => panic!("unexpected thing id: {other:?}"),
            };
            Some((table.clone(), id_text))
        }
        other => panic!("unexpected parent: {other:?}"),
    }
}

#[tokio::test]
async fn identity_sync_writes_rows() {
    let mut temp_file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(temp_file, r#"{{"id":"1","name":"Alice","value":10}}"#).unwrap();
    writeln!(temp_file, r#"{{"id":"2","name":"Bob","value":20}}"#).unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        batch_size: 10,
        dry_run: false,
        ..Default::default()
    };

    let sink = CaptureSink::new();
    sync(&sink, config).await.expect("identity sync");
    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2);
    assert_eq!(row_name(&rows[0]).as_deref(), Some("Alice"));
    assert_eq!(row_name(&rows[1]).as_deref(), Some("Bob"));
}

#[tokio::test]
async fn external_mutate_rewrites_name_through_source_driver() {
    let worker = ensure_fixture_worker();
    let mut temp_file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(temp_file, r#"{{"id":"1","name":"Alice","value":10}}"#).unwrap();
    writeln!(temp_file, r#"{{"id":"2","name":"Bob","value":20}}"#).unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        batch_size: 10,
        dry_run: false,
        ..Default::default()
    };

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
    sync_with_transforms(&sink, config, &pipeline, &apply_opts)
        .await
        .expect("mutate sync");

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row_name(row).as_deref(), Some("mutated"));
    }
}

#[tokio::test]
async fn conversion_rules_run_before_pipeline_stages() {
    let worker = ensure_fixture_worker();
    let mut temp_file = NamedTempFile::with_suffix(".jsonl").unwrap();
    // Nested object matches conversion rule → Thing before Pipeline mutate.
    writeln!(
        temp_file,
        r#"{{"id":"1","name":"Alice","parent":{{"type":"page_id","page_id":"p1"}}}}"#
    )
    .unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        conversion_rules: vec![r#"type="page_id",page_id page:page_id"#.to_string()],
        batch_size: 10,
        dry_run: false,
        ..Default::default()
    };

    let mut pipeline = Pipeline::new();
    pipeline.push_external(
        ExternalTransform::child_stdio(
            ChildStdioMode::Persistent,
            vec![worker.to_string_lossy().to_string(), "mutate".to_string()],
            FramerKind::Ndjson,
        )
        .expect("spawn mutate worker"),
    );
    let apply_opts = ApplyOpts::identity().with_batch_size(10);

    let sink = CaptureSink::new();
    sync_with_transforms(&sink, config, &pipeline, &apply_opts)
        .await
        .expect("rules+mutate sync");

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(row_name(&rows[0]).as_deref(), Some("mutated"));
    assert_eq!(
        row_parent_thing(&rows[0]),
        Some(("page".to_string(), "p1".to_string()))
    );
}

#[tokio::test]
async fn flatten_id_toml_rewrites_composite_array_id_to_text() {
    let mut temp_file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(temp_file, r#"{{"a":"a","b":"b","amount":1}}"#).unwrap();
    writeln!(temp_file, r#"{{"a":1,"b":2,"amount":2}}"#).unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        id_columns: vec!["a".to_string(), "b".to_string()],
        batch_size: 10,
        dry_run: false,
        ..Default::default()
    };

    let cfg = surreal_sync_runtime::parse_transforms_toml(
        r#"
[[transforms]]
type = "flatten_id"
"#,
    )
    .expect("parse flatten_id toml");
    let pipeline = surreal_sync_runtime::Pipeline::from_config(&cfg).expect("pipeline");
    let apply_opts = surreal_sync_runtime::ApplyOpts::from_transforms_config(&cfg);

    let sink = CaptureSink::new();
    sync_with_transforms(&sink, config, &pipeline, &apply_opts)
        .await
        .expect("flatten_id sync");

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, Value::Text("a:b".into()));
    assert_eq!(rows[1].id, Value::Text("1:2".into()));
}
