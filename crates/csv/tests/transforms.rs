//! Transform pipeline tests for CSV import (identity + external mutate).

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, Row, Value};
use surreal_sync_csv::from_csv::{sync, sync_with_transforms, Config};
use surreal_sync_runtime::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};
use tempfile::NamedTempFile;

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

fn row_name(row: &Row) -> Option<String> {
    match row.fields.get("name")? {
        Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

#[tokio::test]
async fn identity_sync_writes_rows() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "id,name,age").unwrap();
    writeln!(temp_file, "1,Alice,30").unwrap();
    writeln!(temp_file, "2,Bob,25").unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        table: "people".to_string(),
        batch_size: 10,
        id_field: Some("id".to_string()),
        id_columns: Vec::new(),
        dry_run: false,
        ..Default::default()
    };

    let sink = CaptureSink::new();
    sync(&sink, config).await.expect("identity sync");
    assert_eq!(sink.rows_written.load(Ordering::SeqCst), 2);
    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(row_name(&rows[0]).as_deref(), Some("Alice"));
    assert_eq!(row_name(&rows[1]).as_deref(), Some("Bob"));
}

#[tokio::test]
async fn external_mutate_rewrites_name_through_source_driver() {
    let worker = ensure_fixture_worker();
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "id,name,age").unwrap();
    writeln!(temp_file, "1,Alice,30").unwrap();
    writeln!(temp_file, "2,Bob,25").unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        table: "people".to_string(),
        batch_size: 10,
        id_field: Some("id".to_string()),
        id_columns: Vec::new(),
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
async fn composite_id_columns_emit_array_ids() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "a,b,amount").unwrap();
    writeln!(temp_file, "x:y,z,1").unwrap();
    writeln!(temp_file, "x,y:z,2").unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        table: "ledger".to_string(),
        batch_size: 10,
        id_field: None,
        id_columns: vec!["a".to_string(), "b".to_string()],
        dry_run: false,
        ..Default::default()
    };

    let sink = CaptureSink::new();
    sync(&sink, config).await.expect("composite id sync");
    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        match &row.id {
            Value::Array { elements, .. } => {
                assert_eq!(elements.len(), 2);
            }
            other => panic!("expected Array id, got {other:?}"),
        }
    }
    // Extreme `:` values stay distinct as Arrays (would collide if flattened).
    let keys: Vec<Vec<String>> = rows
        .iter()
        .map(|r| match &r.id {
            Value::Array { elements, .. } => elements
                .iter()
                .map(|e| match e {
                    Value::Text(s)
                    | Value::VarChar { value: s, .. }
                    | Value::Char { value: s, .. } => s.clone(),
                    other => panic!("unexpected id part: {other:?}"),
                })
                .collect(),
            _ => unreachable!(),
        })
        .collect();
    assert!(keys.contains(&vec!["x:y".into(), "z".into()]));
    assert!(keys.contains(&vec!["x".into(), "y:z".into()]));
}

#[tokio::test]
async fn flatten_id_toml_rewrites_composite_array_id_to_text() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "a,b,amount").unwrap();
    writeln!(temp_file, "a,b,1").unwrap();
    temp_file.flush().unwrap();

    let config = Config {
        files: vec![temp_file.path().to_path_buf()],
        table: "ledger".to_string(),
        batch_size: 10,
        id_field: None,
        id_columns: vec!["a".to_string(), "b".to_string()],
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
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, Value::Text("a:b".into()));
}
