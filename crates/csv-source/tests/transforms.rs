//! Transform pipeline tests for CSV import (identity + external mutate).

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use surreal_sink::SurrealSink;
use surreal_sync_csv_source::{sync, sync_with_transforms, Config};
use sync_core::{UniversalChange, UniversalRelation, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};
use tempfile::NamedTempFile;

struct CaptureSink {
    rows: Mutex<Vec<UniversalRow>>,
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

fn change_to_row(change: &UniversalChange, index: u64) -> UniversalRow {
    UniversalRow::new(
        change.table.clone(),
        index,
        change.id.clone(),
        change.data.clone().unwrap_or_default(),
    )
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        self.rows_written.fetch_add(rows.len(), Ordering::SeqCst);
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[UniversalRelation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        let mut rows = self.rows.lock().expect("lock");
        let index = rows.len() as u64;
        rows.push(change_to_row(change, index));
        self.rows_written.fetch_add(1, Ordering::SeqCst);
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
                "sync-transform",
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

fn row_name(row: &UniversalRow) -> Option<String> {
    match row.fields.get("name")? {
        UniversalValue::Text(value) => Some(value.clone()),
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
            UniversalValue::Array { elements, .. } => {
                assert_eq!(elements.len(), 2);
            }
            other => panic!("expected Array id, got {other:?}"),
        }
    }
    // Extreme `:` values stay distinct as Arrays (would collide if flattened).
    let keys: Vec<Vec<String>> = rows
        .iter()
        .map(|r| match &r.id {
            UniversalValue::Array { elements, .. } => elements
                .iter()
                .map(|e| match e {
                    UniversalValue::Text(s)
                    | UniversalValue::VarChar { value: s, .. }
                    | UniversalValue::Char { value: s, .. } => s.clone(),
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

    let cfg = sync_transform::parse_transforms_toml(
        r#"
[[transforms]]
type = "flatten_id"
"#,
    )
    .expect("parse flatten_id toml");
    let pipeline = sync_transform::Pipeline::from_config(&cfg).expect("pipeline");
    let apply_opts = sync_transform::ApplyOpts::from_transforms_config(&cfg);

    let sink = CaptureSink::new();
    sync_with_transforms(&sink, config, &pipeline, &apply_opts)
        .await
        .expect("flatten_id sync");

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, UniversalValue::Text("a:b".into()));
}
