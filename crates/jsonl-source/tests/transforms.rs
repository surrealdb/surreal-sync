//! Transform pipeline tests for JSONL import (identity + external mutate).
//!
//! Also checks that conversion_rules run before Pipeline stages.

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use surreal_sink::SurrealSink;
use surreal_sync_jsonl_source::{sync, sync_with_transforms, Config};
use sync_core::{UniversalChange, UniversalRelation, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline};
use tempfile::NamedTempFile;

struct CaptureSink {
    rows: Mutex<Vec<UniversalRow>>,
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
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[UniversalRelation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, _change: &UniversalChange) -> anyhow::Result<()> {
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

fn row_parent_thing(row: &UniversalRow) -> Option<(String, String)> {
    match row.fields.get("parent")? {
        UniversalValue::Thing { table, id } => {
            let id_text = match id.as_ref() {
                UniversalValue::Text(s) => s.clone(),
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
    writeln!(
        temp_file,
        r#"{{"id":"1","name":"Alice","value":10}}"#
    )
    .unwrap();
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
async fn external_mutate_rewrites_name_through_write_rows() {
    let worker = ensure_fixture_worker();
    let mut temp_file = NamedTempFile::with_suffix(".jsonl").unwrap();
    writeln!(
        temp_file,
        r#"{{"id":"1","name":"Alice","value":10}}"#
    )
    .unwrap();
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
        )
        .expect("spawn mutate worker"),
    );
    let apply_opts = ApplyOpts::identity().with_batch_size(10);

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
