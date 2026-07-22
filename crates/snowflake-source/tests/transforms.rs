//! Transform pipeline tests for Snowflake full sync (identity + external mutate).

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;
use snowflake_types::ColumnType;
use surreal_sink::SurrealSink;
use surreal_sync_snowflake_source::{
    apply_query_result_with_transforms, QueryResult, SourceOpts, SyncOpts,
};
use sync_core::{UniversalChange, UniversalRelation, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, FramerKind, Pipeline};

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

fn sample_result() -> QueryResult {
    // Lowercase `name` matches the fixture mutate worker's field key.
    QueryResult {
        columns: vec![
            ColumnType::new("id", "fixed"),
            ColumnType::new("name", "text"),
        ],
        rows: vec![
            vec![json!("1"), json!("Alice")],
            vec![json!("2"), json!("Bob")],
        ],
    }
}

fn sample_opts() -> (SourceOpts, SyncOpts) {
    let source = SourceOpts {
        account: "unused".into(),
        user: "unused".into(),
        private_key_pem: String::new(),
        private_key_passphrase: None,
        warehouse: "unused".into(),
        database: "DB".into(),
        schema: "PUBLIC".into(),
        role: None,
        tables: vec!["PEOPLE".into()],
        id_columns: vec!["id".into()],
    };
    let sync = SyncOpts {
        batch_size: 10,
        dry_run: false,
    };
    (source, sync)
}

fn row_name(row: &UniversalRow) -> Option<String> {
    match row.fields.get("name")? {
        UniversalValue::Text(value) => Some(value.clone()),
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
        "PEOPLE",
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
        "PEOPLE",
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
