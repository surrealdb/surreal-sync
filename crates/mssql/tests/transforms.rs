//! Transform pipeline tests (identity + in-place mutate). No SQL Server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Relation, Row, Value};
use surreal_sync_runtime::{write_rows, ApplyOpts, InPlaceTransform, Pipeline};

struct CaptureSink {
    rows: Mutex<Vec<Row>>,
    rows_written: AtomicUsize,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            rows_written: AtomicUsize::new(0),
        }
    }
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
        rows.push(Row::new(
            change.table.clone(),
            index,
            change.id.clone(),
            change.fields.clone().unwrap_or_default(),
        ));
        self.rows_written.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn apply_relation_change(
        &self,
        _change: &surreal_sync_core::RelationChange,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn query(&self, _sql: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

fn fixture_rows() -> Vec<Row> {
    let mut a = HashMap::new();
    a.insert("name".into(), Value::Text("Alice".into()));
    let mut b = HashMap::new();
    b.insert("name".into(), Value::Text("Bob".into()));
    vec![
        Row::new("people", 0, Value::Int64(1), a),
        Row::new("people", 1, Value::Int64(2), b),
    ]
}

fn row_name(row: &Row) -> Option<String> {
    match row.fields.get("name")? {
        Value::Text(value) => Some(value.clone()),
        other => panic!("unexpected name: {other:?}"),
    }
}

struct MutateName;

impl InPlaceTransform for MutateName {
    fn transform(
        &self,
        _table: &str,
        _id: &mut Value,
        fields: Option<&mut HashMap<String, Value>>,
    ) -> anyhow::Result<()> {
        if let Some(fields) = fields {
            fields.insert("name".into(), Value::Text("mutated".into()));
        }
        Ok(())
    }
}

#[tokio::test]
async fn identity_apply_writes_rows() {
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    let sink = CaptureSink::new();

    write_rows(&sink, &pipeline, fixture_rows(), &apply_opts)
        .await
        .expect("identity apply");

    assert_eq!(sink.rows_written.load(Ordering::SeqCst), 2);
    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(row_name(&rows[0]).as_deref(), Some("Alice"));
    assert_eq!(row_name(&rows[1]).as_deref(), Some("Bob"));
}

#[tokio::test]
async fn inplace_mutate_rewrites_name() {
    let mut pipeline = Pipeline::new();
    pipeline.push_inplace(MutateName);
    let apply_opts = ApplyOpts::identity()
        .with_batch_size(10)
        .with_max_in_flight(2);
    let sink = CaptureSink::new();

    write_rows(&sink, &pipeline, fixture_rows(), &apply_opts)
        .await
        .expect("mutate apply");

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row_name(row).as_deref(), Some("mutated"));
    }
}
