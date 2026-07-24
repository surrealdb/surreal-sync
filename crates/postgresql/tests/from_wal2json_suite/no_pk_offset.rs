//! No-PK OFFSET/LIMIT full sync (wal2json) — proves the empty-PK branch is reachable.

use std::collections::HashSet;
use std::sync::Mutex;

use anyhow::Result;
use surreal_sync_core::SurrealSink;
use surreal_sync_core::{Change, Row, Value};
use surreal_sync_postgresql::from_wal2json::{run_full_sync_with_transforms, SourceOpts};
use surreal_sync_postgresql::{get_primary_key_columns, read_offset_table_chunk};
use surreal_sync_runtime::{ApplyOpts, Pipeline};

struct CaptureSink {
    changes: Mutex<Vec<Change>>,
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
    async fn write_rows(&self, rows: &[Row]) -> anyhow::Result<()> {
        // Full sync upserts coalesce to write_rows; mirror into `changes`.
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            changes.push(Change::update(
                row.table.clone(),
                row.id.clone(),
                row.fields.clone(),
            ));
        }
        Ok(())
    }

    async fn write_relations(
        &self,
        _relations: &[surreal_sync_core::Relation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_change(&self, change: &Change) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
        Ok(())
    }

    async fn apply_relation_change(
        &self,
        _change: &surreal_sync_core::RelationChange,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

fn name_of_row(row: &Row) -> String {
    match row.fields.get("name") {
        Some(Value::VarChar { value, .. } | Value::Text(value)) => value.clone(),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn name_of_change(change: &Change) -> String {
    match change.fields.as_ref().and_then(|d| d.get("name")) {
        Some(Value::VarChar { value, .. } | Value::Text(value)) => value.clone(),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn source_opts(conn_str: &str, slot: &str, tables: Vec<String>) -> SourceOpts {
    SourceOpts {
        connection_string: conn_str.to_string(),
        slot_name: slot.to_string(),
        tables,
        schema: "public".to_string(),
        relation_tables: vec![],
    }
}

#[tokio::test]
async fn get_primary_key_columns_returns_empty_for_no_pk_table() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "nopk_probe_w2j").await?;
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE notes (name TEXT NOT NULL)")
        .await?;
    let pks = get_primary_key_columns(&client, "notes").await?;
    assert!(pks.is_empty(), "expected empty PK list, got {pks:?}");
    Ok(())
}

#[tokio::test]
async fn read_offset_table_chunk_assigns_synthetic_ids_with_ctid_order() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "nopk_chunk_w2j").await?;
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "CREATE TABLE notes (name TEXT NOT NULL); \
             INSERT INTO notes (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')",
        )
        .await?;

    let (chunk0, _) = read_offset_table_chunk(&client, "notes", 0, 2, None, &[]).await?;
    let (chunk1, _) = read_offset_table_chunk(&client, "notes", 2, 2, None, &[]).await?;
    let (chunk2, _) = read_offset_table_chunk(&client, "notes", 4, 2, None, &[]).await?;
    assert_eq!(chunk0.len(), 2);
    assert_eq!(chunk1.len(), 2);
    assert_eq!(chunk2.len(), 1);

    let all: Vec<_> = chunk0.into_iter().chain(chunk1).chain(chunk2).collect();
    let names: HashSet<_> = all.iter().map(name_of_row).collect();
    assert_eq!(
        names,
        ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );
    for (i, row) in all.iter().enumerate() {
        assert_eq!(row.id, Value::Int64(i as i64));
        assert_eq!(row.index, i as u64);
    }
    Ok(())
}

#[tokio::test]
async fn full_sync_streams_no_pk_table_via_offset_chunks() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "nopk_full_w2j").await?;
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "CREATE TABLE notes (name TEXT NOT NULL); \
             INSERT INTO notes (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')",
        )
        .await?;

    let sink = CaptureSink::new();
    let sync_opts = surreal_sync_postgresql::SyncOpts {
        batch_size: 2,
        dry_run: false,
    };
    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();

    run_full_sync_with_transforms(
        &sink,
        source_opts(&conn_str, "nopk_full_slot", vec!["notes".to_string()]),
        sync_opts,
        None::<&surreal_sync_core::SyncManager<surreal_sync_core::NullStore>>,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(
        changes.len(),
        5,
        "expected five offset-streamed rows, got {changes:?}"
    );
    let names: HashSet<_> = changes.iter().map(name_of_change).collect();
    assert_eq!(
        names,
        ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );
    for change in &changes {
        assert!(
            matches!(change.id, Value::Int64(_)),
            "no-PK rows should use synthetic Int64 ids, got {:?}",
            change.id
        );
    }
    Ok(())
}
