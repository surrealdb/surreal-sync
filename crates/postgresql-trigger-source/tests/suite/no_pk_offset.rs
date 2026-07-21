//! No-PK OFFSET/LIMIT full sync (trigger) — proves the empty-PK branch is reachable.

use std::collections::HashSet;
use std::sync::Mutex;

use anyhow::Result;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql::{get_primary_key_columns, read_offset_table_chunk};
use surreal_sync_postgresql_trigger_source::{run_full_sync_with_transforms, SourceOpts};
use sync_core::{UniversalChange, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, Pipeline};

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
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        // Full sync upserts coalesce to write_universal_rows; mirror into `changes`.
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            changes.push(UniversalChange::update(
                row.table.clone(),
                row.id.clone(),
                row.fields.clone(),
            ));
        }
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[sync_core::UniversalRelation],
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

fn name_of_row(row: &UniversalRow) -> String {
    match row.fields.get("name") {
        Some(UniversalValue::VarChar { value, .. } | UniversalValue::Text(value)) => value.clone(),
        other => panic!("unexpected name: {other:?}"),
    }
}

fn name_of_change(change: &UniversalChange) -> String {
    match change.data.as_ref().and_then(|d| d.get("name")) {
        Some(UniversalValue::VarChar { value, .. } | UniversalValue::Text(value)) => value.clone(),
        other => panic!("unexpected name: {other:?}"),
    }
}

#[tokio::test]
async fn get_primary_key_columns_returns_empty_for_no_pk_table() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "nopk_probe_trig").await?;
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
    let conn_str = crate::shared::create_test_db(container, "nopk_chunk_trig").await?;
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
        assert_eq!(row.id, UniversalValue::Int64(i as i64));
        assert_eq!(row.index, i as u64);
    }
    Ok(())
}

#[tokio::test]
async fn full_sync_streams_no_pk_table_via_offset_chunks() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn_str = crate::shared::create_test_db(container, "nopk_full_trig").await?;
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
        SourceOpts {
            source_uri: conn_str,
            source_database: Some("public".to_string()),
            tables: vec!["notes".to_string()],
            relation_tables: vec![],
        },
        sync_opts,
        None::<&checkpoint::SyncManager<checkpoint::NullStore>>,
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
            matches!(change.id, UniversalValue::Int64(_)),
            "no-PK rows should use synthetic Int64 ids, got {:?}",
            change.id
        );
    }
    Ok(())
}
