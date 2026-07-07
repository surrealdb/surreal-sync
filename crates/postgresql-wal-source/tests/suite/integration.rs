//! DDL refresh and table-rename behaviour during steady-state streaming.

use anyhow::Result;
use surreal_sync_postgresql_wal_source::{
    run_replication_tail_with_checkpoints, ReplicationTailOptions,
};
use sync_core::{UniversalChange, UniversalValue};

struct CaptureSink {
    changes: std::sync::Mutex<Vec<UniversalChange>>,
}

#[async_trait::async_trait]
impl surreal_sink::SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, _rows: &[sync_core::UniversalRow]) -> anyhow::Result<()> {
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

#[tokio::test]
async fn ddl_refreshes_schema_before_subsequent_rows() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_postgresql_wal().await;
    let db_name = "integ_ddl_refresh";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;
    let slot = "integ_ddl_slot";
    let publication = "integ_ddl_pub";

    let client = crate::shared::pg_connect(&conn_str).await?;
    client
        .batch_execute(
            "CREATE TYPE widget_status AS ENUM ('new'); \
             CREATE TABLE widgets (id INT PRIMARY KEY, status widget_status NOT NULL)",
        )
        .await?;

    let checkpoint =
        crate::shared::capture_head(&conn_str, slot, publication, vec!["widgets".to_string()])
            .await?;

    client
        .batch_execute("ALTER TYPE widget_status ADD VALUE IF NOT EXISTS 'done'")
        .await?;
    client
        .execute("INSERT INTO widgets (id, status) VALUES (1, 'done')", &[])
        .await?;

    let sink = CaptureSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts =
        crate::shared::source_opts(&conn_str, slot, publication, vec!["widgets".to_string()]);

    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    let status = changes
        .iter()
        .find_map(|change| change.data.as_ref()?.get("status"))
        .expect("status field should be present after DDL refresh");
    match status {
        UniversalValue::Enum { value, .. } => assert_eq!(value, "done"),
        other => panic!("expected refreshed ENUM label, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn rename_table_mid_stream_keeps_tracking_new_name() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_postgresql_wal().await;
    let db_name = "integ_rename";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;
    let slot = "integ_rename_slot";
    let publication = "integ_rename_pub";

    let client = crate::shared::pg_connect(&conn_str).await?;
    client
        .batch_execute("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;

    let checkpoint =
        crate::shared::capture_head(&conn_str, slot, publication, vec!["widgets".to_string()])
            .await?;

    client
        .batch_execute("ALTER TABLE widgets RENAME TO widgets_v2")
        .await?;
    client
        .execute("INSERT INTO widgets_v2 (id, n) VALUES (1, 42)", &[])
        .await?;

    let sink = CaptureSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts =
        crate::shared::source_opts(&conn_str, slot, publication, vec!["widgets".to_string()]);

    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert!(
        changes.iter().any(|c| c.table == "widgets_v2"),
        "expected a change on the renamed table 'widgets_v2', got {changes:?}"
    );

    Ok(())
}
