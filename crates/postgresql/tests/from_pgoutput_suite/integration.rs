//! DDL refresh and table-rename behaviour during steady-state streaming.

use anyhow::Result;
use surreal_sync_core::{Change, Value};
use surreal_sync_postgresql::from_pgoutput::{
    run_replication_tail_with_checkpoints, ReplicationTailOptions,
};

struct CaptureSink {
    changes: std::sync::Mutex<Vec<Change>>,
}

#[async_trait::async_trait]
impl surreal_sync_core::SurrealSink for CaptureSink {
    async fn write_rows(&self, rows: &[surreal_sync_core::Row]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; mirror into `changes`.
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

#[tokio::test]
async fn ddl_refreshes_schema_before_subsequent_rows() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_postgresql_pgoutput().await;
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

    run_replication_tail_with_checkpoints::<_, surreal_sync_core::NullStore>(
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
        .find_map(|change| change.fields.as_ref()?.get("status"))
        .expect("status field should be present after DDL refresh");
    match status {
        Value::Enum { value, .. } => assert_eq!(value, "done"),
        other => panic!("expected refreshed ENUM label, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn rename_table_mid_stream_keeps_tracking_new_name() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_postgresql_pgoutput().await;
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

    run_replication_tail_with_checkpoints::<_, surreal_sync_core::NullStore>(
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
