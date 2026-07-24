//! Signal/watermark table helpers for PostgreSQL WAL sync.

use anyhow::{anyhow, Result};
use surreal_sync_runtime::SnapshotSignal;
use tokio_postgres::Client;
use uuid::Uuid;

pub const SIGNAL_TABLE: &str = "surreal_sync_signal";

pub(crate) const EXECUTE_SNAPSHOT_KIND: &str = "execute-snapshot";

pub(crate) fn create_signal_table_sql() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {SIGNAL_TABLE} (\
            id UUID PRIMARY KEY, \
            kind TEXT NOT NULL, \
            tables TEXT, \
            consumed BOOLEAN NOT NULL DEFAULT FALSE);
         ALTER TABLE {SIGNAL_TABLE} REPLICA IDENTITY FULL;"
    )
}

fn parse_signal_tables(payload: Option<&str>) -> Vec<String> {
    payload
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

pub(crate) async fn read_pending_execute_snapshot_signals(
    client: &Client,
) -> Result<Vec<SnapshotSignal>> {
    let rows = client
        .query(
            &format!(
                "SELECT id::text, tables FROM {SIGNAL_TABLE} \
                 WHERE kind = $1 AND consumed = FALSE ORDER BY id"
            ),
            &[&EXECUTE_SNAPSHOT_KIND],
        )
        .await?;

    let mut signals = Vec::new();
    for row in rows {
        let id: String = row.get(0);
        let tables_json: Option<String> = row.get(1);
        let tables = parse_signal_tables(tables_json.as_deref());
        signals.push(SnapshotSignal { id, tables });
    }
    Ok(signals)
}

pub(crate) async fn acknowledge_execute_snapshot_signal(
    client: &Client,
    signal_id: &str,
) -> Result<()> {
    let uuid = Uuid::parse_str(signal_id)
        .map_err(|e| anyhow!("invalid signal UUID '{signal_id}': {e}"))?;
    client
        .execute(
            &format!("UPDATE {SIGNAL_TABLE} SET consumed = TRUE WHERE id = $1"),
            &[&uuid],
        )
        .await?;
    Ok(())
}

pub async fn request_snapshot(client: &Client, tables: &[String]) -> Result<()> {
    client.batch_execute(&create_signal_table_sql()).await?;
    let id = Uuid::new_v4();
    let tables_json = serde_json::to_string(tables)?;
    client
        .execute(
            &format!(
                "INSERT INTO {SIGNAL_TABLE} (id, kind, tables, consumed) VALUES ($1, $2, $3, FALSE)"
            ),
            &[&id, &EXECUTE_SNAPSHOT_KIND, &tables_json],
        )
        .await?;
    Ok(())
}
