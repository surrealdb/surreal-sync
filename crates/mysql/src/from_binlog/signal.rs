//! Signal/watermark table helpers (binlog-native, no triggers).

use anyhow::{anyhow, Result};
use mysql_async::{prelude::*, Pool};
use surreal_sync_runtime::SnapshotSignal;

use crate::from_binlog::client::use_database;

/// Name of the signal/watermark table created on the source.
pub const SIGNAL_TABLE: &str = "surreal_sync_signal";

/// `kind` value identifying an ad-hoc `execute-snapshot` request row.
pub(crate) const EXECUTE_SNAPSHOT_KIND: &str = "execute-snapshot";

/// SQL that creates the signal table shared by watermark sync and ad-hoc snapshot.
pub(crate) fn create_signal_table_sql() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {SIGNAL_TABLE} (\
            id CHAR(36) PRIMARY KEY, \
            kind VARCHAR(32) NOT NULL, \
            tables TEXT NULL, \
            consumed TINYINT(1) NOT NULL DEFAULT 0)"
    )
}

fn parse_signal_tables(payload: Option<&str>) -> Vec<String> {
    payload
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

/// Read pending `execute-snapshot` rows without marking them consumed.
pub(crate) async fn read_pending_execute_snapshot_signals(
    pool: &Pool,
    database: &str,
) -> Result<Vec<SnapshotSignal>> {
    let mut conn = pool.get_conn().await?;
    use_database(&mut conn, database).await?;
    let rows: Vec<mysql_async::Row> = conn
        .exec(
            format!(
                "SELECT id, tables FROM {SIGNAL_TABLE} \
                 WHERE kind = ? AND consumed = 0 ORDER BY id"
            ),
            (EXECUTE_SNAPSHOT_KIND,),
        )
        .await?;

    let mut signals = Vec::new();
    for row in rows {
        let id: String = row.get(0).ok_or_else(|| anyhow!("missing signal id"))?;
        let tables_json: Option<String> = row.get(1);
        let tables = parse_signal_tables(tables_json.as_deref());
        signals.push(SnapshotSignal { id, tables });
    }
    Ok(signals)
}

/// Mark one `execute-snapshot` row consumed after its snapshot succeeds.
pub(crate) async fn acknowledge_execute_snapshot_signal(
    pool: &Pool,
    database: &str,
    signal_id: &str,
) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    use_database(&mut conn, database).await?;
    conn.exec_drop(
        format!("UPDATE {SIGNAL_TABLE} SET consumed = 1 WHERE id = ?"),
        (signal_id,),
    )
    .await?;
    Ok(())
}
