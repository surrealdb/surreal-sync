//! Signal/watermark table helpers (binlog-native, no triggers).

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
