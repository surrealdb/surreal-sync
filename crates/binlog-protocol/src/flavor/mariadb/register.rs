use crate::error::Error;
use crate::flavor::mariadb::gtid_list::MariaDbGtidList;
use crate::options::{MariaDbDumpFlags, MariaDbGtidStrictMode};
use crate::shared::wire::PacketChannel;

pub const MARIA_DB_BINLOG_SEND_ANNOTATE_ROWS: u32 = 0x01;

pub async fn register_session_vars(
    channel: &mut PacketChannel,
    flags: &MariaDbDumpFlags,
) -> Result<(), Error> {
    if flags.send_annotate_rows {
        channel.query("SET @mariadb_slave_capability=4").await?;
    }
    Ok(())
}

/// Register the session variables MariaDB needs before a GTID-mode
/// `COM_BINLOG_DUMP`. MariaDB has no `COM_BINLOG_DUMP_GTID`; instead the server
/// resolves the start position from `@slave_connect_state` when the dump is
/// issued with an empty filename and position 4.
pub async fn register_gtid_session_vars(
    channel: &mut PacketChannel,
    gtid_list: &MariaDbGtidList,
    strict_mode: MariaDbGtidStrictMode,
) -> Result<(), Error> {
    channel.query("SET @mariadb_slave_capability=4").await?;
    let connect_state = gtid_list.to_connect_state();
    // Single-quoted string literal; GTID lists only contain digits, '-' and ','.
    channel
        .query(&format!("SET @slave_connect_state='{connect_state}'"))
        .await?;
    if let Some(statement) = gtid_strict_mode_set_statement(strict_mode) {
        channel.query(statement).await?;
    }
    channel.query("SET @slave_gtid_ignore_duplicates=1").await?;
    Ok(())
}

pub fn gtid_strict_mode_set_statement(mode: MariaDbGtidStrictMode) -> Option<&'static str> {
    match mode {
        MariaDbGtidStrictMode::ServerDefault => None,
        MariaDbGtidStrictMode::On => Some("SET @slave_gtid_strict_mode=1"),
        MariaDbGtidStrictMode::Off => Some("SET @slave_gtid_strict_mode=0"),
    }
}

pub fn annotate_rows_enabled(flags: &MariaDbDumpFlags) -> bool {
    flags.send_annotate_rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_mode_statement_respects_option() {
        assert_eq!(
            gtid_strict_mode_set_statement(MariaDbGtidStrictMode::ServerDefault),
            None
        );
        assert_eq!(
            gtid_strict_mode_set_statement(MariaDbGtidStrictMode::On),
            Some("SET @slave_gtid_strict_mode=1")
        );
        assert_eq!(
            gtid_strict_mode_set_statement(MariaDbGtidStrictMode::Off),
            Some("SET @slave_gtid_strict_mode=0")
        );
    }
}
