use crate::error::Error;
use crate::options::MariaDbDumpFlags;
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

pub fn annotate_rows_enabled(flags: &MariaDbDumpFlags) -> bool {
    flags.send_annotate_rows
}
