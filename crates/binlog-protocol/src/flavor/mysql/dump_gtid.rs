use crate::error::Error;
use crate::flavor::mysql::gtid_set::MySqlGtidSet;
use crate::options::ResumePosition;
use crate::shared::wire::PacketChannel;

/// `BINLOG_THROUGH_GTID` — tells the server to interpret the trailing GTID data
/// block and stream every transaction *not* already present in it. Without this
/// flag the server ignores the GTID data entirely.
const BINLOG_THROUGH_GTID: u16 = 0x04;

/// Issue `COM_BINLOG_DUMP_GTID` (0x1e) to resume streaming from `gtid_set`.
///
/// Wire layout (all integers little-endian):
/// ```text
/// u16        flags (BINLOG_THROUGH_GTID)
/// u32        server-id
/// u32        binlog-filename length (0)
/// string     binlog-filename (empty)
/// u64        binlog-position (4)
/// u32        gtid-data length
/// bytes      gtid-data (binary Gtid_set encoding)
/// ```
pub async fn encode_dump_gtid(
    channel: &mut PacketChannel,
    server_id: u32,
    gtid_set: &MySqlGtidSet,
) -> Result<(), Error> {
    let gtid_data = gtid_set.to_binary();
    let mut payload = Vec::new();
    payload.extend_from_slice(&BINLOG_THROUGH_GTID.to_le_bytes());
    payload.extend_from_slice(&server_id.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes()); // empty binlog filename
    payload.extend_from_slice(&4u64.to_le_bytes()); // binlog position
    payload.extend_from_slice(&(gtid_data.len() as u32).to_le_bytes());
    payload.extend_from_slice(&gtid_data);
    channel.write_command(0x1e, &payload).await
}

pub async fn dump_from_resume(
    channel: &mut PacketChannel,
    server_id: u32,
    resume: &ResumePosition,
    filename: &str,
    position: u32,
) -> Result<(), Error> {
    match resume {
        ResumePosition::MySqlGtid(set) => encode_dump_gtid(channel, server_id, set).await,
        _ => {
            crate::shared::wire::encode_binlog_dump_async(channel, server_id, filename, position)
                .await
        }
    }
}
