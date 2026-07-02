use crate::error::Error;
use crate::flavor::mysql::gtid_set::MySqlGtidSet;
use crate::options::ResumePosition;
use crate::shared::wire::PacketChannel;

pub async fn encode_dump_gtid(
    channel: &mut PacketChannel,
    server_id: u32,
    gtid_set: &MySqlGtidSet,
) -> Result<(), Error> {
    let gtid_data = gtid_set.to_string();
    let mut payload = Vec::new();
    payload.extend_from_slice(&server_id.to_le_bytes());
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&4u32.to_le_bytes());
    payload.extend_from_slice(gtid_data.as_bytes());
    payload.push(0);
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
