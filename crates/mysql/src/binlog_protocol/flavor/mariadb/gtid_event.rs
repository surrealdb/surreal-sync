use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::shared::buf::{read_u32_le, read_u64_le, read_u8};
use crate::binlog_protocol::types::GtidMarker;

/// Parse a MariaDB `GTID_EVENT` (type 0xA2) body.
///
/// The wire layout is `seq_no (u64 LE) + domain_id (u32 LE) + flags2 (u8) + ...`;
/// the `server_id` is carried in the common event header (not the body), so it is
/// supplied by the caller.
pub fn parse(body: &[u8], server_id: u32) -> Result<GtidMarker, Error> {
    let mut payload = body;
    let sequence = read_u64_le(&mut payload)?;
    let domain_id = read_u32_le(&mut payload)?;
    let _flags2 = read_u8(&mut payload)?;
    Ok(GtidMarker::MariaDb {
        domain_id,
        server_id,
        sequence,
    })
}
