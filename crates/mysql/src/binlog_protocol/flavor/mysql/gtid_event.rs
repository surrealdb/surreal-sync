use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::shared::buf::{read_u64_le, read_u8};
use crate::binlog_protocol::types::GtidMarker;

pub fn parse(body: &[u8]) -> Result<GtidMarker, Error> {
    let mut payload = body;
    let _flags = read_u8(&mut payload)?;
    let mut source_id = [0u8; 16];
    if payload.len() < 16 {
        return Err(Error::UnexpectedEof);
    }
    source_id.copy_from_slice(&payload[..16]);
    payload = &payload[16..];
    let gno = read_u64_le(&mut payload)?;
    Ok(GtidMarker::MySql { source_id, gno })
}
