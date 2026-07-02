use crate::error::Error;
use crate::shared::buf::{read_u32_le, read_u64_le, read_u8};
use crate::types::GtidMarker;

pub fn parse(body: &[u8]) -> Result<GtidMarker, Error> {
    let mut payload = body;
    let _commit_flag = read_u8(&mut payload)?;
    let domain_id = read_u32_le(&mut payload)?;
    let server_id = read_u32_le(&mut payload)?;
    let sequence = read_u64_le(&mut payload)?;
    Ok(GtidMarker::MariaDb {
        domain_id,
        server_id,
        sequence,
    })
}
