use crate::error::Error;
use crate::flavor::mariadb::gtid_list::{MariaDbGtid, MariaDbGtidList};
use crate::shared::buf::{read_u32_le, read_u64_le};

/// Parse a MariaDB `GTID_LIST` event body.
///
/// Layout: `count (u32 LE)` followed by `count` entries of
/// `domain_id (u32 LE) + server_id (u32 LE) + seq_no (u64 LE)`.
pub fn parse(body: &[u8]) -> Result<MariaDbGtidList, Error> {
    let mut payload = body;
    let count = read_u32_le(&mut payload)? as usize;
    let mut list = MariaDbGtidList::default();
    for _ in 0..count {
        if payload.len() < 16 {
            break;
        }
        let domain_id = read_u32_le(&mut payload)?;
        let server_id = read_u32_le(&mut payload)?;
        let sequence = read_u64_le(&mut payload)?;
        list.add(MariaDbGtid {
            domain_id,
            server_id,
            sequence,
        })?;
    }
    Ok(list)
}
