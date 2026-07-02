use crate::error::Error;
use crate::flavor::mariadb::gtid_list::MariaDbGtidList;
use crate::shared::buf::read_u64_le;

pub fn parse(body: &[u8]) -> Result<MariaDbGtidList, Error> {
    let mut payload = body;
    let _count = read_u64_le(&mut payload)?;
    let mut list = MariaDbGtidList::default();
    while payload.len() >= 4 + 4 + 8 {
        let gtid = crate::flavor::mariadb::gtid_event::parse(payload)?;
        if let crate::types::GtidMarker::MariaDb {
            domain_id,
            server_id,
            sequence,
        } = gtid
        {
            list.add(crate::flavor::mariadb::gtid_list::MariaDbGtid {
                domain_id,
                server_id,
                sequence,
            })?;
        }
        payload = &payload[4 + 4 + 8 + 1..];
    }
    Ok(list)
}
