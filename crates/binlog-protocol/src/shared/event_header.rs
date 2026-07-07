use crate::error::Error;
use crate::shared::event_type::EventType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventHeader {
    pub timestamp: u32,
    pub raw_type_code: u8,
    pub event_type: EventType,
    pub server_id: u32,
    pub event_size: u32,
    pub log_pos: u32,
    pub flags: u16,
    pub file_offset: u64,
}

impl EventHeader {
    pub const HEADER_LEN: usize = 19;

    pub fn parse(data: &[u8], file_offset: u64) -> Result<Self, Error> {
        if data.len() < Self::HEADER_LEN {
            return Err(Error::UnexpectedEof);
        }
        let timestamp = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let raw_type_code = data[4];
        let event_type = EventType::from_code(raw_type_code);
        let server_id = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);
        let event_size = u32::from_le_bytes([data[9], data[10], data[11], data[12]]);
        let log_pos = u32::from_le_bytes([data[13], data[14], data[15], data[16]]);
        let flags = u16::from_le_bytes([data[17], data[18]]);
        Ok(Self {
            timestamp,
            raw_type_code,
            event_type,
            server_id,
            event_size,
            log_pos,
            flags,
            file_offset,
        })
    }
}
