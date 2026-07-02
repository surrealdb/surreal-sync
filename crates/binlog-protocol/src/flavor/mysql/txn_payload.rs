use crate::error::Error;
use crate::event::{EventParser, RawEvent};
use crate::shared::checksum::event_body;
use crate::shared::event_header::EventHeader;

pub fn unwrap(body: &[u8], parser: &mut EventParser) -> Result<Vec<RawEvent>, Error> {
    if body.len() < 8 {
        return Err(Error::UnexpectedEof);
    }
    let payload_size = u32::from_le_bytes([body[4], body[5], body[6], body[7]]) as usize;
    let payload = &body[8..8 + payload_size.min(body.len().saturating_sub(8))];
    let mut events = Vec::new();
    let mut offset = 0usize;
    while offset + EventHeader::HEADER_LEN <= payload.len() {
        let header = EventHeader::parse(&payload[offset..], offset as u64)?;
        let end = offset + header.event_size as usize;
        if end > payload.len() {
            break;
        }
        let inner_event = &payload[offset..end];
        let body = event_body(inner_event, parser.checksum_enabled())?;
        let event = parser.parse(header, body)?;
        offset = end;
        events.push(event);
    }
    Ok(events)
}
