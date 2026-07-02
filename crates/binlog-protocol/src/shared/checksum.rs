use crate::error::Error;
use crate::shared::event_header::EventHeader;
use crate::shared::event_type::EventType;

pub fn verify_crc32(data: &[u8], expected: u32) -> Result<(), Error> {
    let computed = crc32fast::hash(data);
    if computed != expected {
        return Err(Error::Protocol(format!(
            "checksum mismatch: expected {expected:#x}, got {computed:#x}"
        )));
    }
    Ok(())
}

/// Return the event body slice, stripping a CRC32 footer from the full event when present.
pub fn event_body(full_event: &[u8], _checksum_enabled: bool) -> Result<&[u8], Error> {
    if full_event.len() < EventHeader::HEADER_LEN {
        return Err(Error::UnexpectedEof);
    }
    let header = EventHeader::parse(&full_event[..EventHeader::HEADER_LEN], 0)?;
    let mut end = full_event.len();
    if !matches!(
        header.event_type,
        EventType::FormatDescription | EventType::Rotate
    ) && full_event.len() >= EventHeader::HEADER_LEN + 4
    {
        end = full_event.len() - 4;
    }
    Ok(&full_event[EventHeader::HEADER_LEN..end])
}

pub fn strip_crc32(body: &[u8]) -> Result<&[u8], Error> {
    if body.len() < 4 {
        return Err(Error::UnexpectedEof);
    }
    let crc_pos = body.len() - 4;
    let expected = u32::from_le_bytes([
        body[crc_pos],
        body[crc_pos + 1],
        body[crc_pos + 2],
        body[crc_pos + 3],
    ]);
    let payload = &body[..crc_pos];
    verify_crc32(payload, expected)?;
    Ok(payload)
}

/// Strip a CRC32 footer without verifying (used when starting mid-binlog without FDE).
pub fn strip_crc32_unchecked(body: &[u8]) -> Result<&[u8], Error> {
    if body.len() < 4 {
        return Err(Error::UnexpectedEof);
    }
    Ok(&body[..body.len() - 4])
}
