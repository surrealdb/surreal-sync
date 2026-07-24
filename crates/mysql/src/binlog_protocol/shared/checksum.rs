use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::shared::event_header::EventHeader;
use crate::binlog_protocol::shared::event_type::EventType;

pub fn verify_crc32(data: &[u8], expected: u32) -> Result<(), Error> {
    let computed = crc32fast::hash(data);
    if computed != expected {
        return Err(Error::Protocol(format!(
            "checksum mismatch: expected {expected:#x}, got {computed:#x}"
        )));
    }
    Ok(())
}

/// Return the event body slice, verifying and stripping the CRC32 footer from
/// the full event when checksums are negotiated.
///
/// When `checksum_enabled` is set every event (header + body) is protected by a
/// trailing CRC32; we recompute it and error on mismatch so a corrupt or
/// truncated event fails loudly rather than being silently mis-decoded. Two
/// events are exempt from verification:
/// - The FORMAT_DESCRIPTION event, whose footer is handled by the parser (its
///   last body byte is the checksum-algorithm marker).
/// - The artificial "fake" ROTATE emitted at stream start (`log_pos == 0`),
///   which arrives before any FORMAT_DESCRIPTION event; its checksum handling is
///   version-dependent, so its footer is stripped without verification.
pub fn event_body(full_event: &[u8], checksum_enabled: bool) -> Result<&[u8], Error> {
    if full_event.len() < EventHeader::HEADER_LEN {
        return Err(Error::UnexpectedEof);
    }
    let header = EventHeader::parse(&full_event[..EventHeader::HEADER_LEN], 0)?;
    let has_footer_room = full_event.len() >= EventHeader::HEADER_LEN + 4;
    match header.event_type {
        // The FORMAT_DESCRIPTION event carries the checksum algorithm byte as its
        // last body byte, so its footer is handled by the parser, not stripped here.
        EventType::FormatDescription => Ok(&full_event[EventHeader::HEADER_LEN..]),
        // ROTATE events carry a CRC32 footer whenever checksums are negotiated. It
        // must be stripped: otherwise the trailing CRC bytes leak into the binlog
        // filename and, when the first CRC byte happens to be an ASCII digit,
        // corrupt the tracked position (e.g. `mysql-bin.0000035`). The artificial
        // "fake" rotate (log_pos == 0) precedes the FORMAT_DESCRIPTION event, so
        // its footer is stripped without verification (mid-binlog-without-FDE
        // case); real rotates (log_pos > 0) are verified like any other event.
        EventType::Rotate => {
            if checksum_enabled && has_footer_room {
                if header.log_pos == 0 {
                    Ok(&full_event[EventHeader::HEADER_LEN..full_event.len() - 4])
                } else {
                    verify_and_strip_footer(full_event)
                }
            } else {
                Ok(&full_event[EventHeader::HEADER_LEN..])
            }
        }
        _ => {
            if checksum_enabled && has_footer_room {
                verify_and_strip_footer(full_event)
            } else {
                Ok(&full_event[EventHeader::HEADER_LEN..])
            }
        }
    }
}

/// Verify the trailing CRC32 over `full_event[..len-4]` and return the event body
/// (header and footer removed).
fn verify_and_strip_footer(full_event: &[u8]) -> Result<&[u8], Error> {
    let crc_pos = full_event.len() - 4;
    let expected = u32::from_le_bytes([
        full_event[crc_pos],
        full_event[crc_pos + 1],
        full_event[crc_pos + 2],
        full_event[crc_pos + 3],
    ]);
    verify_crc32(&full_event[..crc_pos], expected)?;
    Ok(&full_event[EventHeader::HEADER_LEN..crc_pos])
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an event (Xid type, `log_pos` > 0) framed with a trailing CRC32 over
    /// the whole event, mirroring the on-wire layout when checksums are negotiated.
    fn framed_event(body: &[u8]) -> Vec<u8> {
        let total = (EventHeader::HEADER_LEN + body.len() + 4) as u32;
        let mut event = Vec::new();
        event.extend_from_slice(&0u32.to_le_bytes()); // timestamp
        event.push(EventType::Xid.code()); // type code
        event.extend_from_slice(&1u32.to_le_bytes()); // server_id
        event.extend_from_slice(&total.to_le_bytes()); // event_size
        event.extend_from_slice(&200u32.to_le_bytes()); // log_pos (> 0)
        event.extend_from_slice(&0u16.to_le_bytes()); // flags
        event.extend_from_slice(body);
        let crc = crc32fast::hash(&event);
        event.extend_from_slice(&crc.to_le_bytes());
        event
    }

    #[test]
    fn valid_crc_parses_and_strips_footer() {
        let body = [0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44];
        let event = framed_event(&body);
        let parsed = event_body(&event, true).expect("valid crc must parse");
        assert_eq!(parsed, &body, "footer stripped, body intact");
    }

    #[test]
    fn corrupted_crc_is_rejected() {
        let body = [0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44];
        let mut event = framed_event(&body);
        // Corrupt a body byte so the recorded CRC no longer matches.
        let idx = EventHeader::HEADER_LEN + 2;
        event[idx] ^= 0xFF;
        let err = event_body(&event, true).expect_err("checksum mismatch must error");
        assert!(
            format!("{err}").contains("checksum mismatch"),
            "expected checksum mismatch, got: {err}"
        );
    }

    #[test]
    fn checksum_disabled_returns_full_body_unverified() {
        // Without negotiated checksums there is no footer to strip or verify;
        // the full post-header payload is returned verbatim.
        let body = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let total = (EventHeader::HEADER_LEN + body.len()) as u32;
        let mut event = Vec::new();
        event.extend_from_slice(&0u32.to_le_bytes());
        event.push(EventType::Xid.code());
        event.extend_from_slice(&1u32.to_le_bytes());
        event.extend_from_slice(&total.to_le_bytes());
        event.extend_from_slice(&200u32.to_le_bytes());
        event.extend_from_slice(&0u16.to_le_bytes());
        event.extend_from_slice(&body);
        let parsed = event_body(&event, false).expect("no-checksum parse");
        assert_eq!(parsed, &body);
    }
}
