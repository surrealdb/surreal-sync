use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::event::{EventParser, RawEvent};
use crate::binlog_protocol::shared::buf::{read_bytes, read_lenenc_int};
use crate::binlog_protocol::shared::checksum::event_body;
use crate::binlog_protocol::shared::event_header::EventHeader;

const OTW_PAYLOAD_HEADER_END_MARK: u64 = 0;
const OTW_PAYLOAD_SIZE_FIELD: u64 = 1;
const OTW_PAYLOAD_COMPRESSION_TYPE_FIELD: u64 = 2;
const OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD: u64 = 3;

const COMPRESSION_ZSTD: u64 = 0;
const COMPRESSION_NONE: u64 = 255;

pub fn unwrap(body: &[u8], parser: &mut EventParser) -> Result<Vec<RawEvent>, Error> {
    let decoded = decode_payload(body)?;
    let payload = decoded.as_slice();
    let mut events = Vec::new();
    let mut offset = 0usize;
    while offset + EventHeader::HEADER_LEN <= payload.len() {
        let header = EventHeader::parse(&payload[offset..], offset as u64)?;
        let end = offset + header.event_size as usize;
        if end <= offset {
            return Err(Error::Protocol(
                "transaction payload inner event made no progress".into(),
            ));
        }
        if end > payload.len() {
            return Err(Error::UnexpectedEof);
        }
        let inner_event = &payload[offset..end];
        // MySQL omits per-event checksums inside TRANSACTION_PAYLOAD_EVENT; the
        // outer event checksum protects the compressed payload on the wire.
        let body = event_body(inner_event, false)?;
        let event = parser.parse(header, body)?;
        offset = end;
        events.push(event);
    }
    if offset != payload.len() {
        return Err(Error::Protocol(format!(
            "transaction payload has {} trailing bytes",
            payload.len() - offset
        )));
    }
    Ok(events)
}

fn decode_payload(body: &[u8]) -> Result<Vec<u8>, Error> {
    let mut payload = body;
    let mut compression_type = None;
    let mut payload_size = None;
    let mut uncompressed_size = None;

    loop {
        let field_type = read_lenenc_int(&mut payload)?;
        if field_type == OTW_PAYLOAD_HEADER_END_MARK {
            break;
        }
        let field_len = read_lenenc_int(&mut payload)? as usize;
        let value_bytes = read_bytes(&mut payload, field_len)?;
        let value = read_net_store_length_value(&value_bytes)?;
        match field_type {
            OTW_PAYLOAD_COMPRESSION_TYPE_FIELD => compression_type = Some(value),
            OTW_PAYLOAD_SIZE_FIELD => payload_size = Some(value),
            OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD => uncompressed_size = Some(value),
            other => {
                return Err(Error::UnsupportedFeature(format!(
                    "unknown TRANSACTION_PAYLOAD_EVENT header field {other}"
                )));
            }
        }
    }

    let compression_type = compression_type.ok_or_else(|| {
        Error::Protocol("TRANSACTION_PAYLOAD_EVENT missing compression type".into())
    })?;
    let payload_size = payload_size
        .ok_or_else(|| Error::Protocol("TRANSACTION_PAYLOAD_EVENT missing payload size".into()))?
        as usize;
    if payload.len() < payload_size {
        return Err(Error::UnexpectedEof);
    }
    if payload.len() > payload_size {
        return Err(Error::Protocol(format!(
            "TRANSACTION_PAYLOAD_EVENT declares {payload_size} payload bytes but has {}",
            payload.len()
        )));
    }

    let decoded = match compression_type {
        COMPRESSION_ZSTD => zstd::decode_all(payload).map_err(|e| {
            Error::Protocol(format!(
                "failed to decompress zstd transaction payload: {e}"
            ))
        })?,
        COMPRESSION_NONE => payload.to_vec(),
        other => {
            return Err(Error::UnsupportedFeature(format!(
                "unsupported TRANSACTION_PAYLOAD_EVENT compression type {other}; supported types are zstd(0) and none(255)"
            )));
        }
    };

    if let Some(expected) = uncompressed_size {
        if decoded.len() as u64 != expected {
            return Err(Error::Protocol(format!(
                "TRANSACTION_PAYLOAD_EVENT uncompressed size mismatch: expected {expected}, got {}",
                decoded.len()
            )));
        }
    }

    Ok(decoded)
}

fn read_net_store_length_value(bytes: &[u8]) -> Result<u64, Error> {
    let mut payload = bytes;
    let value = read_lenenc_int(&mut payload)?;
    if !payload.is_empty() {
        return Err(Error::Protocol(
            "TRANSACTION_PAYLOAD_EVENT TLV value has trailing bytes".into(),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lenenc(value: u64) -> Vec<u8> {
        match value {
            0..=250 => vec![value as u8],
            251..=0xffff => {
                let mut out = vec![0xfc];
                out.extend_from_slice(&(value as u16).to_le_bytes());
                out
            }
            0x1_0000..=0xff_ffff => {
                vec![
                    0xfd,
                    (value & 0xff) as u8,
                    ((value >> 8) & 0xff) as u8,
                    ((value >> 16) & 0xff) as u8,
                ]
            }
            _ => {
                let mut out = vec![0xfe];
                out.extend_from_slice(&value.to_le_bytes());
                out
            }
        }
    }

    fn tlv(field_type: u64, value: u64) -> Vec<u8> {
        let encoded = lenenc(value);
        let mut out = lenenc(field_type);
        out.extend_from_slice(&lenenc(encoded.len() as u64));
        out.extend_from_slice(&encoded);
        out
    }

    fn transaction_payload(
        compression_type: u64,
        payload: &[u8],
        uncompressed_size: u64,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&tlv(OTW_PAYLOAD_COMPRESSION_TYPE_FIELD, compression_type));
        out.extend_from_slice(&tlv(OTW_PAYLOAD_SIZE_FIELD, payload.len() as u64));
        out.extend_from_slice(&tlv(OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD, uncompressed_size));
        out.extend_from_slice(&lenenc(OTW_PAYLOAD_HEADER_END_MARK));
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn decodes_zstd_transaction_payload() {
        let inner = b"inner-binlog-events";
        let compressed = zstd::encode_all(&inner[..], 0).expect("compress");
        let body = transaction_payload(COMPRESSION_ZSTD, &compressed, inner.len() as u64);
        assert_eq!(decode_payload(&body).expect("decode zstd"), inner);
    }

    #[test]
    fn rejects_unknown_compression_type() {
        let body = transaction_payload(42, b"payload", 7);
        let err = decode_payload(&body).expect_err("unsupported compression must fail");
        assert!(
            format!("{err}").contains("unsupported TRANSACTION_PAYLOAD_EVENT compression type 42"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_size_mismatch() {
        let body = transaction_payload(COMPRESSION_NONE, b"payload", 99);
        let err = decode_payload(&body).expect_err("mismatched size must fail");
        assert!(
            format!("{err}").contains("uncompressed size mismatch"),
            "unexpected error: {err}"
        );
    }
}
