//! Pluggable message framing for external transform I/O.

use anyhow::Result;
use bytes::{Bytes, BytesMut};

/// Supported wire framers (v1: NDJSON only).
///
/// Selected per command stage via `stdio.framer` in transforms TOML and passed
/// into child-stdio transport constructors so the config is not dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FramerKind {
    /// Newline-delimited JSON (`stdio.framer = "ndjson"`).
    #[default]
    Ndjson,
}

impl FramerKind {
    /// Resolve to the concrete [`Framer`] implementation for this kind.
    pub fn into_framer(self) -> NdjsonFramer {
        match self {
            FramerKind::Ndjson => NdjsonFramer,
        }
    }
}

/// Frames discrete payloads on a byte stream.
///
/// Implementations write into a reused output buffer and parse the next
/// complete message from a [`BytesMut`] read buffer without allocating a
/// per-line `String` (payload is returned as [`Bytes`]).
pub trait Framer: Send + Sync {
    /// Append one framed message for `payload` into `out` (reused across calls).
    fn write_message(&self, payload: &[u8], out: &mut Vec<u8>);

    /// Parse the next complete message from `buf`.
    ///
    /// Returns `Ok(None)` if the buffer does not yet contain a full message
    /// (incomplete). On success, advances `buf` past the framed message and
    /// returns the payload bytes (without framing).
    fn next_message(&self, buf: &mut BytesMut) -> Result<Option<Bytes>>;
}

/// Newline-delimited framing: each message is `payload` + `\n`.
///
/// Payload is expected to be compact JSON when used with the external
/// NDJSON wire protocol; this type does not parse JSON itself.
#[derive(Debug, Default, Clone, Copy)]
pub struct NdjsonFramer;

impl Framer for NdjsonFramer {
    fn write_message(&self, payload: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(payload);
        out.push(b'\n');
    }

    fn next_message(&self, buf: &mut BytesMut) -> Result<Option<Bytes>> {
        let Some(pos) = buf.iter().position(|&b| b == b'\n') else {
            return Ok(None);
        };
        if pos > 0 && buf[pos - 1] == b'\r' {
            // Tolerate CRLF: payload is buf[0..pos-1], consume through `\n`.
            let mut line = buf.split_to(pos + 1);
            let payload = line.split_to(pos - 1).freeze();
            Ok(Some(payload))
        } else {
            let mut line = buf.split_to(pos + 1);
            let payload = line.split_to(pos).freeze();
            Ok(Some(payload))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_roundtrip_single_and_multi() {
        let framer = NdjsonFramer;
        let mut out = Vec::new();
        framer.write_message(br#"{"a":1}"#, &mut out);
        framer.write_message(br#"{"b":2}"#, &mut out);

        let mut buf = BytesMut::from(out.as_slice());
        let m1 = framer.next_message(&mut buf).unwrap().expect("msg1");
        let m2 = framer.next_message(&mut buf).unwrap().expect("msg2");
        assert_eq!(&m1[..], br#"{"a":1}"#);
        assert_eq!(&m2[..], br#"{"b":2}"#);
        assert!(framer.next_message(&mut buf).unwrap().is_none());
        assert!(buf.is_empty());
    }

    #[test]
    fn ndjson_incomplete_buffer() {
        let framer = NdjsonFramer;
        let mut buf = BytesMut::from(&b"{\"partial\":tru"[..]);
        assert!(framer.next_message(&mut buf).unwrap().is_none());
        assert_eq!(&buf[..], b"{\"partial\":tru");

        buf.extend_from_slice(b"e}\n");
        let m = framer.next_message(&mut buf).unwrap().expect("complete");
        assert_eq!(&m[..], br#"{"partial":true}"#);
        assert!(buf.is_empty());
    }

    #[test]
    fn ndjson_empty_payload_line() {
        let framer = NdjsonFramer;
        let mut buf = BytesMut::from(&b"\n"[..]);
        let m = framer.next_message(&mut buf).unwrap().expect("empty line");
        assert!(m.is_empty());
    }

    #[test]
    fn ndjson_crlf() {
        let framer = NdjsonFramer;
        let mut buf = BytesMut::from(&b"hello\r\n"[..]);
        let m = framer.next_message(&mut buf).unwrap().expect("crlf");
        assert_eq!(&m[..], b"hello");
    }
}
