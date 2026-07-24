use std::collections::VecDeque;

use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::event::{EventBody, RawEvent};
use crate::binlog_protocol::flavor::Flavor;
use crate::binlog_protocol::shared::checksum::event_body;
use crate::binlog_protocol::shared::event_header::EventHeader;
use crate::binlog_protocol::shared::stream::EventStream;

/// Parses concatenated binlog event bytes (post-COM_BINLOG_DUMP packet payloads).
pub struct BinlogBytesReader {
    data: Vec<u8>,
    offset: usize,
    stream: EventStream,
    pending: VecDeque<RawEvent>,
}

impl BinlogBytesReader {
    pub fn new(data: Vec<u8>, flavor: Flavor) -> Self {
        Self {
            data,
            offset: 0,
            stream: EventStream::new(flavor, "mysql-bin.000001"),
            pending: VecDeque::new(),
        }
    }

    pub fn enable_checksum(&mut self) {
        self.stream.parser_mut().enable_checksum();
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    pub fn next_event(&mut self) -> Result<Option<RawEvent>, Error> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }

        if self.offset + EventHeader::HEADER_LEN > self.data.len() {
            return Ok(None);
        }
        let header = EventHeader::parse(&self.data[self.offset..], self.stream.file_offset())?;
        let total = header.event_size as usize;
        if self.offset + total > self.data.len() {
            return Ok(None);
        }
        let event_bytes = &self.data[self.offset..self.offset + total];
        self.offset += total;

        let body = event_body(event_bytes, self.stream.parser_mut().checksum_enabled())?;
        let header = EventHeader::parse(
            &event_bytes[..EventHeader::HEADER_LEN],
            self.stream.file_offset(),
        )?;
        let event = self.stream.parser_mut().parse(header, body)?;
        self.stream.advance(&event.header, &event.body);

        if let EventBody::TransactionPayload(inner) = event.body {
            self.pending.extend(inner);
            return Ok(self.pending.pop_front());
        }
        Ok(Some(event))
    }

    /// Drain all events, expanding transaction payloads inline.
    pub fn all_events(&mut self) -> Result<Vec<RawEvent>, Error> {
        let mut out = Vec::new();
        while let Some(event) = self.next_event()? {
            out.push(event);
        }
        Ok(out)
    }
}
