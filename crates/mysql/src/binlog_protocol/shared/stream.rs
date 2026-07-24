use crate::binlog_protocol::error::Error;
use crate::binlog_protocol::event::{EventBody, EventParser, RawEvent};
use crate::binlog_protocol::flavor::Flavor;
use crate::binlog_protocol::shared::event_header::EventHeader;

pub struct EventStream {
    flavor: Flavor,
    current_file: String,
    file_offset: u64,
    parser: EventParser,
    fake_rotate: bool,
}

impl EventStream {
    pub fn new(flavor: Flavor, initial_file: impl Into<String>) -> Self {
        Self {
            flavor,
            current_file: initial_file.into(),
            file_offset: 4,
            parser: EventParser::new(flavor),
            fake_rotate: false,
        }
    }

    pub fn parser_mut(&mut self) -> &mut EventParser {
        &mut self.parser
    }

    pub fn current_file(&self) -> &str {
        &self.current_file
    }

    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    pub fn set_file_and_offset(&mut self, file: impl Into<String>, offset: u64) {
        self.current_file = file.into();
        self.file_offset = offset;
        self.fake_rotate = false;
    }

    /// Advance stream offsets after an event has been parsed (mirrors `read_event` bookkeeping).
    pub fn advance(&mut self, header: &EventHeader, body: &EventBody) {
        if let EventBody::Rotate(rotate) = body {
            if rotate.position == 0 {
                self.fake_rotate = true;
            } else {
                self.fake_rotate = false;
                self.current_file = rotate.next_file.clone();
                self.file_offset = rotate.position;
            }
        } else if !self.fake_rotate {
            if header.log_pos > 0 {
                self.file_offset = u64::from(header.log_pos);
            } else {
                self.file_offset = header.file_offset + u64::from(header.event_size);
            }
        } else {
            self.file_offset += u64::from(header.event_size);
        }
    }

    pub fn read_event(&mut self, data: &[u8]) -> Result<RawEvent, Error> {
        if data.len() < EventHeader::HEADER_LEN {
            return Err(Error::UnexpectedEof);
        }
        let header = EventHeader::parse(&data[..EventHeader::HEADER_LEN], self.file_offset)?;
        let body_len = header.event_size as usize;
        if data.len() < body_len {
            return Err(Error::UnexpectedEof);
        }
        let body = &data[EventHeader::HEADER_LEN..body_len];
        let event = self.parser.parse(header.clone(), body)?;

        self.advance(&header, &event.body);

        Ok(event)
    }
}
