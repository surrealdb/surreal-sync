use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::Error;
use crate::event::RawEvent;
use crate::flavor::Flavor;
use crate::shared::event_header::EventHeader;
use crate::shared::stream::EventStream;

pub struct BinlogFileReader {
    file: File,
    buffer: Vec<u8>,
    stream: EventStream,
    parser: crate::event::EventParser,
}

impl BinlogFileReader {
    pub fn open(path: impl AsRef<Path>, flavor: Flavor) -> Result<Self, Error> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("mysql-bin.000001")
            .to_string();
        Ok(Self {
            file,
            buffer: Vec::new(),
            stream: EventStream::new(flavor, filename),
            parser: crate::event::EventParser::new(flavor),
        })
    }

    pub fn next_event(&mut self) -> Result<Option<RawEvent>, Error> {
        loop {
            if self.buffer.len() >= EventHeader::HEADER_LEN {
                let header = EventHeader::parse(&self.buffer, self.stream.file_offset())?;
                let total = header.event_size as usize;
                if self.buffer.len() >= total {
                    let event_bytes = self.buffer.drain(..total).collect::<Vec<_>>();
                    let body = &event_bytes[EventHeader::HEADER_LEN..];
                    return Ok(Some(self.parser.parse(header, body)?));
                }
            }
            let mut chunk = [0u8; 8192];
            let n = self.file.read(&mut chunk)?;
            if n == 0 {
                return Ok(None);
            }
            self.buffer.extend_from_slice(&chunk[..n]);
        }
    }
}
