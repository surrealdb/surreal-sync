use crate::error::Error;
use crate::flavor::Flavor;
use crate::shared::event_header::EventHeader;
use crate::shared::event_type::EventType;
use crate::shared::events::rows::RowsEvent;
use crate::shared::events::{
    rows_event_six_byte_table_id, table_id_from_post_header, FormatDescriptionEvent,
    HeartbeatEvent, QueryEvent, RotateEvent, TableMapEvent, XidEvent, ROWS_HEADER_LEN_V2,
};
use crate::types::GtidMarker;

fn rows_event_v2_var_header(raw_type_code: u8) -> bool {
    matches!(raw_type_code, 30..=32)
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawEvent {
    pub header: EventHeader,
    pub body: EventBody,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EventBody {
    FormatDescription(FormatDescriptionEvent),
    Rotate(RotateEvent),
    TableMap(TableMapEvent),
    Rows(RowsEvent),
    Xid(XidEvent),
    Query(QueryEvent),
    Heartbeat(HeartbeatEvent),
    Gtid(GtidMarker),
    TransactionPayload(Vec<RawEvent>),
    Ignored,
}

pub struct EventParser {
    flavor: Flavor,
    checksum: bool,
    checksum_verified: bool,
    post_header_lengths: Vec<u8>,
    table_maps: std::collections::HashMap<u64, TableMapEvent>,
}

impl EventParser {
    pub fn new(flavor: Flavor) -> Self {
        Self {
            flavor,
            checksum: false,
            checksum_verified: false,
            post_header_lengths: Vec::new(),
            table_maps: std::collections::HashMap::new(),
        }
    }

    pub fn enable_checksum(&mut self) {
        self.checksum = true;
        self.checksum_verified = true;
    }

    pub fn checksum_enabled(&self) -> bool {
        self.checksum
    }

    pub fn parse(&mut self, header: EventHeader, body: &[u8]) -> Result<RawEvent, Error> {
        let event_type = header.event_type.for_flavor(self.flavor);
        let parsed_body = match event_type {
            EventType::FormatDescription => {
                let fde = FormatDescriptionEvent::parse(body)?;
                self.checksum = fde.checksum_alg != 0;
                self.checksum_verified = self.checksum;
                self.post_header_lengths = fde.post_header_lengths.clone();
                EventBody::FormatDescription(fde)
            }
            EventType::Rotate => EventBody::Rotate(RotateEvent::parse(body)?),
            EventType::TableMap => {
                let post_header_len = self.post_header_len(header.raw_type_code);
                let tm = TableMapEvent::parse(body, post_header_len, self.flavor)?;
                self.table_maps.insert(tm.table_id, tm.clone());
                EventBody::TableMap(tm)
            }
            EventType::WriteRows => {
                let post_header_len = self.post_header_len(header.raw_type_code);
                let tm = self.table_map_for_body(body, header.raw_type_code)?;
                EventBody::Rows(RowsEvent::parse_write(
                    body,
                    &tm.columns,
                    self.flavor,
                    rows_event_six_byte_table_id(header.raw_type_code, self.flavor),
                    rows_event_v2_var_header(header.raw_type_code),
                    post_header_len,
                )?)
            }
            EventType::UpdateRows => {
                let post_header_len = self.post_header_len(header.raw_type_code);
                let tm = self.table_map_for_body(body, header.raw_type_code)?;
                EventBody::Rows(RowsEvent::parse_update(
                    body,
                    &tm.columns,
                    self.flavor,
                    rows_event_six_byte_table_id(header.raw_type_code, self.flavor),
                    rows_event_v2_var_header(header.raw_type_code),
                    post_header_len,
                )?)
            }
            EventType::DeleteRows => {
                let post_header_len = self.post_header_len(header.raw_type_code);
                let tm = self.table_map_for_body(body, header.raw_type_code)?;
                EventBody::Rows(RowsEvent::parse_delete(
                    body,
                    &tm.columns,
                    self.flavor,
                    rows_event_six_byte_table_id(header.raw_type_code, self.flavor),
                    rows_event_v2_var_header(header.raw_type_code),
                    post_header_len,
                )?)
            }
            EventType::Xid => EventBody::Xid(XidEvent::parse(body)?),
            EventType::Query => EventBody::Query(QueryEvent::parse(body)?),
            EventType::Heartbeat => EventBody::Heartbeat(HeartbeatEvent::parse(body)?),
            EventType::MySqlGtid => EventBody::Gtid(crate::flavor::mysql::gtid_event::parse(body)?),
            EventType::MariaDbGtid => {
                EventBody::Gtid(crate::flavor::mariadb::gtid_event::parse(body)?)
            }
            EventType::TransactionPayload => {
                let inner = crate::flavor::mysql::txn_payload::unwrap(body, self)?;
                EventBody::TransactionPayload(inner)
            }
            EventType::StartEncryption => return Err(Error::EncryptedBinlog),
            EventType::GtidList
            | EventType::AnnotateRows
            | EventType::BinlogCheckpoint
            | EventType::PartialUpdateRows
            | EventType::Unknown(_) => EventBody::Ignored,
        };

        Ok(RawEvent {
            header: EventHeader {
                event_type,
                ..header
            },
            body: parsed_body,
        })
    }

    fn post_header_len(&self, raw_type_code: u8) -> usize {
        let idx = raw_type_code as usize;
        if idx != 0 && self.post_header_lengths.len() >= idx {
            return self.post_header_lengths[idx - 1] as usize;
        }
        match raw_type_code {
            30..=32 => ROWS_HEADER_LEN_V2,
            23..=25 => 6,
            19 => 8,
            _ => 8,
        }
    }

    fn table_map_for_body(&self, body: &[u8], raw_type_code: u8) -> Result<TableMapEvent, Error> {
        let table_id = table_id_from_post_header(
            body,
            self.post_header_len(raw_type_code),
            self.flavor,
            raw_type_code,
        )?;
        self.table_maps
            .get(&table_id)
            .cloned()
            .ok_or_else(|| Error::Protocol(format!("missing table map for table_id {table_id}")))
    }
}
