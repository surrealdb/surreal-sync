use crate::binlog_protocol::event::{EventBody, RawEvent};
use crate::binlog_protocol::flavor::mariadb::gtid_list::{MariaDbGtid, MariaDbGtidList};
use crate::binlog_protocol::shared::events::sanitize_binlog_file;
use crate::binlog_protocol::shared::position::PositionTracker;
use crate::binlog_protocol::types::BinlogPosition;

pub struct MariaDbPositionTracker {
    file: String,
    file_offset: u64,
    gtid_list: Option<MariaDbGtidList>,
    pending_gtid: Option<MariaDbGtid>,
}

impl MariaDbPositionTracker {
    pub fn new(file: impl Into<String>, position: u64) -> Self {
        Self {
            file: file.into(),
            file_offset: position,
            gtid_list: None,
            pending_gtid: None,
        }
    }

    pub fn with_gtid_list(list: MariaDbGtidList) -> Self {
        Self {
            file: String::new(),
            file_offset: 0,
            gtid_list: Some(list),
            pending_gtid: None,
        }
    }

    pub fn on_event(&mut self, event: &RawEvent, file: &str) {
        let header = &event.header;
        self.file = file.to_string();
        // MariaDB 11.4+ may emit log_pos=0; track via file_offset + event_size.
        if header.log_pos > 0 {
            self.file_offset = u64::from(header.log_pos);
        } else {
            self.file_offset = header.file_offset + u64::from(header.event_size);
        }

        match &event.body {
            EventBody::Gtid(_) => {}
            EventBody::Xid(_) => self.fold_pending_gtid(),
            EventBody::Query(query) if query.is_gtid_commit_boundary() => self.fold_pending_gtid(),
            _ => {}
        }
    }

    fn fold_pending_gtid(&mut self) {
        if let (Some(ref mut list), Some(gtid)) = (&mut self.gtid_list, self.pending_gtid.take()) {
            let _ = list.add(gtid);
        }
    }

    pub fn set_pending_gtid(&mut self, gtid: MariaDbGtid) {
        self.pending_gtid = Some(gtid);
    }

    /// Enter GTID tracking mode seeded with `list`. Subsequent GTID events are
    /// accumulated into the list and `position()` reports `MariaDbGtid`.
    pub fn seed_gtid_list(&mut self, list: MariaDbGtidList) {
        self.gtid_list = Some(list);
    }
}

impl PositionTracker for MariaDbPositionTracker {
    fn on_event(&mut self, event: &RawEvent, file: &str) {
        MariaDbPositionTracker::on_event(self, event, file);
    }

    fn position(&self) -> BinlogPosition {
        if let Some(ref list) = self.gtid_list {
            BinlogPosition::MariaDbGtid {
                executed: list.clone(),
            }
        } else {
            BinlogPosition::file_pos(sanitize_binlog_file(&self.file), self.file_offset)
        }
    }

    fn commit(&mut self, position: BinlogPosition) {
        match position {
            BinlogPosition::FilePos { file, pos } => {
                self.file = file;
                self.file_offset = pos;
                self.gtid_list = None;
            }
            BinlogPosition::MariaDbGtid { executed } => {
                self.gtid_list = Some(executed);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binlog_protocol::shared::event_header::EventHeader;
    use crate::binlog_protocol::shared::event_type::EventType;
    use crate::binlog_protocol::types::{QueryEvent, XidEvent};

    fn header(event_type: EventType, log_pos: u32) -> EventHeader {
        EventHeader {
            timestamp: 0,
            raw_type_code: event_type.code(),
            event_type,
            server_id: 1,
            event_size: 40,
            log_pos,
            flags: 0,
            file_offset: 0,
        }
    }

    fn event(event_type: EventType, log_pos: u32, body: EventBody) -> RawEvent {
        RawEvent {
            header: header(event_type, log_pos),
            body,
        }
    }

    fn query(sql: &str) -> EventBody {
        EventBody::Query(QueryEvent {
            thread_id: 1,
            database: "test".to_string(),
            sql: sql.to_string(),
        })
    }

    fn seeded_tracker() -> MariaDbPositionTracker {
        MariaDbPositionTracker::with_gtid_list(MariaDbGtidList::parse("0-1-10").unwrap())
    }

    #[test]
    fn begin_query_does_not_advance_pending_gtid() {
        let mut tracker = seeded_tracker();
        tracker.set_pending_gtid(MariaDbGtid {
            domain_id: 0,
            server_id: 1,
            sequence: 11,
        });

        tracker.on_event(
            &event(EventType::Query, 150, query("BEGIN")),
            "mysql-bin.000001",
        );

        match tracker.position() {
            BinlogPosition::MariaDbGtid { executed } => {
                assert_eq!(executed.to_connect_state(), "0-1-10");
            }
            other => panic!("expected MariaDbGtid, got {other:?}"),
        }
    }

    #[test]
    fn xid_advances_pending_gtid() {
        let mut tracker = seeded_tracker();
        tracker.set_pending_gtid(MariaDbGtid {
            domain_id: 0,
            server_id: 1,
            sequence: 11,
        });

        tracker.on_event(
            &event(EventType::Xid, 200, EventBody::Xid(XidEvent { xid: 42 })),
            "mysql-bin.000001",
        );

        match tracker.position() {
            BinlogPosition::MariaDbGtid { executed } => {
                assert_eq!(executed.to_connect_state(), "0-1-11");
            }
            other => panic!("expected MariaDbGtid, got {other:?}"),
        }
    }

    #[test]
    fn ddl_query_advances_pending_gtid() {
        let mut tracker = seeded_tracker();
        tracker.set_pending_gtid(MariaDbGtid {
            domain_id: 0,
            server_id: 1,
            sequence: 11,
        });

        tracker.on_event(
            &event(
                EventType::Query,
                200,
                query("CREATE TABLE widgets (id INT)"),
            ),
            "mysql-bin.000001",
        );

        match tracker.position() {
            BinlogPosition::MariaDbGtid { executed } => {
                assert_eq!(executed.to_connect_state(), "0-1-11");
            }
            other => panic!("expected MariaDbGtid, got {other:?}"),
        }
    }
}
