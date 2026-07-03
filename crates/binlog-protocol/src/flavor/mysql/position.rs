use crate::event::{EventBody, RawEvent};
use crate::flavor::mysql::gtid_set::{Gtid, MySqlGtidSet};
use crate::shared::events::sanitize_binlog_file;
use crate::shared::position::PositionTracker;
use crate::types::BinlogPosition;

pub struct MySqlPositionTracker {
    file: String,
    position: u64,
    gtid_set: Option<MySqlGtidSet>,
    pending_gtid: Option<Gtid>,
}

impl MySqlPositionTracker {
    pub fn new(file: impl Into<String>, position: u64) -> Self {
        Self {
            file: file.into(),
            position,
            gtid_set: None,
            pending_gtid: None,
        }
    }

    pub fn with_gtid(gtid_set: MySqlGtidSet) -> Self {
        Self {
            file: String::new(),
            position: 0,
            gtid_set: Some(gtid_set),
            pending_gtid: None,
        }
    }

    /// Record the GTID announced by a `GTID_LOG_EVENT`. It is accumulated into the
    /// executed set at the next commit boundary (analogous to the MariaDB
    /// tracker's `set_pending_gtid`), so runtime positions advance transactionally.
    pub fn set_pending_gtid(&mut self, gtid: Gtid) {
        self.pending_gtid = Some(gtid);
    }

    /// Enter GTID tracking mode seeded with `set`. Subsequent GTID events are
    /// accumulated into the set and `position()` reports `MySqlGtid`.
    pub fn seed_gtid_set(&mut self, set: MySqlGtidSet) {
        self.gtid_set = Some(set);
    }

    pub fn on_event(&mut self, event: &RawEvent, file: &str) {
        let header = &event.header;
        self.file = file.to_string();
        if header.log_pos > 0 {
            self.position = u64::from(header.log_pos);
        } else {
            self.position = header.file_offset + u64::from(header.event_size);
        }

        match &event.body {
            // The GTID marker itself only declares the pending GTID; it is folded
            // into the executed set at the following commit boundary.
            EventBody::Gtid(_) => {}
            EventBody::Xid(_) => self.fold_pending_gtid(),
            EventBody::Query(query) if query.is_gtid_commit_boundary() => self.fold_pending_gtid(),
            _ => {}
        }
    }

    fn fold_pending_gtid(&mut self) {
        if let Some(gtid) = self.pending_gtid.take() {
            // Seeing a GTID event means the server runs in GTID mode; lazily
            // enter GTID tracking so `position()` reports `MySqlGtid`.
            let set = self.gtid_set.get_or_insert_with(MySqlGtidSet::default);
            let _ = set.add_gtid(gtid);
        }
    }
}

impl PositionTracker for MySqlPositionTracker {
    fn on_event(&mut self, event: &RawEvent, file: &str) {
        MySqlPositionTracker::on_event(self, event, file);
    }

    fn position(&self) -> BinlogPosition {
        match self.gtid_set {
            Some(ref set) if !set.is_empty() => BinlogPosition::MySqlGtid {
                executed: set.clone(),
            },
            _ => BinlogPosition::file_pos(sanitize_binlog_file(&self.file), self.position),
        }
    }

    fn commit(&mut self, position: BinlogPosition) {
        match position {
            BinlogPosition::FilePos { file, pos } => {
                self.file = file;
                self.position = pos;
                self.gtid_set = None;
            }
            BinlogPosition::MySqlGtid { executed } => {
                self.gtid_set = Some(executed);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::event_header::EventHeader;
    use crate::shared::event_type::EventType;
    use crate::types::QueryEvent;

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

    #[test]
    fn defaults_to_file_pos_without_gtid() {
        let tracker = MySqlPositionTracker::new("mysql-bin.000001", 4);
        assert!(matches!(tracker.position(), BinlogPosition::FilePos { .. }));
    }

    #[test]
    fn gtid_event_then_commit_reports_mysql_gtid() {
        let mut tracker = MySqlPositionTracker::new("mysql-bin.000001", 4);
        let source_id = [7u8; 16];

        // A GTID_LOG_EVENT declares the transaction's GTID...
        tracker.on_event(
            &event(
                EventType::MySqlGtid,
                100,
                EventBody::Gtid(crate::types::GtidMarker::MySql { source_id, gno: 5 }),
            ),
            "mysql-bin.000001",
        );
        tracker.set_pending_gtid(Gtid { source_id, gno: 5 });
        // ...still file+pos until a commit boundary folds it in.
        assert!(matches!(tracker.position(), BinlogPosition::FilePos { .. }));

        // A row/XID event at the transaction boundary accumulates the GTID.
        tracker.on_event(
            &event(
                EventType::Xid,
                200,
                EventBody::Xid(crate::types::XidEvent { xid: 42 }),
            ),
            "mysql-bin.000001",
        );

        match tracker.position() {
            BinlogPosition::MySqlGtid { executed } => {
                assert_eq!(
                    executed.to_string(),
                    "07070707-0707-0707-0707-070707070707:5"
                );
            }
            other => panic!("expected MySqlGtid, got {other:?}"),
        }
    }

    #[test]
    fn begin_query_does_not_advance_pending_gtid() {
        let mut tracker = MySqlPositionTracker::new("mysql-bin.000001", 4);
        let source_id = [8u8; 16];
        tracker.set_pending_gtid(Gtid { source_id, gno: 6 });

        tracker.on_event(
            &event(EventType::Query, 150, query("BEGIN")),
            "mysql-bin.000001",
        );

        assert!(matches!(tracker.position(), BinlogPosition::FilePos { .. }));
    }

    #[test]
    fn ddl_query_advances_pending_gtid() {
        let mut tracker = MySqlPositionTracker::new("mysql-bin.000001", 4);
        let source_id = [9u8; 16];
        tracker.set_pending_gtid(Gtid { source_id, gno: 7 });

        tracker.on_event(
            &event(
                EventType::Query,
                150,
                query("ALTER TABLE widgets ADD COLUMN name TEXT"),
            ),
            "mysql-bin.000001",
        );

        assert!(matches!(
            tracker.position(),
            BinlogPosition::MySqlGtid { .. }
        ));
    }
}
