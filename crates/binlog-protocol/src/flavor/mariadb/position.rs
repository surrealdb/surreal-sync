use crate::flavor::mariadb::gtid_list::{MariaDbGtid, MariaDbGtidList};
use crate::shared::event_header::EventHeader;
use crate::shared::event_type::EventType;
use crate::shared::events::sanitize_binlog_file;
use crate::shared::position::PositionTracker;
use crate::types::BinlogPosition;

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

    pub fn on_event(&mut self, header: &EventHeader, file: &str, event_type: EventType) {
        self.file = file.to_string();
        // MariaDB 11.4+ may emit log_pos=0; track via file_offset + event_size.
        if header.log_pos > 0 {
            self.file_offset = u64::from(header.log_pos);
        } else {
            self.file_offset = header.file_offset + u64::from(header.event_size);
        }

        match event_type {
            EventType::MariaDbGtid => {}
            EventType::Xid | EventType::Rotate | EventType::Query => {
                if let (Some(ref mut list), Some(gtid)) =
                    (&mut self.gtid_list, self.pending_gtid.take())
                {
                    let _ = list.add(gtid);
                }
            }
            _ => {}
        }
    }

    pub fn set_pending_gtid(&mut self, gtid: MariaDbGtid) {
        self.pending_gtid = Some(gtid);
    }
}

impl PositionTracker for MariaDbPositionTracker {
    fn on_header(&mut self, header: &EventHeader, file: &str) {
        self.on_event(header, file, header.event_type);
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
