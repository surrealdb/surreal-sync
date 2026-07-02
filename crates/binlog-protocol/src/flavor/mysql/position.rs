use crate::flavor::mysql::gtid_set::MySqlGtidSet;
use crate::shared::event_header::EventHeader;
use crate::shared::event_type::EventType;
use crate::shared::events::sanitize_binlog_file;
use crate::shared::position::PositionTracker;
use crate::types::BinlogPosition;

pub struct MySqlPositionTracker {
    file: String,
    position: u64,
    gtid_set: Option<MySqlGtidSet>,
}

impl MySqlPositionTracker {
    pub fn new(file: impl Into<String>, position: u64) -> Self {
        Self {
            file: file.into(),
            position,
            gtid_set: None,
        }
    }

    pub fn with_gtid(gtid_set: MySqlGtidSet) -> Self {
        Self {
            file: String::new(),
            position: 0,
            gtid_set: Some(gtid_set),
        }
    }
}

impl PositionTracker for MySqlPositionTracker {
    fn on_header(&mut self, header: &EventHeader, file: &str) {
        self.file = file.to_string();
        if header.log_pos > 0 {
            self.position = u64::from(header.log_pos);
        } else {
            self.position = header.file_offset + u64::from(header.event_size);
        }
    }

    fn position(&self) -> BinlogPosition {
        if let Some(ref set) = self.gtid_set {
            BinlogPosition::MySqlGtid {
                executed: set.clone(),
            }
        } else {
            BinlogPosition::file_pos(sanitize_binlog_file(&self.file), self.position)
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

impl MySqlPositionTracker {
    pub fn on_event(&mut self, header: &EventHeader, file: &str, event_type: EventType) {
        self.on_header(header, file);
        let _ = event_type;
    }
}
