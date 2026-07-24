use crate::binlog_protocol::event::RawEvent;
use crate::binlog_protocol::types::BinlogPosition;

pub(crate) trait PositionTracker: Send {
    fn on_event(&mut self, event: &RawEvent, file: &str);
    fn position(&self) -> BinlogPosition;
    fn commit(&mut self, position: BinlogPosition);
}
