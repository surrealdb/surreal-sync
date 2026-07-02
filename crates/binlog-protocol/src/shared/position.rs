use crate::shared::event_header::EventHeader;
use crate::types::BinlogPosition;

pub(crate) trait PositionTracker: Send {
    fn on_header(&mut self, header: &EventHeader, file: &str);
    fn position(&self) -> BinlogPosition;
    fn commit(&mut self, position: BinlogPosition);
}
