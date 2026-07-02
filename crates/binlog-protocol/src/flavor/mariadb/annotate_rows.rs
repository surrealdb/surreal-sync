//! MariaDB ANNOTATE_ROWS events (type 160) carry SQL text; ignored at protocol layer v1.

pub fn is_annotate_rows_event(event_type: u8) -> bool {
    event_type == 160
}
