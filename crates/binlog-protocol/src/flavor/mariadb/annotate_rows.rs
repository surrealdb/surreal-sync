//! Not decoded for sync: TABLE_MAP already supplies row column layout and QUERY covers DDL;
//! annotate SQL would be redundant except for optional debugging context.

pub fn is_annotate_rows_event(event_type: u8) -> bool {
    event_type == 160
}
