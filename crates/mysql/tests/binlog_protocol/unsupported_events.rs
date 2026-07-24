use surreal_sync_mysql::binlog_protocol::column_types::*;
use surreal_sync_mysql::binlog_protocol::{
    CellValue, ColumnDef, ColumnMetadata, Flavor, JsonDiffOperation, RowChange, RowsEvent,
};

#[test]
fn partial_update_rows_decodes_json_diff_after_image() {
    let columns = vec![
        ColumnDef::new(MYSQL_TYPE_LONG, ColumnMetadata::None),
        ColumnDef::new(MYSQL_TYPE_JSON, ColumnMetadata::Blob { length_bytes: 1 }),
    ];
    let body = hex::decode(concat!(
        "010000000000", // table_id
        "0000",         // flags
        "0200",         // var_header_len = 2 (no extra row info)
        "02",           // width = 2 columns
        "03",           // before bitmap: id + json
        "03",           // after bitmap: id + json
        "00",           // before null bitmap
        "01000000",     // before id
        "02",           // before JSON length
        "0402",         // before JSON false literal
        "01",           // shared value_options = PARTIAL_JSON_UPDATES
        "01",           // partial_bits: first JSON column is partial
        "00",           // after null bitmap
        "01000000",     // after id
        "08",           // partial JSON diff payload length
        "00",           // REPLACE
        "03",           // path length
        "242e61",       // $.a
        "02",           // data length
        "0401",         // JSON true literal
    ))
    .expect("valid hex");

    let event = RowsEvent::parse_partial_update(&body, &columns, Flavor::MySql, true, true, 10)
        .expect("parse partial update rows");
    assert_eq!(event.table_id, 1);
    let RowChange::Update { before, after } = &event.rows[0] else {
        panic!("expected update row");
    };
    assert_eq!(before[0], CellValue::Int(1));
    assert_eq!(before[1], CellValue::JsonBytes(vec![0x04, 0x02]));
    assert_eq!(after[0], CellValue::Int(1));
    let CellValue::JsonDiff(diffs) = &after[1] else {
        panic!("expected JSON diff after cell, got {:?}", after[1]);
    };
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].operation, JsonDiffOperation::Replace);
    assert_eq!(diffs[0].path, "$.a");
    assert_eq!(diffs[0].data.as_deref(), Some(&[0x04, 0x01][..]));
}
