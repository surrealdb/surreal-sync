//! Synthetic WRITE_ROWS v2 fixture for multi-column decode.

use surreal_sync_mysql::binlog_protocol::column_types::*;
use surreal_sync_mysql::binlog_protocol::{
    CellValue, ColumnDef, ColumnMetadata, Flavor, RowChange, RowsEvent, TableMapEvent,
};

fn multi_column_table() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new(MYSQL_TYPE_LONG, ColumnMetadata::None),
        ColumnDef::new(
            MYSQL_TYPE_VARCHAR,
            ColumnMetadata::String { max_length: 255 },
        ),
        ColumnDef::new(MYSQL_TYPE_DATETIME2, ColumnMetadata::Temporal { fsp: 0 }),
        ColumnDef::new(
            MYSQL_TYPE_NEWDECIMAL,
            ColumnMetadata::Numeric {
                precision: 19,
                scale: 5,
            },
        ),
        ColumnDef::new(MYSQL_TYPE_JSON, ColumnMetadata::Blob { length_bytes: 2 }),
    ]
}

#[test]
fn write_rows_v2_multi_column_roundtrip() {
    let columns = multi_column_table();
    let body = hex::decode(concat!(
        "010000000000",         // table_id
        "0000",                 // flags
        "0200",                 // var_header_len = 2 (no extra row info)
        "05",                   // width = 5 columns
        "1f",                   // columns present (all 5)
        "00",                   // null bitmap (no nulls)
        "2a000000",             // INT 42
        "026869",               // VARCHAR "hi"
        "19b39ea7ad",           // DATETIME2 2024-06-15 ~10:30:00
        "80000000003039010932", // DECIMAL 12345.67890 (10 bytes)
        "0700",                 // JSON length (LE u16)
        "7b2261223a317d",       // {"a":1}
    ))
    .expect("valid hex");

    let event = RowsEvent::parse_write(&body, &columns, Flavor::MySql, true, true, 10)
        .expect("parse write rows v2");
    assert_eq!(event.table_id, 1);
    assert_eq!(event.rows.len(), 1);

    let RowChange::Insert(cells) = &event.rows[0] else {
        panic!("expected insert row");
    };
    assert_eq!(cells.len(), 5);
    assert_eq!(cells[0], CellValue::Int(42));
    assert_eq!(cells[1], CellValue::String("hi".into()));
    assert!(matches!(cells[2], CellValue::DateTime { year: 2024, .. }));
    assert!(matches!(cells[3], CellValue::Decimal(_)));
    assert!(matches!(cells[4], CellValue::JsonBytes(_)));
}

#[test]
fn table_map_parses_signedness_optional_metadata() {
    // 2 columns: signed TINYINT + unsigned TINYINT; signedness bitmap 0b01000000 (col1 unsigned).
    let body = hex::decode(concat!(
        "010000000000", // table_id (6 bytes)
        "0000",         // flags
        "04",           // db name len
        "74657374",     // "test"
        "00",           // db name terminator
        "03",           // table name len
        "747074",       // "tpt"
        "00",           // table name terminator
        "02",           // 2 columns
        "0103",         // TINY, LONG
        "00",           // metadata len (none)
        "00",           // null bitmap
        "01",           // SIGNEDNESS type
        "01",           // length 1 byte
        "40",           // MSB-first: col0 signed, col1 unsigned
    ))
    .expect("valid hex");

    let tm = TableMapEvent::parse(&body, 8, Flavor::MySql).expect("parse table map");
    assert_eq!(tm.columns.len(), 2);
    assert!(!tm.columns[0].unsigned);
    assert!(tm.columns[1].unsigned);
}
