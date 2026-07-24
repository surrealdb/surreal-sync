use surreal_sync_mysql::binlog_protocol::column_types::*;
use surreal_sync_mysql::binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn varbinary() {
    assert_kind(MYSQL_TYPE_VAR_STRING, Flavor::MySql, CellValueKind::String);
    let column = col(
        MYSQL_TYPE_VAR_STRING,
        ColumnMetadata::String { max_length: 255 },
    );
    let v = decode_hex(&column, Flavor::MySql, "02 de ad");
    match v {
        CellValue::String(s) => assert_eq!(s.as_bytes(), &[0xde, 0xad]),
        other => panic!("{other:?}"),
    }
}

#[test]
fn blob_bytes() {
    assert_kind(MYSQL_TYPE_LONG_BLOB, Flavor::MySql, CellValueKind::String);
    let column = col(
        MYSQL_TYPE_LONG_BLOB,
        ColumnMetadata::Blob { length_bytes: 1 },
    );
    let v = decode_hex(&column, Flavor::MySql, "03 01 02 03");
    match v {
        CellValue::String(s) => assert_eq!(s.as_bytes(), &[1, 2, 3]),
        other => panic!("{other:?}"),
    }
}
