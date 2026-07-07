use binlog_protocol::column_types::*;
use binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn varchar() {
    assert_kind(MYSQL_TYPE_VARCHAR, Flavor::MySql, CellValueKind::String);
    let column = col(
        MYSQL_TYPE_VARCHAR,
        ColumnMetadata::String { max_length: 255 },
    );
    let v = decode_hex(&column, Flavor::MySql, "03 61 62 63");
    assert_eq!(v, CellValue::String("abc".into()));
}

#[test]
fn char_string() {
    assert_kind(MYSQL_TYPE_STRING, Flavor::MySql, CellValueKind::String);
    let column = col(MYSQL_TYPE_STRING, ColumnMetadata::String { max_length: 10 });
    let v = decode_hex(&column, Flavor::MySql, "03 61 62 63");
    assert_eq!(v, CellValue::String("abc".into()));
}

#[test]
fn enum_type() {
    let column = col(MYSQL_TYPE_ENUM, ColumnMetadata::EnumSet { max_length: 0 });
    let v = decode_hex(&column, Flavor::MySql, "02 00");
    assert_eq!(v, CellValue::String("2".into()));
}

#[test]
fn text_blob() {
    assert_kind(MYSQL_TYPE_BLOB, Flavor::MySql, CellValueKind::String);
    let column = col(MYSQL_TYPE_BLOB, ColumnMetadata::Blob { length_bytes: 1 });
    let v = decode_hex(&column, Flavor::MySql, "04 68 65 6c 6c");
    assert_eq!(v, CellValue::String("hell".into()));
}
