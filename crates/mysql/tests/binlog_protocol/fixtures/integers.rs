use surreal_sync_mysql::binlog_protocol::column_types::*;
use surreal_sync_mysql::binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, col_unsigned, decode_hex};

#[test]
fn tinyint_signed() {
    assert_kind(MYSQL_TYPE_TINY, Flavor::MySql, CellValueKind::Int);
    let v = decode_hex(
        &col(MYSQL_TYPE_TINY, ColumnMetadata::None),
        Flavor::MySql,
        "fb",
    );
    assert_eq!(v, CellValue::Int(-5));
}

#[test]
fn tinyint_unsigned() {
    let v = decode_hex(
        &col_unsigned(MYSQL_TYPE_TINY, ColumnMetadata::None),
        Flavor::MySql,
        "ff",
    );
    assert_eq!(v, CellValue::UInt(255));
}

#[test]
fn smallint() {
    assert_kind(MYSQL_TYPE_SHORT, Flavor::MySql, CellValueKind::Int);
    let v = decode_hex(
        &col(MYSQL_TYPE_SHORT, ColumnMetadata::None),
        Flavor::MySql,
        "3412",
    );
    assert_eq!(v, CellValue::Int(0x1234));
}

#[test]
fn mediumint() {
    assert_kind(MYSQL_TYPE_INT24, Flavor::MySql, CellValueKind::Int);
    let v = decode_hex(
        &col(MYSQL_TYPE_INT24, ColumnMetadata::None),
        Flavor::MySql,
        "563412",
    );
    assert_eq!(v, CellValue::Int(0x123456));
}

#[test]
fn int_column() {
    assert_kind(MYSQL_TYPE_LONG, Flavor::MySql, CellValueKind::Int);
    let v = decode_hex(
        &col(MYSQL_TYPE_LONG, ColumnMetadata::None),
        Flavor::MySql,
        "78563412",
    );
    assert_eq!(v, CellValue::Int(0x12345678));
}

#[test]
fn bigint() {
    assert_kind(MYSQL_TYPE_LONGLONG, Flavor::MySql, CellValueKind::Int);
    let v = decode_hex(
        &col(MYSQL_TYPE_LONGLONG, ColumnMetadata::None),
        Flavor::MySql,
        "ef cd ab 90 78 56 34 12",
    );
    assert_eq!(v, CellValue::Int(0x1234567890abcdef_u64 as i64));
}

#[test]
fn bigint_unsigned() {
    let v = decode_hex(
        &col_unsigned(MYSQL_TYPE_LONGLONG, ColumnMetadata::None),
        Flavor::MySql,
        "ff ff ff ff ff ff ff 7f",
    );
    assert_eq!(v, CellValue::UInt(i64::MAX as u64));
}
