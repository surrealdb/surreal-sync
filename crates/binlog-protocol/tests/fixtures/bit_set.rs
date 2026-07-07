use binlog_protocol::column_types::*;
use binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn bit_type() {
    assert_kind(MYSQL_TYPE_BIT, Flavor::MySql, CellValueKind::Bit);
    let column = col(MYSQL_TYPE_BIT, ColumnMetadata::Bit { length_bits: 8 });
    let v = decode_hex(&column, Flavor::MySql, "aa");
    assert_eq!(v, CellValue::Bit(vec![0xaa]));
}

#[test]
fn set_type() {
    let column = col(MYSQL_TYPE_SET, ColumnMetadata::EnumSet { max_length: 0 });
    let v = decode_hex(&column, Flavor::MySql, "05 00 00 00 00 00 00 00");
    assert_eq!(v, CellValue::String("5".into()));
}
