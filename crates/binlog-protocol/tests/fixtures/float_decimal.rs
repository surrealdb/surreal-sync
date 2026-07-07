use binlog_protocol::column_types::*;
use binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn float_type() {
    assert_kind(MYSQL_TYPE_FLOAT, Flavor::MySql, CellValueKind::Float);
    let v = decode_hex(
        &col(
            MYSQL_TYPE_FLOAT,
            ColumnMetadata::FloatPackLength { bytes: 4 },
        ),
        Flavor::MySql,
        "00 00 80 3f",
    );
    assert_eq!(v, CellValue::Float(1.0));
}

#[test]
fn double_type() {
    assert_kind(MYSQL_TYPE_DOUBLE, Flavor::MySql, CellValueKind::Double);
    let v = decode_hex(
        &col(
            MYSQL_TYPE_DOUBLE,
            ColumnMetadata::FloatPackLength { bytes: 8 },
        ),
        Flavor::MySql,
        "00 00 00 00 00 00 f0 3f",
    );
    assert_eq!(v, CellValue::Double(1.0));
}

#[test]
fn decimal_type() {
    assert_kind(MYSQL_TYPE_NEWDECIMAL, Flavor::MySql, CellValueKind::Decimal);
    let column = col(
        MYSQL_TYPE_NEWDECIMAL,
        ColumnMetadata::Numeric {
            precision: 10,
            scale: 2,
        },
    );
    let v = decode_hex(
        &column,
        Flavor::MySql,
        "00 00 00 00 00 00 00 00 00 00 00 00 00 01 23 45",
    );
    assert!(matches!(v, CellValue::Decimal(_)));
}
