use binlog_protocol::column_types::*;
use binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn date() {
    assert_kind(MYSQL_TYPE_DATE, Flavor::MySql, CellValueKind::Date);
    // 2024-06-15: (2024<<9)|(6<<5)|15 = 1036495 = 0x0FC0F
    let v = decode_hex(
        &col(MYSQL_TYPE_DATE, ColumnMetadata::None),
        Flavor::MySql,
        "cf d0 0f",
    );
    match v {
        CellValue::Date { year, month, day } => {
            assert_eq!((year, month, day), (2024, 6, 15));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn datetime_v1() {
    assert_kind(MYSQL_TYPE_DATETIME, Flavor::MySql, CellValueKind::DateTime);
    // 20240615103045 packed little-endian u64
    let v = decode_hex(
        &col(MYSQL_TYPE_DATETIME, ColumnMetadata::None),
        Flavor::MySql,
        "45 5a ac a2 68 12 00 00",
    );
    match v {
        CellValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            ..
        } => {
            assert_eq!(
                (year, month, day, hour, minute, second),
                (2024, 6, 15, 10, 30, 45)
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn datetime2() {
    assert_kind(MYSQL_TYPE_DATETIME2, Flavor::MySql, CellValueKind::DateTime);
    let column = col(MYSQL_TYPE_DATETIME2, ColumnMetadata::Temporal { fsp: 0 });
    let v = decode_hex(&column, Flavor::MySql, "19 b3 9e a7 ad");
    assert!(matches!(v, CellValue::DateTime { year: 2024, .. }));
}

#[test]
fn time2() {
    assert_kind(MYSQL_TYPE_TIME2, Flavor::MySql, CellValueKind::Time);
    let column = col(MYSQL_TYPE_TIME2, ColumnMetadata::Temporal { fsp: 0 });
    // 10:30:00 with sign bit set (positive)
    let v = decode_hex(&column, Flavor::MySql, "80 a7 80");
    match v {
        CellValue::Time {
            hour,
            minute,
            second,
            ..
        } => {
            assert_eq!((hour, minute, second), (10, 30, 0));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn timestamp2() {
    assert_kind(
        MYSQL_TYPE_TIMESTAMP2,
        Flavor::MySql,
        CellValueKind::TimestampMillis,
    );
    let column = col(MYSQL_TYPE_TIMESTAMP2, ColumnMetadata::Temporal { fsp: 0 });
    let v = decode_hex(&column, Flavor::MySql, "00 ca 9a 3b");
    match v {
        CellValue::TimestampMillis(ms) => assert_eq!(ms, 1_000_000_000_000),
        other => panic!("{other:?}"),
    }
}

#[test]
fn year_type() {
    assert_kind(MYSQL_TYPE_YEAR, Flavor::MySql, CellValueKind::Int);
    let v = decode_hex(
        &col(MYSQL_TYPE_YEAR, ColumnMetadata::None),
        Flavor::MySql,
        "7c",
    );
    assert_eq!(v, CellValue::Year(2024));
}
