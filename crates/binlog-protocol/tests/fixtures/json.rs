use binlog_protocol::column_types::*;
use binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn mysql_json_binary() {
    assert_kind(MYSQL_TYPE_JSON, Flavor::MySql, CellValueKind::JsonBytes);
    let column = col(MYSQL_TYPE_JSON, ColumnMetadata::Blob { length_bytes: 1 });
    let payload = "04 01 00 00 00";
    let v = decode_hex(&column, Flavor::MySql, payload);
    match v {
        CellValue::JsonBytes(bytes) => assert_eq!(bytes, vec![1, 0, 0, 0]),
        other => panic!("{other:?}"),
    }
}

#[test]
fn mariadb_json_as_text() {
    assert_kind(MYSQL_TYPE_JSON, Flavor::MariaDb, CellValueKind::JsonText);
    let column = col(MYSQL_TYPE_JSON, ColumnMetadata::Blob { length_bytes: 1 });
    let v = decode_hex(&column, Flavor::MariaDb, "07 7b 22 61 22 3a 31 7d");
    assert_eq!(v, CellValue::JsonText(r#"{"a":1}"#.into()));
}
