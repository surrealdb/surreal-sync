use surreal_sync_mysql::binlog_protocol::column_types::*;
use surreal_sync_mysql::binlog_protocol::{CellValue, CellValueKind, ColumnMetadata, Flavor};

use super::{assert_kind, col, decode_hex};

#[test]
fn geometry_bytes() {
    assert_kind(MYSQL_TYPE_GEOMETRY, Flavor::MySql, CellValueKind::Bytes);
    let column = col(
        MYSQL_TYPE_GEOMETRY,
        ColumnMetadata::Blob { length_bytes: 1 },
    );
    let v = decode_hex(&column, Flavor::MySql, "04 01 01 00 00");
    assert_eq!(v, CellValue::Bytes(vec![1, 1, 0, 0]));
}
