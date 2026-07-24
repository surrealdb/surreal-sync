//! Shared helpers for captured binlog fixture capture and replay tests.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use surreal_sync_mysql::binlog_protocol::{CellValue, EventBody, Flavor, RawEvent};

pub const FIXTURE_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/binlog_protocol/fixtures/captured"
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureMeta {
    pub server_image: String,
    pub flavor: String,
    pub checksum_enabled: bool,
    pub description: String,
    pub sql: String,
}

pub fn fixture_paths(base: &str) -> (PathBuf, PathBuf) {
    let dir = Path::new(FIXTURE_DIR);
    (
        dir.join(format!("{base}.bin")),
        dir.join(format!("{base}.meta.json")),
    )
}

pub fn load_fixture(base: &str) -> (Vec<u8>, FixtureMeta) {
    let (bin_path, meta_path) = fixture_paths(base);
    let bytes = std::fs::read(&bin_path)
        .unwrap_or_else(|e| panic!("read fixture {}: {e}", bin_path.display()));
    let meta: FixtureMeta = serde_json::from_slice(
        &std::fs::read(&meta_path)
            .unwrap_or_else(|e| panic!("read meta {}: {e}", meta_path.display())),
    )
    .unwrap_or_else(|e| panic!("parse meta {}: {e}", meta_path.display()));
    (bytes, meta)
}

pub fn flavor_from_meta(meta: &FixtureMeta) -> Flavor {
    match meta.flavor.as_str() {
        "mariadb" => Flavor::MariaDb,
        _ => Flavor::MySql,
    }
}

#[derive(Debug, Serialize)]
pub struct SnapshotEvent {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_op: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cells: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zero_log_pos: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_alg: Option<u8>,
}

pub fn normalize_cell(cell: &CellValue) -> serde_json::Value {
    match cell {
        CellValue::Null => serde_json::Value::Null,
        CellValue::Int(v) => serde_json::json!(v),
        CellValue::UInt(v) => serde_json::json!(v),
        CellValue::Float(v) => serde_json::json!(v),
        CellValue::Double(v) => serde_json::json!(v),
        CellValue::Decimal(v) => serde_json::json!(v.to_string()),
        CellValue::String(v) => serde_json::json!(v),
        CellValue::Bytes(v) | CellValue::Bit(v) | CellValue::JsonBytes(v) => {
            serde_json::json!(hex::encode(v))
        }
        CellValue::JsonText(v) => serde_json::json!(v),
        CellValue::JsonDiff(diffs) => serde_json::json!(diffs
            .iter()
            .map(|diff| serde_json::json!({
                "op": format!("{:?}", diff.operation),
                "path": diff.path,
                "data": diff.data.as_ref().map(hex::encode),
            }))
            .collect::<Vec<_>>()),
        CellValue::Date { year, month, day } => {
            serde_json::json!({ "year": year, "month": month, "day": day })
        }
        CellValue::Time {
            hour,
            minute,
            second,
            micros,
        } => {
            serde_json::json!({ "hour": hour, "minute": minute, "second": second, "micros": micros })
        }
        CellValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        } => serde_json::json!({
            "year": year,
            "month": month,
            "day": day,
            "hour": hour,
            "minute": minute,
            "second": second,
            "micros": micros
        }),
        CellValue::TimestampMillis(v) => serde_json::json!(v),
        CellValue::Year(v) => serde_json::json!(v),
    }
}

pub fn normalize_events(events: Vec<RawEvent>, table: &str) -> Vec<SnapshotEvent> {
    let mut table_maps = std::collections::HashMap::new();
    let mut out = Vec::new();

    for event in events {
        match event.body {
            EventBody::FormatDescription(fde) => out.push(SnapshotEvent {
                kind: "FormatDescription".into(),
                database: None,
                table: None,
                query_prefix: None,
                gtid: None,
                xid: None,
                row_op: None,
                cells: None,
                before: None,
                after: None,
                zero_log_pos: None,
                checksum_alg: Some(fde.checksum_alg),
            }),
            EventBody::Gtid(gtid) => out.push(SnapshotEvent {
                kind: "Gtid".into(),
                database: None,
                table: None,
                query_prefix: None,
                gtid: Some(format!("{gtid:?}")),
                xid: None,
                row_op: None,
                cells: None,
                before: None,
                after: None,
                zero_log_pos: None,
                checksum_alg: None,
            }),
            EventBody::Query(q) => {
                let prefix = q.sql.chars().take(32).collect::<String>();
                out.push(SnapshotEvent {
                    kind: "Query".into(),
                    database: Some(q.database),
                    table: None,
                    query_prefix: Some(prefix),
                    gtid: None,
                    xid: None,
                    row_op: None,
                    cells: None,
                    before: None,
                    after: None,
                    zero_log_pos: None,
                    checksum_alg: None,
                });
            }
            EventBody::TableMap(tm) => {
                table_maps.insert(tm.table_id, tm.table.clone());
                if tm.table == table {
                    out.push(SnapshotEvent {
                        kind: "TableMap".into(),
                        database: Some(tm.database),
                        table: Some(tm.table),
                        query_prefix: None,
                        gtid: None,
                        xid: None,
                        row_op: None,
                        cells: None,
                        before: None,
                        after: None,
                        zero_log_pos: None,
                        checksum_alg: None,
                    });
                }
            }
            EventBody::Rows(rows) => {
                let Some(mapped_table) = table_maps.get(&rows.table_id) else {
                    continue;
                };
                if mapped_table != table {
                    continue;
                }
                for row in rows.rows {
                    match row {
                        surreal_sync_mysql::binlog_protocol::RowChange::Insert(cells) => {
                            out.push(SnapshotEvent {
                                kind: "Rows".into(),
                                database: None,
                                table: Some(table.into()),
                                query_prefix: None,
                                gtid: None,
                                xid: None,
                                row_op: Some("insert".into()),
                                cells: Some(cells.iter().map(normalize_cell).collect()),
                                before: None,
                                after: None,
                                zero_log_pos: Some(event.header.log_pos == 0),
                                checksum_alg: None,
                            })
                        }
                        surreal_sync_mysql::binlog_protocol::RowChange::Update {
                            before,
                            after,
                        } => {
                            out.push(SnapshotEvent {
                                kind: "Rows".into(),
                                database: None,
                                table: Some(table.into()),
                                query_prefix: None,
                                gtid: None,
                                xid: None,
                                row_op: Some("update".into()),
                                cells: None,
                                before: Some(before.iter().map(normalize_cell).collect()),
                                after: Some(after.iter().map(normalize_cell).collect()),
                                zero_log_pos: Some(event.header.log_pos == 0),
                                checksum_alg: None,
                            });
                        }
                        surreal_sync_mysql::binlog_protocol::RowChange::Delete(cells) => {
                            out.push(SnapshotEvent {
                                kind: "Rows".into(),
                                database: None,
                                table: Some(table.into()),
                                query_prefix: None,
                                gtid: None,
                                xid: None,
                                row_op: Some("delete".into()),
                                cells: Some(cells.iter().map(normalize_cell).collect()),
                                before: None,
                                after: None,
                                zero_log_pos: Some(event.header.log_pos == 0),
                                checksum_alg: None,
                            })
                        }
                    }
                }
            }
            EventBody::Xid(xid) => out.push(SnapshotEvent {
                kind: "Xid".into(),
                database: None,
                table: None,
                query_prefix: None,
                gtid: None,
                xid: Some(format!("xid={}", xid.xid)),
                row_op: None,
                cells: None,
                before: None,
                after: None,
                zero_log_pos: None,
                checksum_alg: None,
            }),
            EventBody::Rotate(_) | EventBody::Heartbeat(_) | EventBody::Ignored => {}
            EventBody::TransactionPayload(_) => {}
        }
    }

    out
}

pub fn mysql_schema_sql() -> &'static str {
    r#"
CREATE TABLE wire_fixture (
    pk1 INT NOT NULL,
    pk2 CHAR(8) NOT NULL,
    i64 BIGINT NOT NULL,
    dec_val DECIMAL(19,5) NOT NULL,
    f32_val FLOAT NOT NULL,
    txt VARCHAR(64) NOT NULL,
    jdoc JSON NOT NULL,
    enm ENUM('alpha','beta') NOT NULL,
    st SET('x','y','z') NOT NULL,
    bits BIT(8) NOT NULL,
    yr YEAR NOT NULL,
    dt DATE NOT NULL,
    tm TIME(3) NOT NULL,
    dtm DATETIME(6) NOT NULL,
    ts TIMESTAMP(3) NULL,
    blob_col VARBINARY(16) NOT NULL,
    geo GEOMETRY NOT NULL,
    PRIMARY KEY (pk1, pk2)
);
"#
}

pub fn mariadb_schema_sql() -> &'static str {
    r#"
CREATE TABLE wire_fixture (
    pk1 INT NOT NULL,
    pk2 CHAR(8) NOT NULL,
    i64 BIGINT NOT NULL,
    dec_val DECIMAL(19,5) NOT NULL,
    f32_val FLOAT NOT NULL,
    txt VARCHAR(64) NOT NULL,
    jdoc LONGTEXT NOT NULL,
    enm ENUM('alpha','beta') NOT NULL,
    st SET('x','y','z') NOT NULL,
    bits BIT(8) NOT NULL,
    yr YEAR NOT NULL,
    dt DATE NOT NULL,
    tm TIME(3) NOT NULL,
    dtm DATETIME(6) NOT NULL,
    ts TIMESTAMP(3) NULL,
    blob_col VARBINARY(16) NOT NULL,
    geo GEOMETRY NOT NULL,
    PRIMARY KEY (pk1, pk2)
);
"#
}

pub fn primary_insert_sql(json_literal: &str) -> String {
    format!(
        "INSERT INTO wire_fixture VALUES (
            1, 'pk-alpha',
            -9223372036854775807,
            12345.67890,
            3.14159,
            'hello-binlog',
            {json_literal},
            'alpha',
            'x,y',
            b'10101010',
            2024,
            '2024-06-15',
            '13:45:01.500',
            '2024-06-15 10:30:00.123456',
            '2024-06-15 10:30:00.000',
            UNHEX('DEADBEEF01234567'),
            ST_GeomFromText('POINT(1 2)')
        )"
    )
}

pub fn update_sql() -> &'static str {
    "UPDATE wire_fixture SET txt = 'updated', dec_val = 99999.99000, enm = 'beta', st = 'y,z' \
     WHERE pk1 = 1 AND pk2 = 'pk-alpha'"
}

pub fn delete_sql() -> &'static str {
    "DELETE FROM wire_fixture WHERE pk1 = 1 AND pk2 = 'pk-alpha'"
}

pub fn mariadb_txn_sql(json_literal: &str) -> String {
    format!(
        "START TRANSACTION;
         INSERT INTO wire_fixture VALUES (
            2, 'txn-two', 42, 1.00000, 1.5, 'txn-a', {json_literal}, 'alpha', 'x', b'00001111',
            2023, '2023-01-01', '01:02:03.000', '2023-01-01 01:02:03.000000', '2023-01-01 00:00:00.000',
            UNHEX('01020304'), ST_GeomFromText('POINT(3 4)'));
         INSERT INTO wire_fixture VALUES (
            3, 'txn-thr', 43, 2.00000, 2.5, 'txn-b', {json_literal}, 'beta', 'y', b'11110000',
            2022, '2022-02-02', '02:03:04.000', '2022-02-02 02:03:04.000000', '2022-02-02 00:00:00.000',
            UNHEX('05060708'), ST_GeomFromText('POINT(5 6)'));
         UPDATE wire_fixture SET txt = 'txn-upd' WHERE pk1 = 2;
         COMMIT;"
    )
}

pub fn capture_sql_block(flavor: Flavor) -> String {
    let (schema, json_lit) = match flavor {
        Flavor::MariaDb => (mariadb_schema_sql(), "'{\"a\":1,\"b\":\"two\"}'"),
        Flavor::MySql => (mysql_schema_sql(), "'{\"a\":1,\"b\":\"two\"}'"),
    };
    let mut sql = String::new();
    sql.push_str(schema);
    sql.push('\n');
    sql.push_str(&primary_insert_sql(json_lit));
    sql.push_str(";\n");
    sql.push_str(update_sql());
    sql.push_str(";\n");
    sql.push_str(delete_sql());
    sql.push_str(";\n");
    if flavor == Flavor::MariaDb {
        sql.push_str(&mariadb_txn_sql(json_lit));
        sql.push('\n');
    }
    sql
}
