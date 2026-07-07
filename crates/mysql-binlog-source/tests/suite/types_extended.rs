//! Extended binlog-only types missing from the unified dataset (GEOMETRY, BIT, ENUM, etc.).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use binlog_protocol::{BinlogClient, CellValue, EventBody, ReplicaOptions, SslMode};
use mysql_async::prelude::*;

async fn connect_client(conn_str: &str, server_id: u32) -> Result<BinlogClient> {
    let binlog_conn = crate::shared::repl_connection_string(conn_str);
    let (host, port, user, pass, _) = crate::shared::parse_mysql_uri(&binlog_conn)?;
    BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username: user,
        password: pass,
        server_id,
        ssl: SslMode::Disabled,
        blocking_poll: Duration::from_millis(200),
        flavor: None,
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))
}

#[tokio::test]
async fn extended_types_decode_from_binlog_stream() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let conn_str = crate::shared::create_test_db(container, "types_ext").await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop(
        "CREATE TABLE all_types_binlog (
            id INT PRIMARY KEY,
            geom GEOMETRY NOT NULL,
            flag BIT(8) NOT NULL,
            status ENUM('active','inactive') NOT NULL,
            yr YEAR NOT NULL,
            t TIME NOT NULL,
            payload BLOB NOT NULL,
            blob_data BLOB NOT NULL
        )",
    )
    .await?;

    let mut client = connect_client(&conn_str, 9_002_001).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    conn.exec_drop(
        "INSERT INTO all_types_binlog \
         (id, geom, flag, status, yr, t, payload, blob_data) \
         VALUES (1, ST_GeomFromText('POINT(1 2)'), b'10101010', 'active', 2024, '13:45:00', \
                 UNHEX('DEADBEEF'), UNHEX('010203'))",
        (),
    )
    .await?;

    let mut table_maps: HashMap<u64, binlog_protocol::TableMapEvent> = HashMap::new();
    let mut cells: Option<Vec<CellValue>> = None;
    for _ in 0..80 {
        let events = client
            .next_events(32)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for event in events {
            if let EventBody::TableMap(tm) = event.body {
                table_maps.insert(tm.table_id, tm);
            } else if let EventBody::Rows(rows) = event.body {
                let Some(tm) = table_maps.get(&rows.table_id) else {
                    continue;
                };
                if tm.table != "all_types_binlog" {
                    continue;
                }
                if let Some(row) = rows.rows.into_iter().next() {
                    cells = Some(match row {
                        binlog_protocol::RowChange::Insert(v) => v,
                        _ => continue,
                    });
                    break;
                }
            }
        }
        if cells.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let cells = cells.context("expected INSERT row for all_types_binlog")?;
    assert_eq!(cells.len(), 8);

    assert!(matches!(cells[1], CellValue::Bytes(_)), "geometry as bytes");
    assert!(matches!(cells[2], CellValue::Bit(_)), "BIT column");
    // Binlog carries the 1-based ENUM index only; label lookup needs schema metadata.
    assert!(
        matches!(cells[3], CellValue::String(ref s) if s == "1")
            || matches!(cells[3], CellValue::Int(1)),
        "ENUM index for 'active', got {:?}",
        cells[3]
    );
    assert!(
        matches!(
            cells[4],
            CellValue::Int(_) | CellValue::UInt(_) | CellValue::Year(_)
        ),
        "YEAR column, got {:?}",
        cells[4]
    );
    assert!(matches!(cells[5], CellValue::Time { .. }), "TIME column");
    assert!(
        matches!(cells[6], CellValue::Bytes(_) | CellValue::String(_)),
        "payload BLOB, got {:?}",
        cells[6]
    );
    assert!(
        matches!(cells[7], CellValue::Bytes(_) | CellValue::String(_)),
        "BLOB, got {:?}",
        cells[7]
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}
