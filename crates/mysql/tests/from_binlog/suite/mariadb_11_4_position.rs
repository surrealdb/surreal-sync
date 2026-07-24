//! MariaDB 11.4+ transactions may emit `log_pos=0` on intra-txn events (go-mysql #893).
//! Position tracking must still advance via file_offset + event_size.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use mysql_async::prelude::*;
use surreal_sync_mysql::binlog_protocol::{BinlogClient, EventBody, ReplicaOptions, SslMode};

#[tokio::test]
async fn mariadb_11_4_intra_txn_zero_log_pos_still_advances_position() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mariadb_binlog().await;
    let conn_str = crate::shared::create_test_db(container, "mdb_pos").await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE pos_probe (id INT PRIMARY KEY, v INT)")
        .await?;

    let binlog_conn = crate::shared::repl_connection_string(&conn_str);
    let (host, port, user, pass, _) = crate::shared::parse_mysql_uri(&binlog_conn)?;
    let mut client = BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username: user,
        password: pass,
        server_id: 9_004_050,
        ssl: SslMode::Disabled,
        blocking_poll: Duration::from_millis(200),
        flavor: Some(surreal_sync_mysql::binlog_protocol::Flavor::MariaDb),
        mariadb_flags: surreal_sync_mysql::binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode:
            surreal_sync_mysql::binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await
    .context("connect binlog client")?;

    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    conn.query_drop("START TRANSACTION").await?;
    for i in 1..=5 {
        conn.exec_drop("INSERT INTO pos_probe (id, v) VALUES (?, ?)", (i, i * 10))
            .await?;
    }
    conn.exec_drop("UPDATE pos_probe SET v = v + 1 WHERE id <= 3", ())
        .await?;
    conn.query_drop("COMMIT").await?;

    let mut table_maps: HashMap<u64, surreal_sync_mysql::binlog_protocol::TableMapEvent> =
        HashMap::new();
    let mut positions = Vec::new();
    let mut saw_zero_log_pos = false;

    for _ in 0..100 {
        let events = client.next_events(32).await.context("next_events")?;
        for event in events {
            if event.header.log_pos == 0 {
                saw_zero_log_pos = true;
            }
            match event.body {
                EventBody::TableMap(tm) => {
                    table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows)
                    if table_maps
                        .get(&rows.table_id)
                        .is_some_and(|tm| tm.table == "pos_probe") =>
                {
                    positions.push(client.current_position());
                }
                _ => {}
            }
        }
        if positions.len() >= 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        positions.len() >= 6,
        "expected multiple row events in txn, got {}",
        positions.len()
    );
    assert!(
        saw_zero_log_pos,
        "MariaDB 11.4+ should emit at least one event with log_pos=0 in this scenario"
    );

    for window in positions.windows(2) {
        assert!(
            window[0] < window[1],
            "positions must strictly advance even when log_pos=0: {:?} -> {:?}",
            window[0],
            window[1]
        );
    }

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}
