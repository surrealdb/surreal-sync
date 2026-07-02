//! INSERT/UPDATE/DELETE, transaction boundaries, and GTID resume.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use binlog_protocol::{BinlogClient, EventBody, ReplicaOptions, RowChange, SslMode};
use mysql_async::prelude::*;
use surreal_sync_mysql_binlog_source::{run_incremental_sync, BinlogCheckpoint, SourceOpts};

struct MemSink {
    changes: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl surreal_sink::SurrealSink for MemSink {
    async fn write_universal_rows(&self, _rows: &[sync_core::UniversalRow]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[sync_core::UniversalRelation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_universal_change(
        &self,
        change: &sync_core::UniversalChange,
    ) -> anyhow::Result<()> {
        self.changes
            .lock()
            .expect("lock")
            .push(format!("{:?}:{}", change.operation, change.table));
        Ok(())
    }

    async fn apply_universal_relation_change(
        &self,
        _change: &sync_core::UniversalRelationChange,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

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
    })
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))
}

#[tokio::test]
async fn dml_and_transaction_boundaries_observed() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let conn_str = crate::shared::create_test_db(container, "integ_dml").await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE items (id INT PRIMARY KEY, val INT)")
        .await?;

    let mut client = connect_client(&conn_str, 9_003_001).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    conn.query_drop("START TRANSACTION").await?;
    conn.exec_drop("INSERT INTO items (id, val) VALUES (1, 10)", ())
        .await?;
    conn.exec_drop("UPDATE items SET val = 20 WHERE id = 1", ())
        .await?;
    conn.query_drop("COMMIT").await?;
    conn.exec_drop("DELETE FROM items WHERE id = 1", ()).await?;

    let mut table_maps: HashMap<u64, binlog_protocol::TableMapEvent> = HashMap::new();
    let mut ops = Vec::new();
    for _ in 0..100 {
        let events = client
            .next_events(32)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for event in events {
            match event.body {
                EventBody::TableMap(tm) => {
                    table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows)
                    if table_maps
                        .get(&rows.table_id)
                        .is_some_and(|tm| tm.table == "items") =>
                {
                    for row in rows.rows {
                        ops.push(match row {
                            RowChange::Insert(_) => "insert",
                            RowChange::Update { .. } => "update",
                            RowChange::Delete(_) => "delete",
                        });
                    }
                }
                EventBody::Xid(_) => ops.push("xid"),
                _ => {}
            }
        }
        if ops.iter().filter(|o| **o != "xid").count() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(ops.contains(&"insert"));
    assert!(ops.contains(&"update"));
    assert!(ops.contains(&"delete"));
    assert!(ops.contains(&"xid"));

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn gtid_checkpoint_resume_applies_subsequent_changes() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_gtid";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;

    let mut client = connect_client(&conn_str, 9_003_002).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (1, 1)", ())
        .await?;
    // Drain first insert so checkpoint is past it.
    drain_items_events(&mut client).await?;

    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };

    drop(client);

    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (2, 2)", ())
        .await?;
    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (3, 3)", ())
        .await?;

    let sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_003),
        flavor: Some(container.flavor()),
        mysql_boolean_paths: None,
    };

    run_incremental_sync(
        &sink,
        source_opts,
        checkpoint,
        chrono::Utc::now() + chrono::Duration::seconds(30),
        None,
    )
    .await?;

    let applied = sink.changes.lock().expect("lock").clone();
    assert!(
        applied.iter().any(|s| s.contains("widgets")),
        "expected widget changes, got {applied:?}"
    );
    assert!(
        applied.len() >= 2,
        "expected at least two incremental changes, got {applied:?}"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn repl_user_can_stream_changes() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_repl";
    let admin_conn = crate::shared::create_test_db(container, db_name).await?;

    let admin_pool = mysql_async::Pool::from_url(&admin_conn)?;
    let mut admin = admin_pool.get_conn().await?;
    admin
        .query_drop(
            "CREATE USER IF NOT EXISTS 'surreal_sync'@'%' IDENTIFIED BY 'surreal_sync_pass'",
        )
        .await?;
    admin
        .query_drop("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'surreal_sync'@'%'")
        .await?;
    admin
        .query_drop(format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP ON `{db_name}`.* TO 'surreal_sync'@'%'"
        ))
        .await?;
    admin.query_drop("FLUSH PRIVILEGES").await?;
    admin
        .query_drop("CREATE TABLE repl_probe (id INT PRIMARY KEY, v INT)")
        .await?;

    let repl_conn = admin_conn.replace("root:testpass@", "surreal_sync:surreal_sync_pass@");

    let mut client = connect_client(&repl_conn, 9_003_004).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &repl_conn).await?;

    admin
        .exec_drop("INSERT INTO repl_probe (id, v) VALUES (1, 42)", ())
        .await?;

    let mut table_maps = HashMap::new();
    let mut saw = false;
    for _ in 0..60 {
        let events = client
            .next_events(16)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for event in events {
            if let EventBody::TableMap(tm) = event.body {
                table_maps.insert(tm.table_id, tm);
            } else if let EventBody::Rows(rows) = event.body {
                if table_maps
                    .get(&rows.table_id)
                    .is_some_and(|tm| tm.table == "repl_probe")
                {
                    saw = true;
                    break;
                }
            }
        }
        if saw {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(saw, "repl user should receive row events");

    drop(admin);
    admin_pool.disconnect().await?;
    Ok(())
}

async fn drain_items_events(client: &mut BinlogClient) -> Result<()> {
    let mut table_maps = HashMap::new();
    for _ in 0..60 {
        let events = client
            .next_events(32)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for event in events {
            if let EventBody::TableMap(tm) = event.body {
                table_maps.insert(tm.table_id, tm);
            } else if let EventBody::Rows(rows) = event.body {
                if table_maps
                    .get(&rows.table_id)
                    .is_some_and(|tm| tm.table == "widgets" || tm.table == "items")
                {
                    client.commit(client.current_position());
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("timed out draining initial row event")
}
