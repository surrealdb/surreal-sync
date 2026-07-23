//! INSERT/UPDATE/DELETE, transaction boundaries, and GTID resume.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use binlog_protocol::{BinlogClient, EventBody, ReplicaOptions, RowChange, SslMode};
use checkpoint::Checkpoint;
use mysql_async::prelude::*;
use surreal_sync_mysql_binlog_source::{
    run_replication_tail_with_checkpoints, BinlogCheckpoint, CatchUpProgress,
    ReplicationTailOptions, SourceOpts,
};
use sync_core::{UniversalChange, UniversalValue};

struct MemSink {
    changes: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl surreal_sink::SurrealSink for MemSink {
    async fn write_universal_rows(&self, rows: &[sync_core::UniversalRow]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; mirror observations.
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            changes.push(format!(
                "{:?}:{}",
                sync_core::UniversalChangeOp::Update,
                row.table
            ));
        }
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

struct CaptureSink {
    changes: std::sync::Mutex<Vec<UniversalChange>>,
}

#[async_trait::async_trait]
impl surreal_sink::SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[sync_core::UniversalRow]) -> anyhow::Result<()> {
        // Homogeneous Update upserts coalesce here; mirror into `changes`.
        let mut changes = self.changes.lock().expect("lock");
        for row in rows {
            changes.push(UniversalChange::update(
                row.table.clone(),
                row.id.clone(),
                row.fields.clone(),
            ));
        }
        Ok(())
    }

    async fn write_universal_relations(
        &self,
        _relations: &[sync_core::UniversalRelation],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn apply_universal_change(&self, change: &UniversalChange) -> anyhow::Result<()> {
        self.changes.lock().expect("lock").push(change.clone());
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
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
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
    // Generous budget so full-suite parallel container load can't starve this poll.
    for _ in 0..300 {
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
async fn mysql_partial_json_update_is_applied_as_full_json_change() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    if container.flavor() != binlog_protocol::Flavor::MySql {
        return Ok(());
    }
    let db_name = "integ_partial_json";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE docs (id INT PRIMARY KEY, doc JSON NOT NULL)")
        .await?;
    conn.exec_drop(
        "INSERT INTO docs (id, doc) VALUES (1, JSON_OBJECT('nested', JSON_OBJECT('count', 1), 'pad', REPEAT('x', 2048)))",
        (),
    )
    .await?;

    let mut client = connect_client(&conn_str, 9_003_004).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;
    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.query_drop("SET SESSION binlog_row_value_options = 'PARTIAL_JSON'")
        .await?;
    conn.exec_drop(
        "UPDATE docs SET doc = JSON_SET(doc, '$.nested.count', 42) WHERE id = 1",
        (),
    )
    .await?;

    let sink = CaptureSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["docs".to_string()],
        server_id: Some(9_003_005),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };
    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(5)),
            None,
        ),
        None,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    let update = changes
        .iter()
        .find(|change| change.table == "docs")
        .expect("expected docs update");
    let data = update.data.as_ref().expect("update data");
    let Some(UniversalValue::Json(json)) = data.get("doc") else {
        panic!("expected JSON doc value, got {:?}", data.get("doc"));
    };
    assert_eq!(json["nested"]["count"], serde_json::json!(42));
    assert_eq!(json["pad"], serde_json::json!("x".repeat(2048)));

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
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let tmp = tempfile::TempDir::new()?;
    let manager = checkpoint::SyncManager::new(checkpoint::FilesystemStore::new(tmp.path()));
    run_replication_tail_with_checkpoints(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        Some(&manager),
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
    let persisted: CatchUpProgress = manager
        .read_checkpoint(checkpoint::SyncPhase::CatchUpProgress)
        .await?;
    assert_ne!(
        persisted.position.position,
        BinlogCheckpoint::from_cli_string("file:mysql-bin.000001:4")?.position,
        "incremental sync should persist a real stream checkpoint"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn filtered_only_catch_up_advances_catch_up_progress_when_drained() -> Result<()> {
    // Syncing only `keep` while unrelated `noise` rows flood the binlog must still
    // advance CatchUpProgress on the interval once the apply window is drained
    // (no commits — filtered traffic never sinks).
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_filtered_catchup";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE keep (id INT PRIMARY KEY, n INT)")
        .await?;
    conn.query_drop("CREATE TABLE noise (id INT PRIMARY KEY, n INT)")
        .await?;

    let mut client = connect_client(&conn_str, 9_003_080).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;
    let start = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    for i in 1..=40 {
        conn.exec_drop("INSERT INTO noise (id, n) VALUES (?, ?)", (i, i * 10))
            .await?;
    }

    let sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["keep".to_string()],
        server_id: Some(9_003_081),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let tmp = tempfile::TempDir::new()?;
    let manager = checkpoint::SyncManager::new(checkpoint::FilesystemStore::new(tmp.path()));
    use tokio_util::sync::CancellationToken;
    let cancel = CancellationToken::new();
    let canceller = cancel.clone();
    tokio::spawn(async move {
        // Enough time for interval persists through filtered noise; avoid a long deadline.
        tokio::time::sleep(Duration::from_secs(2)).await;
        canceller.cancel();
    });
    let mut options = ReplicationTailOptions::stream(None, None).with_cancel(cancel);
    options.checkpoint_interval = Duration::from_millis(50);

    run_replication_tail_with_checkpoints(
        &sink,
        source_opts,
        start.clone(),
        options,
        Some(&manager),
    )
    .await?;

    let applied = sink.changes.lock().expect("lock").clone();
    assert!(
        applied.is_empty(),
        "filtered-only catch-up must not sink noise rows, got {applied:?}"
    );

    let persisted: CatchUpProgress = manager
        .read_checkpoint(checkpoint::SyncPhase::CatchUpProgress)
        .await?;
    assert!(
        persisted.position.position > start.position,
        "CatchUpProgress must advance through filtered noise: start={:?} persisted={:?}",
        start.position,
        persisted.position.position
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn ddl_refreshes_schema_before_subsequent_rows() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_ddl_refresh";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, status ENUM('new'))")
        .await?;

    let mut client = connect_client(&conn_str, 9_003_030).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;
    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.query_drop("ALTER TABLE widgets MODIFY status ENUM('new','done')")
        .await?;
    conn.exec_drop("INSERT INTO widgets (id, status) VALUES (1, 'done')", ())
        .await?;

    let sink = CaptureSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_031),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    let status = changes
        .iter()
        .find_map(|change| change.data.as_ref()?.get("status"))
        .expect("status field should be present after DDL refresh");
    match status {
        UniversalValue::Enum { value, .. } => assert_eq!(value, "done"),
        other => panic!("expected refreshed ENUM label, got {other:?}"),
    }

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn mariadb_gtid_checkpoint_resume_applies_subsequent_changes() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mariadb_binlog().await;
    let db_name = "integ_gtid_mdb";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;

    let mut client = connect_client(&conn_str, 9_004_001).await?;
    // Start in GTID mode so the captured checkpoint is a MariaDbGtid position.
    crate::shared::start_binlog_at_mariadb_gtid_end(&mut client, &conn_str).await?;

    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (1, 1)", ())
        .await?;
    // Drain first insert so checkpoint is past it.
    drain_items_events(&mut client).await?;

    let position = client.current_position();
    assert!(
        matches!(
            position,
            binlog_protocol::BinlogPosition::MariaDbGtid { .. }
        ),
        "expected a MariaDbGtid runtime checkpoint, got {position:?}"
    );

    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position,
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
        server_id: Some(9_004_002),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
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
        "expected at least two incremental changes after GTID resume, got {applied:?}"
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
    // Generous budget so full-suite parallel container load can't starve this poll.
    for _ in 0..300 {
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

/// MySQL 8 `caching_sha2_password` full auth over plaintext (RSA) must succeed
/// without TLS. A freshly created user forces a cache miss (AuthMoreData 0x04).
#[tokio::test]
async fn caching_sha2_rsa_full_auth_without_tls() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_sha2_rsa";
    let admin_conn = crate::shared::create_test_db(container, db_name).await?;

    let admin_pool = mysql_async::Pool::from_url(&admin_conn)?;
    let mut admin = admin_pool.get_conn().await?;

    // Unique user so the server has no cached password hash yet.
    let user = format!("sha2_rsa_{}", std::process::id());
    let pass = "sha2_rsa_pass";
    admin
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await?;
    admin
        .query_drop(format!(
            "CREATE USER '{user}'@'%' IDENTIFIED WITH caching_sha2_password BY '{pass}'"
        ))
        .await?;
    admin
        .query_drop(format!(
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%'"
        ))
        .await?;
    admin
        .query_drop(format!("GRANT SELECT ON `{db_name}`.* TO '{user}'@'%'"))
        .await?;
    admin.query_drop("FLUSH PRIVILEGES").await?;

    let sha2_conn = admin_conn.replacen("root:testpass@", &format!("{user}:{pass}@"), 1);
    let (host, port, username, password, _) = crate::shared::parse_mysql_uri(&sha2_conn)?;

    // Plaintext connection: must use RSA full auth, not TLS cleartext.
    let client = BinlogClient::connect(ReplicaOptions {
        host,
        port,
        username,
        password,
        server_id: 9_003_020,
        ssl: SslMode::Disabled,
        blocking_poll: Duration::from_millis(200),
        flavor: None,
        mariadb_flags: binlog_protocol::MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
    })
    .await
    .map_err(|e| anyhow::anyhow!("caching_sha2 RSA full auth failed: {e}"))?;

    drop(client);
    admin
        .query_drop(format!("DROP USER IF EXISTS '{user}'@'%'"))
        .await?;
    drop(admin);
    admin_pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn mysql_runtime_checkpoint_is_gtid() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_mysql_gtid";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;

    // Start from the current master position via file+pos. On a GTID-enabled
    // MySQL server the stream still carries GTID_LOG_EVENTs, so the tracker must
    // accumulate them and report a MySqlGtid runtime position.
    let mut client = connect_client(&conn_str, 9_003_010).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;

    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (1, 1)", ())
        .await?;
    drain_items_events(&mut client).await?;

    let position = client.current_position();
    assert!(
        matches!(position, binlog_protocol::BinlogPosition::MySqlGtid { .. }),
        "expected a MySqlGtid runtime checkpoint on a GTID-enabled MySQL server, got {position:?}"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn sequential_full_sync_captures_real_master_position() -> Result<()> {
    use checkpoint::{Checkpoint, SyncManager, SyncPhase};
    use surreal_sync_mysql_binlog_source::{run_full_sync, SyncOpts};

    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_seq_full";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE gadgets (id INT PRIMARY KEY, n INT)")
        .await?;
    conn.exec_drop("INSERT INTO gadgets (id, n) VALUES (1, 10)", ())
        .await?;
    conn.exec_drop("INSERT INTO gadgets (id, n) VALUES (2, 20)", ())
        .await?;

    let tmp = tempfile::TempDir::new()?;
    let manager = SyncManager::new(checkpoint::FilesystemStore::new(tmp.path()));

    let sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["gadgets".to_string()],
        server_id: Some(9_003_020),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };
    let sync_opts = SyncOpts {
        batch_size: 1000,
        dry_run: false,
    };

    run_full_sync(&sink, &source_opts, &sync_opts, Some(&manager)).await?;

    let end: BinlogCheckpoint = manager.read_checkpoint(SyncPhase::FullSyncEnd).await?;
    assert_ne!(
        end.to_cli_string(),
        "file:mysql-bin.000001:4",
        "FullSyncEnd must capture the real master position, not the placeholder"
    );

    // A post-snapshot insert must be captured by incremental resume from the end
    // checkpoint (no gap beyond at-least-once).
    conn.exec_drop("INSERT INTO gadgets (id, n) VALUES (3, 30)", ())
        .await?;

    let inc_sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let inc_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["gadgets".to_string()],
        server_id: Some(9_003_021),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };
    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &inc_sink,
        inc_opts,
        end,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;

    let applied = inc_sink.changes.lock().expect("lock").clone();
    assert!(
        applied.iter().any(|s| s.contains("gadgets")),
        "expected the post-snapshot insert to be captured after resuming from FullSyncEnd, got {applied:?}"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn interleaved_snapshot_cancel_persists_lower_bound_and_skips_end() -> Result<()> {
    use tokio_util::sync::CancellationToken;

    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_interleaved_cancel";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;
    for i in 1..=50 {
        conn.exec_drop("INSERT INTO widgets (id, n) VALUES (?, ?)", (i, i))
            .await?;
    }

    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_070),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let tmp = tempfile::TempDir::new()?;
    let manager = checkpoint::SyncManager::new(checkpoint::FilesystemStore::new(tmp.path()));

    // Pre-cancel: the snapshot emits FullSyncStart (the streaming lower bound)
    // before copying rows, then unwinds at the first stream poll.
    let cancel = CancellationToken::new();
    cancel.cancel();

    let outcome = surreal_sync_mysql_binlog_source::run_interleaved_snapshot_full_sync(
        &sink,
        &source_opts,
        8,
        cancel,
        Some(&manager),
        surreal_sync_mysql_binlog_source::InterleavedFullSyncOptions::default(),
    )
    .await?;

    assert!(
        outcome.cancelled,
        "expected the snapshot to report cancelled"
    );

    // FullSyncStart (lower bound) must be persisted so a restart re-snapshots
    // and resumes streaming from here without missing changes.
    let start: BinlogCheckpoint = manager
        .read_checkpoint(checkpoint::SyncPhase::FullSyncStart)
        .await?;
    assert_eq!(start.position, outcome.start.position);

    // FullSyncEnd must NOT have been emitted on cancellation.
    assert!(
        manager
            .read_checkpoint::<BinlogCheckpoint>(checkpoint::SyncPhase::FullSyncEnd)
            .await
            .is_err(),
        "FullSyncEnd must not be written when the snapshot is cancelled"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn rename_table_mid_stream_keeps_tracking_new_name() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_rename";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;

    let mut client = connect_client(&conn_str, 9_003_040).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;
    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    // Rename a SYNCED table mid-stream, then write to the new name.
    conn.query_drop("RENAME TABLE widgets TO widgets_v2")
        .await?;
    conn.exec_drop("INSERT INTO widgets_v2 (id, n) VALUES (1, 42)", ())
        .await?;

    let sink = CaptureSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        // The user syncs "widgets"; after RENAME the stream must follow to
        // "widgets_v2" instead of silently filtering the change out.
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_041),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert!(
        changes.iter().any(|c| c.table == "widgets_v2"),
        "expected a change on the renamed table 'widgets_v2', got {changes:?}"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_stops_follow_and_flushes_resumable_checkpoint() -> Result<()> {
    use tokio_util::sync::CancellationToken;

    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_cancel";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;

    let mut client = connect_client(&conn_str, 9_003_050).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;
    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (1, 1)", ())
        .await?;

    let sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_051),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let tmp = tempfile::TempDir::new()?;
    let manager = checkpoint::SyncManager::new(checkpoint::FilesystemStore::new(tmp.path()));
    let cancel = CancellationToken::new();

    // Cancel a continuous follow after a short delay; the sync must return
    // cleanly (not error, not hang) after flushing a checkpoint.
    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        canceller.cancel();
    });

    run_replication_tail_with_checkpoints(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(None, None).with_cancel(cancel),
        Some(&manager),
    )
    .await?;

    let applied = sink.changes.lock().expect("lock").clone();
    assert!(
        applied.iter().any(|s| s.contains("widgets")),
        "expected the pre-cancel insert to be applied, got {applied:?}"
    );

    // A resumable CatchUpProgress checkpoint must have been flushed on the way out.
    let progress: CatchUpProgress = manager
        .read_checkpoint(checkpoint::SyncPhase::CatchUpProgress)
        .await?;
    assert!(
        manager
            .read_checkpoint::<BinlogCheckpoint>(checkpoint::SyncPhase::FullSyncEnd)
            .await
            .is_err(),
        "stream heartbeats must not overwrite FullSyncEnd"
    );

    // Resume from the flushed checkpoint and observe a subsequent insert.
    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (2, 2)", ())
        .await?;
    let resume_sink = MemSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    let resume_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_052),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };
    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &resume_sink,
        resume_opts,
        progress.position,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;
    let resumed = resume_sink.changes.lock().expect("lock").clone();
    assert!(
        resumed.iter().any(|s| s.contains("widgets")),
        "expected the post-cancel insert to be captured on resume, got {resumed:?}"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn start_at_head_checkpoint_resumes_from_current_position() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_head";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, n INT)")
        .await?;
    // Rows written BEFORE capturing head must NOT be replayed.
    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (1, 1)", ())
        .await?;

    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["widgets".to_string()],
        server_id: Some(9_003_060),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    // Explicit "start at head": capture the current master position.
    let head = surreal_sync_mysql_binlog_source::capture_head_checkpoint(&source_opts).await?;

    // Rows written AFTER capturing head must be applied.
    conn.exec_drop("INSERT INTO widgets (id, n) VALUES (2, 2)", ())
        .await?;

    let sink = CaptureSink {
        changes: std::sync::Mutex::new(Vec::new()),
    };
    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        head,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(30)),
            None,
        ),
        None,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    let widget_ids: Vec<i64> = changes
        .iter()
        .filter(|c| c.table == "widgets")
        .filter_map(|c| match &c.id {
            UniversalValue::Int64(v) => Some(*v),
            UniversalValue::Int32(v) => Some(*v as i64),
            UniversalValue::Int8 { value, .. } => Some(*value as i64),
            _ => None,
        })
        .collect();
    assert!(
        widget_ids.contains(&2),
        "expected the post-head insert (id=2) to be applied, got ids {widget_ids:?}"
    );
    assert!(
        !widget_ids.contains(&1),
        "the pre-head insert (id=1) must NOT be replayed when starting at head, got {widget_ids:?}"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn steady_state_stream_honors_execute_snapshot_signal() -> Result<()> {
    use surreal_sync_mysql_binlog_source::request_snapshot;

    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "integ_adhoc_stream";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE items (id INT PRIMARY KEY, val INT)")
        .await?;
    for i in 1..=10 {
        conn.exec_drop("INSERT INTO items (id, val) VALUES (?, ?)", (i, i * 10))
            .await?;
    }

    let mut client = connect_client(&conn_str, 9_003_070).await?;
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str).await?;
    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.query_drop("CREATE TABLE extra (id INT PRIMARY KEY, score INT)")
        .await?;
    for i in 1..=8 {
        conn.exec_drop("INSERT INTO extra (id, score) VALUES (?, ?)", (i, i * 7))
            .await?;
    }
    request_snapshot(&pool, db_name, &["extra".to_string()]).await?;

    let sink = crate::common::MemSink::default();
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["items".to_string()],
        server_id: Some(9_003_071),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    run_replication_tail_with_checkpoints::<_, checkpoint::NullStore>(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(60)),
            None,
        ),
        None,
    )
    .await?;

    let items = sink.value_map("items", "val").await;
    assert!(
        items.is_empty(),
        "items were inserted before stream start and are not in the table filter snapshot path"
    );
    let extra = sink.value_map("extra", "score").await;
    assert_eq!(
        extra.len(),
        8,
        "extra table should be snapshotted via execute-snapshot signal during stream"
    );

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

async fn drain_items_events(client: &mut BinlogClient) -> Result<()> {
    let mut table_maps = HashMap::new();
    let mut saw_target_row = false;
    // Generous budget so full-suite parallel container load can't starve this poll.
    for _ in 0..300 {
        let events = client
            .next_events(32)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        for event in events {
            match event.body {
                EventBody::TableMap(tm) => {
                    table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows) => {
                    if table_maps
                        .get(&rows.table_id)
                        .is_some_and(|tm| tm.table == "widgets" || tm.table == "items")
                    {
                        saw_target_row = true;
                    }
                }
                EventBody::Xid(_) if saw_target_row => {
                    client.commit(client.current_position());
                    return Ok(());
                }
                _ => {}
            }
            if saw_target_row
                && matches!(
                    client.current_position(),
                    binlog_protocol::BinlogPosition::MySqlGtid { .. }
                        | binlog_protocol::BinlogPosition::MariaDbGtid { .. }
                )
            {
                client.commit(client.current_position());
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("timed out draining initial row event")
}
