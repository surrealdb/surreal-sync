//! Transform pipeline e2e: identity path + external worker against real MySQL.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use anyhow::Result;
use mysql_async::prelude::*;
use surreal_sink::SurrealSink;
use surreal_sync_mysql_binlog_source::{
    run_full_sync_cancellable_with_transforms, run_replication_tail_with_transforms,
    BinlogCheckpoint, ReplicationTailOptions, SourceOpts, SyncOpts,
};
use sync_core::{UniversalChange, UniversalRow, UniversalValue};
use sync_transform::{ApplyOpts, ChildStdioMode, ExternalTransform, Pipeline};

struct CaptureSink {
    changes: Mutex<Vec<UniversalChange>>,
    rows: Mutex<Vec<UniversalRow>>,
}

impl CaptureSink {
    fn new() -> Self {
        Self {
            changes: Mutex::new(Vec::new()),
            rows: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SurrealSink for CaptureSink {
    async fn write_universal_rows(&self, rows: &[UniversalRow]) -> anyhow::Result<()> {
        self.rows.lock().expect("lock").extend(rows.iter().cloned());
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

fn fixture_worker_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates
    p.pop(); // workspace root
    p.push("target");
    p.push("debug");
    p.push("sync-transform-fixture-worker");
    p
}

fn ensure_fixture_worker() -> PathBuf {
    let path = fixture_worker_path();
    if !path.is_file() {
        let status = Command::new("cargo")
            .args([
                "build",
                "-p",
                "sync-transform",
                "--bin",
                "sync-transform-fixture-worker",
            ])
            .current_dir({
                let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                root.pop();
                root.pop();
                root
            })
            .status()
            .expect("spawn cargo build fixture worker");
        assert!(
            status.success(),
            "failed to build sync-transform-fixture-worker"
        );
    }
    assert!(
        path.is_file(),
        "fixture worker missing after build: {}",
        path.display()
    );
    path
}

fn name_field(change: &UniversalChange) -> Option<String> {
    let data = change.data.as_ref()?;
    match data.get("name")? {
        UniversalValue::VarChar { value, .. } | UniversalValue::Text(value) => Some(value.clone()),
        other => panic!("unexpected name value: {other:?}"),
    }
}

fn row_name_field(row: &UniversalRow) -> Option<String> {
    match row.fields.get("name")? {
        UniversalValue::VarChar { value, .. } | UniversalValue::Text(value) => Some(value.clone()),
        other => panic!("unexpected name value: {other:?}"),
    }
}

#[tokio::test]
async fn identity_pipeline_matches_direct_incremental_sync() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "xf_identity";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;

    let mut client = {
        let binlog_conn = crate::shared::repl_connection_string(&conn_str);
        let (host, port, user, pass, _) = crate::shared::parse_mysql_uri(&binlog_conn)?;
        binlog_protocol::BinlogClient::connect(binlog_protocol::ReplicaOptions {
            host,
            port,
            username: user,
            password: pass,
            server_id: 9_004_001,
            ssl: binlog_protocol::SslMode::Disabled,
            blocking_poll: std::time::Duration::from_millis(200),
            flavor: None,
            mariadb_flags: binlog_protocol::MariaDbDumpFlags {
                send_annotate_rows: true,
            },
            mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
    };
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str)
        .await?;

    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.exec_drop(
        "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
        (),
    )
    .await?;

    let sink = CaptureSink::new();
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["people".to_string()],
        server_id: Some(9_004_002),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    let pipeline = Pipeline::new();
    let apply_opts = ApplyOpts::identity();
    run_replication_tail_with_transforms(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(20)),
            None,
        ),
        None::<&checkpoint::SyncManager<checkpoint::NullStore>>,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(changes.len(), 2, "expected two inserts, got {changes:?}");
    let mut names: Vec<_> = changes.iter().filter_map(name_field).collect();
    names.sort();
    assert_eq!(names, vec!["alice".to_string(), "bob".to_string()]);

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn external_mutate_worker_transforms_incremental_changes() -> Result<()> {
    crate::shared::init_logging();
    let worker = ensure_fixture_worker();

    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "xf_external";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;

    let mut client = {
        let binlog_conn = crate::shared::repl_connection_string(&conn_str);
        let (host, port, user, pass, _) = crate::shared::parse_mysql_uri(&binlog_conn)?;
        binlog_protocol::BinlogClient::connect(binlog_protocol::ReplicaOptions {
            host,
            port,
            username: user,
            password: pass,
            server_id: 9_004_011,
            ssl: binlog_protocol::SslMode::Disabled,
            blocking_poll: std::time::Duration::from_millis(200),
            flavor: None,
            mariadb_flags: binlog_protocol::MariaDbDumpFlags {
                send_annotate_rows: true,
            },
            mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
    };
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str)
        .await?;

    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.exec_drop(
        "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
        (),
    )
    .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);

    let sink = CaptureSink::new();
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["people".to_string()],
        server_id: Some(9_004_012),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    run_replication_tail_with_transforms(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(20)),
            None,
        ),
        None::<&checkpoint::SyncManager<checkpoint::NullStore>>,
        &pipeline,
        &apply_opts,
    )
    .await?;

    let changes = sink.changes.lock().expect("lock").clone();
    assert_eq!(changes.len(), 2, "expected two transformed inserts, got {changes:?}");
    for change in &changes {
        assert_eq!(
            name_field(change).as_deref(),
            Some("mutated"),
            "external mutate worker should rewrite name; got {change:?}"
        );
    }

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn external_mutate_worker_transforms_full_sync_rows() -> Result<()> {
    crate::shared::init_logging();
    let worker = ensure_fixture_worker();

    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "xf_full";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;
    conn.exec_drop(
        "INSERT INTO people (id, name) VALUES (1, 'carol'), (2, 'dave')",
        (),
    )
    .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity();

    let sink = CaptureSink::new();
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["people".to_string()],
        server_id: Some(9_004_021),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };
    let sync_opts = SyncOpts {
        batch_size: 100,
        dry_run: false,
    };

    run_full_sync_cancellable_with_transforms(
        &sink,
        &source_opts,
        &sync_opts,
        None::<&checkpoint::SyncManager<checkpoint::NullStore>>,
        &tokio_util::sync::CancellationToken::new(),
        &pipeline,
        &apply_opts,
    )
    .await?;

    let rows = sink.rows.lock().expect("lock").clone();
    assert_eq!(rows.len(), 2, "expected two snapshotted rows, got {rows:?}");
    for row in &rows {
        assert_eq!(
            row_name_field(row).as_deref(),
            Some("mutated"),
            "full sync write_rows should run external mutate; got {row:?}"
        );
    }

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}

async fn mem_surreal_sink(
) -> Result<(surreal2_sink::Surreal2Sink, surrealdb::Surreal<surrealdb::engine::any::Any>)> {
    let db = surrealdb::engine::any::connect("memory").await?;
    db.use_ns("test").use_db("test").await?;
    Ok((surreal2_sink::Surreal2Sink::new(db.clone()), db))
}

#[tokio::test]
async fn external_mutate_worker_writes_mutated_fields_to_surrealdb() -> Result<()> {
    crate::shared::init_logging();
    let worker = ensure_fixture_worker();

    let container = crate::shared::shared_mysql_binlog().await;
    let db_name = "xf_surreal";
    let conn_str = crate::shared::create_test_db(container, db_name).await?;

    let pool = mysql_async::Pool::from_url(&conn_str)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(64))")
        .await?;

    let mut client = {
        let binlog_conn = crate::shared::repl_connection_string(&conn_str);
        let (host, port, user, pass, _) = crate::shared::parse_mysql_uri(&binlog_conn)?;
        binlog_protocol::BinlogClient::connect(binlog_protocol::ReplicaOptions {
            host,
            port,
            username: user,
            password: pass,
            server_id: 9_004_031,
            ssl: binlog_protocol::SslMode::Disabled,
            blocking_poll: std::time::Duration::from_millis(200),
            flavor: None,
            mariadb_flags: binlog_protocol::MariaDbDumpFlags {
                send_annotate_rows: true,
            },
            mariadb_gtid_strict_mode: binlog_protocol::MariaDbGtidStrictMode::ServerDefault,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
    };
    crate::shared::start_binlog_at_master_end(&mut client, &conn_str)
        .await?;

    let checkpoint = BinlogCheckpoint {
        flavor: container.flavor(),
        position: client.current_position(),
        timestamp: chrono::Utc::now(),
    };
    drop(client);

    conn.exec_drop(
        "INSERT INTO people (id, name) VALUES (1, 'alice'), (2, 'bob')",
        (),
    )
    .await?;

    let mut pipeline = Pipeline::new();
    let ext = ExternalTransform::child_stdio(
        ChildStdioMode::Persistent,
        vec![
            worker.to_string_lossy().into_owned(),
            "mutate".to_string(),
        ],
    )?;
    pipeline.push_external(ext);
    let apply_opts = ApplyOpts::identity().with_batch_size(1);

    let (sink, db) = mem_surreal_sink().await?;
    let source_opts = SourceOpts {
        connection_string: conn_str.clone(),
        database: Some(db_name.to_string()),
        tables: vec!["people".to_string()],
        server_id: Some(9_004_032),
        flavor: Some(container.flavor()),
        ssl: surreal_sync_mysql_binlog_source::SslMode::Disabled,
        mariadb_gtid_strict_mode:
            surreal_sync_mysql_binlog_source::MariaDbGtidStrictMode::ServerDefault,
    };

    run_replication_tail_with_transforms(
        &sink,
        source_opts,
        checkpoint,
        ReplicationTailOptions::stream(
            Some(chrono::Utc::now() + chrono::Duration::seconds(20)),
            None,
        ),
        None::<&checkpoint::SyncManager<checkpoint::NullStore>>,
        &pipeline,
        &apply_opts,
    )
    .await?;

    #[derive(Debug, serde::Deserialize)]
    struct PeopleRow {
        name: Option<String>,
    }
    let mut resp = db.query("SELECT name FROM people").await?;
    let rows: Vec<PeopleRow> = resp.take(0)?;
    assert_eq!(rows.len(), 2, "expected two people in SurrealDB, got {rows:?}");
    for row in &rows {
        assert_eq!(
            row.name.as_deref(),
            Some("mutated"),
            "SurrealDB sink path must receive external mutate; got {row:?}"
        );
    }

    drop(conn);
    pool.disconnect().await?;
    Ok(())
}
