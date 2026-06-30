//! E2E tests for the wal2json watermark snapshot+stream backend.
//!
//! These mirror the framework's guarantees against a real PostgreSQL slot and
//! an in-memory SurrealDB sink: parity under concurrent writes, bounded chunk
//! memory, and bounded (continuously freed) WAL retention. They are kept small
//! and deterministic (small chunk sizes over a few hundred rows) per the CI
//! speed constraints.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use surreal2_sink::Surreal2Sink;
use surreal_sink::SurrealSink;
use surreal_sync_postgresql_wal2json_source::{Lsn, SourceOpts, Wal2JsonWatermarkSource};
use surreal_sync_snapshot_stream::{
    run_snapshot_stream, NoopCheckpointer, SnapshotStreamConfig, WatermarkSource,
};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use tokio_postgres::{Client as PgClient, NoTls};

/// Connect to an embedded in-memory SurrealDB and wrap it as a sink.
async fn mem_sink() -> Result<(Surreal2Sink, Surreal<Any>)> {
    let db = surrealdb::engine::any::connect("memory")
        .await
        .context("Failed to connect to in-memory SurrealDB")?;
    db.use_ns("test").use_db("test").await?;
    Ok((Surreal2Sink::new(db.clone()), db))
}

/// Open a fresh PostgreSQL client for the given connection string.
async fn pg_connect(conn: &str) -> Result<PgClient> {
    let (client, connection) = tokio_postgres::connect(conn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });
    Ok(client)
}

/// Create the `users` table and seed it with `n` rows (`val = v{i}`).
async fn seed_users(client: &PgClient, n: i32) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS users; CREATE TABLE users (id SERIAL PRIMARY KEY, val TEXT);")
        .await?;
    for i in 1..=n {
        client
            .execute("INSERT INTO users (val) VALUES ($1)", &[&format!("v{i}")])
            .await?;
    }
    Ok(())
}

/// Read `(id, val)` for every row in PostgreSQL `users`.
async fn pg_users(client: &PgClient) -> Result<BTreeMap<i64, Option<String>>> {
    let rows = client.query("SELECT id, val FROM users", &[]).await?;
    let mut out = BTreeMap::new();
    for row in rows {
        let id: i32 = row.get(0);
        let val: Option<String> = row.get(1);
        out.insert(id as i64, val);
    }
    Ok(out)
}

/// Read `(id, val)` for every row in SurrealDB `users`.
async fn surreal_users(db: &Surreal<Any>) -> Result<BTreeMap<i64, Option<String>>> {
    #[derive(serde::Deserialize)]
    struct Row {
        id: i64,
        val: Option<String>,
    }
    let mut resp = db.query("SELECT meta::id(id) AS id, val FROM users").await?;
    let rows: Vec<Row> = resp.take(0)?;
    Ok(rows.into_iter().map(|r| (r.id, r.val)).collect())
}

/// Drain any change-stream events remaining after the snapshot, applying real
/// data changes to the sink (skipping the signal/watermark rows) until the
/// stream is caught up. This mimics the handoff to the incremental runner so a
/// final parity check is deterministic.
async fn drain_to_catch_up(
    source: &mut Wal2JsonWatermarkSource,
    sink: &Surreal2Sink,
) -> Result<()> {
    let signal_table = source.signal_table().to_string();
    loop {
        let events = source.next_stream_events().await?;
        if events.is_empty() {
            break;
        }
        for event in events {
            // Watermark/signal rows are not real data.
            if event.table == signal_table {
                continue;
            }
            sink.apply_universal_change(&event.change).await?;
        }
        let pos = source.current_position().await?;
        source.commit_consumed(pos).await?;
    }
    Ok(())
}

fn source_opts(conn: &str, slot: &str) -> SourceOpts {
    SourceOpts {
        connection_string: conn.to_string(),
        slot_name: slot.to_string(),
        tables: vec![],
        schema: "public".to_string(),
        relation_tables: vec![],
    }
}

/// Snapshot under concurrent INSERT/UPDATE/DELETE traffic must converge to
/// source parity once the stream is caught up.
#[tokio::test]
async fn snapshot_stream_parity_under_concurrent_writes() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn = crate::shared::create_test_db(container, "wm_parity").await?;

    let setup = pg_connect(&conn).await?;
    seed_users(&setup, 200).await?;

    let (sink, db) = mem_sink().await?;
    let mut source =
        Wal2JsonWatermarkSource::connect(&source_opts(&conn, "wm_parity_slot")).await?;

    // Concurrent writer: update existing rows, insert new ones, delete a few.
    let writer_conn = conn.clone();
    let writer = tokio::spawn(async move {
        let client = pg_connect(&writer_conn).await?;
        for i in 0..40 {
            let target = (i % 200) + 1;
            client
                .execute(
                    "UPDATE users SET val = $1 WHERE id = $2",
                    &[&format!("updated{i}"), &target],
                )
                .await?;
            client
                .execute("INSERT INTO users (val) VALUES ($1)", &[&format!("new{i}")])
                .await?;
            if i % 7 == 0 {
                client
                    .execute("DELETE FROM users WHERE id = $1", &[&(target + 1)])
                    .await?;
            }
            tokio::time::sleep(std::time::Duration::from_millis(8)).await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let config = SnapshotStreamConfig { chunk_size: 8 };
    let mut checkpointer = NoopCheckpointer;
    let result = run_snapshot_stream(&mut source, &sink, &config, &mut checkpointer).await?;

    // Cheap bounded-memory check: the buffer never exceeds one chunk.
    assert!(
        result.peak_buffered_rows <= config.chunk_size,
        "peak buffered rows {} exceeded chunk size {}",
        result.peak_buffered_rows,
        config.chunk_size
    );

    writer.await??;

    // Catch the stream up to the final source state, then compare.
    drain_to_catch_up(&mut source, &sink).await?;

    let expected = pg_users(&setup).await?;
    let actual = surreal_users(&db).await?;
    assert_eq!(
        expected, actual,
        "SurrealDB did not converge to PostgreSQL state"
    );
    assert!(!expected.is_empty());

    Ok(())
}

/// Read the slot's `restart_lsn`, `confirmed_flush_lsn`, and retained WAL bytes.
async fn slot_retention(client: &PgClient, slot: &str) -> Result<(Lsn, Option<Lsn>, i64)> {
    let row = client
        .query_one(
            "SELECT restart_lsn::text,
                    confirmed_flush_lsn::text,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?;
    let restart: String = row.get(0);
    let confirmed: Option<String> = row.get(1);
    let retained: i64 = row.get(2);
    let confirmed = confirmed.map(|s| Lsn::parse(&s)).transpose()?;
    Ok((Lsn::parse(&restart)?, confirmed, retained))
}

/// With a small chunk size spanning many windows, the slot must be advanced
/// during the snapshot (continuous WAL freeing), and retained WAL must be
/// reclaimable after catch-up + CHECKPOINT rather than pinned for the whole
/// snapshot.
#[tokio::test]
async fn snapshot_stream_bounded_wal_retention() -> Result<()> {
    let container = crate::shared::postgres().await;
    let conn = crate::shared::create_test_db(container, "wm_wal").await?;

    let setup = pg_connect(&conn).await?;
    seed_users(&setup, 300).await?;

    let (sink, db) = mem_sink().await?;
    let slot = "wm_wal_slot";
    let mut source = Wal2JsonWatermarkSource::connect(&source_opts(&conn, slot)).await?;

    let (restart_before, confirmed_before, _) = slot_retention(&setup, slot).await?;

    // Light concurrent traffic so the stream (and the slot) keeps moving.
    let writer_conn = conn.clone();
    let writer = tokio::spawn(async move {
        let client = pg_connect(&writer_conn).await?;
        for i in 0..40 {
            let target = (i % 300) + 1;
            client
                .execute(
                    "UPDATE users SET val = $1 WHERE id = $2",
                    &[&format!("u{i}"), &target],
                )
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let config = SnapshotStreamConfig { chunk_size: 8 };
    let mut checkpointer = NoopCheckpointer;
    let result = run_snapshot_stream(&mut source, &sink, &config, &mut checkpointer).await?;
    assert!(result.peak_buffered_rows <= config.chunk_size);

    writer.await??;

    let current_wal: String = setup
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    let total_generated: i64 =
        (Lsn::parse(&current_wal)?.as_u64() - restart_before.as_u64()) as i64;

    // The slot must have been advanced during the snapshot, not pinned: its
    // confirmed-flush position moved past the creation point as the per-window
    // `commit_consumed` calls freed consumed WAL logically.
    let (_, confirmed_after, _) = slot_retention(&setup, slot).await?;
    let confirmed_after = confirmed_after.expect("confirmed_flush_lsn set after consuming windows");
    if let Some(before) = confirmed_before {
        assert!(
            confirmed_after > before,
            "slot confirmed_flush_lsn did not advance during snapshot ({before} -> {confirmed_after})"
        );
    }
    assert!(
        confirmed_after.as_u64() > restart_before.as_u64(),
        "slot confirmed_flush_lsn ({confirmed_after}) did not move past creation point ({restart_before})"
    );

    // The physical `restart_lsn` only advances once the decoder passes a
    // `RUNNING_XACTS` WAL record (emitted at checkpoints / `pg_log_standby_snapshot`)
    // that sits below the confirmed position. Force that record, decode past it,
    // then CHECKPOINT so the now-unneeded WAL is actually reclaimed.
    drain_to_catch_up(&mut source, &sink).await?;
    setup
        .query_one("SELECT pg_log_standby_snapshot()", &[])
        .await?;
    setup
        .execute("INSERT INTO users (val) VALUES ($1)", &[&"reclaim-marker"])
        .await?;
    drain_to_catch_up(&mut source, &sink).await?;
    setup.batch_execute("CHECKPOINT").await?;

    let (restart_final, _, retained_final) = slot_retention(&setup, slot).await?;
    assert!(
        restart_final > restart_before,
        "slot restart_lsn did not advance ({restart_before} -> {restart_final})"
    );
    assert!(
        retained_final < total_generated,
        "retained WAL after catch-up ({retained_final}) not bounded below total ({total_generated})"
    );

    // Correctness is preserved: freeing never dropped an unapplied change.
    let expected = pg_users(&setup).await?;
    let actual = surreal_users(&db).await?;
    assert_eq!(expected, actual, "parity broken under bounded-WAL retention");

    Ok(())
}

/// The recorded peak buffered-row count must equal the chunk size and be
/// independent of table size (two sizes, same small chunk).
#[tokio::test]
async fn snapshot_stream_peak_buffer_independent_of_table_size() -> Result<()> {
    let container = crate::shared::postgres().await;

    async fn run_peak(conn: &str, slot: &str, rows: i32, chunk_size: usize) -> Result<usize> {
        let setup = pg_connect(conn).await?;
        seed_users(&setup, rows).await?;
        let (sink, _db) = mem_sink().await?;
        let mut source =
            Wal2JsonWatermarkSource::connect(&source_opts(conn, slot)).await?;
        let config = SnapshotStreamConfig { chunk_size };
        let mut checkpointer = NoopCheckpointer;
        let result = run_snapshot_stream(&mut source, &sink, &config, &mut checkpointer).await?;
        Ok(result.peak_buffered_rows)
    }

    let conn_small = crate::shared::create_test_db(container, "wm_peak_small").await?;
    let conn_large = crate::shared::create_test_db(container, "wm_peak_large").await?;

    let peak_small = run_peak(&conn_small, "wm_peak_small_slot", 80, 8).await?;
    let peak_large = run_peak(&conn_large, "wm_peak_large_slot", 320, 8).await?;

    assert_eq!(peak_small, 8, "peak should equal full chunk size");
    assert_eq!(peak_large, 8, "peak should equal full chunk size");
    assert_eq!(peak_small, peak_large, "peak must not scale with table size");

    Ok(())
}
