//! Interleaved snapshot e2e tests for the pgoutput WAL watermark backend.

use std::collections::BTreeMap;

use anyhow::Result;
use surreal2_sink::Surreal2Sink;
use surreal_sink::SurrealSink;
use surreal_sync_interleaved_snapshot::{
    run_interleaved_snapshot, InterleavedSnapshotConfig, NoopCheckpointer, WatermarkSource,
};
use surreal_sync_postgresql_pgoutput_source::{PgoutputWatermarkSource, SourceOpts, SIGNAL_TABLE};
use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use tokio_postgres::{Client as PgClient, NoTls};

async fn mem_sink() -> Result<(Surreal2Sink, Surreal<Any>)> {
    let db = surrealdb::engine::any::connect("memory").await?;
    db.use_ns("test").use_db("test").await?;
    Ok((Surreal2Sink::new(db.clone()), db))
}

async fn pg_connect(conn: &str) -> Result<PgClient> {
    let (client, connection) = tokio_postgres::connect(conn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });
    Ok(client)
}

async fn seed_users(client: &PgClient, n: i32) -> Result<()> {
    client
        .batch_execute(
            "DROP TABLE IF EXISTS users; CREATE TABLE users (id SERIAL PRIMARY KEY, val TEXT);",
        )
        .await?;
    for i in 1..=n {
        client
            .execute("INSERT INTO users (val) VALUES ($1)", &[&format!("v{i}")])
            .await?;
    }
    Ok(())
}

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

async fn surreal_users(db: &Surreal<Any>) -> Result<BTreeMap<i64, Option<String>>> {
    #[derive(serde::Deserialize)]
    struct Row {
        id: i64,
        val: Option<String>,
    }
    let mut resp = db
        .query("SELECT meta::id(id) AS id, val FROM users")
        .await?;
    let rows: Vec<Row> = resp.take(0)?;
    Ok(rows.into_iter().map(|r| (r.id, r.val)).collect())
}

async fn drain_to_catch_up(
    source: &mut PgoutputWatermarkSource,
    sink: &Surreal2Sink,
) -> Result<()> {
    let signal_table = SIGNAL_TABLE.to_string();
    loop {
        let events = source.next_reconciliation_events().await?;
        if events.is_empty() {
            break;
        }
        for event in events {
            if event.table == signal_table {
                continue;
            }
            sink.apply_universal_change(&event.change).await?;
        }
        let pos = source.current_position().await?;
        source.commit_reconciled(pos).await?;
    }
    Ok(())
}

fn source_opts(conn: &str, slot: &str, publication: &str) -> SourceOpts {
    SourceOpts {
        connection_string: conn.to_string(),
        schema: "public".to_string(),
        tables: vec![],
        slot_name: slot.to_string(),
        publication_name: publication.to_string(),
    }
}

#[tokio::test]
async fn postgresql_pgoutput_interleaved_snapshot_parity_under_concurrent_writes() -> Result<()> {
    crate::shared::init_logging();
    let container = crate::shared::shared_postgresql_pgoutput().await;
    let conn = crate::shared::create_test_db(container, "wm_parity").await?;

    let setup = pg_connect(&conn).await?;
    seed_users(&setup, 200).await?;

    let (sink, db) = mem_sink().await?;
    let mut source =
        PgoutputWatermarkSource::connect(&source_opts(&conn, "wm_parity_slot", "wm_parity_pub"))
            .await?;

    let writer_conn = conn.clone();
    let writer = tokio::spawn(async move {
        let client = pg_connect(&writer_conn).await?;
        for i in 0..40 {
            let target: i32 = (i % 200) + 1;
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

    let config = InterleavedSnapshotConfig { chunk_size: 8 };
    let mut checkpointer = NoopCheckpointer;
    let result = run_interleaved_snapshot(&mut source, &sink, &config, &mut checkpointer).await?;

    assert!(
        result.peak_buffered_rows <= config.chunk_size,
        "peak buffered rows {} exceeded chunk size {}",
        result.peak_buffered_rows,
        config.chunk_size
    );

    writer.await??;
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
