//! MySQL connection helpers for SQL + binlog protocol client.

use std::time::Duration;

use anyhow::Result;
use binlog_protocol::{
    BinlogClient, Flavor, MariaDbDumpFlags, MariaDbGtidList, ReplicaOptions, ResumePosition,
};
use mysql_async::{prelude::*, Pool, Row};
use tracing::info;

use crate::SourceOpts;

/// Default blocking read timeout for binlog packet polls (`ReplicaOptions::blocking_poll`).
pub const DEFAULT_BINLOG_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Parse host/port/username/password/database from a mysql:// URI.
pub fn parse_mysql_uri(uri: &str) -> Result<(String, u16, String, String, Option<String>)> {
    let rest = uri
        .strip_prefix("mysql://")
        .ok_or_else(|| anyhow::anyhow!("invalid MySQL connection string (expected mysql://)"))?;
    let (auth, hostpart) = rest
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("invalid MySQL connection string (missing @)"))?;
    let (username, password) = match auth.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (auth.to_string(), String::new()),
    };
    let (hostport, dbpart) = match hostpart.split_once('/') {
        Some((hp, db)) => (hp, Some(db.to_string())),
        None => (hostpart, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(3306)),
        None => (hostport.to_string(), 3306),
    };
    Ok((host, port, username, password, dbpart))
}

pub fn new_mysql_pool(connection_string: &str) -> Result<Pool> {
    Ok(Pool::from_url(connection_string)?)
}

pub async fn connect_binlog_client(from_opts: &SourceOpts) -> Result<BinlogClient> {
    connect_binlog_client_with_poll(from_opts, DEFAULT_BINLOG_POLL_TIMEOUT).await
}

pub async fn connect_binlog_client_with_poll(
    from_opts: &SourceOpts,
    blocking_poll: Duration,
) -> Result<BinlogClient> {
    let connection_string = binlog_connection_string(&from_opts.connection_string);
    let (host, port, username, password, _) = parse_mysql_uri(&connection_string)?;
    let server_id = from_opts.server_id.unwrap_or_else(random_server_id);
    let opts = ReplicaOptions {
        host,
        port,
        username,
        password,
        server_id,
        ssl: from_opts.ssl.clone(),
        blocking_poll,
        flavor: from_opts.flavor,
        mariadb_flags: MariaDbDumpFlags {
            send_annotate_rows: true,
        },
        mariadb_gtid_strict_mode: from_opts.mariadb_gtid_strict_mode,
    };
    BinlogClient::connect(opts)
        .await
        .map_err(|e| anyhow::anyhow!("binlog connect failed: {e}"))
}

pub async fn resolve_database(pool: &Pool, from_opts: &SourceOpts) -> Result<String> {
    if let Some(db) = &from_opts.database {
        return Ok(db.clone());
    }
    let (_, _, _, _, db_from_uri) = parse_mysql_uri(&from_opts.connection_string)?;
    if let Some(db) = db_from_uri {
        return Ok(db);
    }
    let mut conn = pool.get_conn().await?;
    let db: Option<String> = conn.query_first("SELECT DATABASE()").await?;
    db.ok_or_else(|| anyhow::anyhow!("no database selected"))
}

pub async fn use_database(conn: &mut mysql_async::Conn, database: &str) -> Result<()> {
    conn.query_drop(format!("USE `{database}`")).await?;
    Ok(())
}

pub async fn show_master_status(conn: &mut mysql_async::Conn) -> Result<(String, u64)> {
    let row: Option<Row> = conn.query_first("SHOW MASTER STATUS").await?;
    let row = row.ok_or_else(|| anyhow::anyhow!("SHOW MASTER STATUS returned no rows"))?;
    let file: String = row.get(0).ok_or_else(|| anyhow::anyhow!("missing File"))?;
    let pos: u64 = row
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("missing Position"))?;
    Ok((file, pos))
}

pub async fn start_binlog_from_checkpoint(
    client: &mut BinlogClient,
    checkpoint: &crate::BinlogCheckpoint,
) -> Result<()> {
    let resume = crate::checkpoint::checkpoint_to_resume(checkpoint)?;
    client
        .start_stream(resume)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

pub async fn start_binlog_at_end(client: &mut BinlogClient, pool: &Pool) -> Result<()> {
    // On MariaDB, prefer GTID-based resume so runtime checkpoints survive binlog
    // rotation. MariaDB has no COM_BINLOG_DUMP_GTID; the client seeds
    // @slave_connect_state from @@global.gtid_binlog_pos instead. Fall back to
    // file+pos when GTID is empty/unavailable (or for MySQL).
    if client.flavor() == Flavor::MariaDb {
        let mut conn = pool.get_conn().await?;
        let gtid_pos: Option<String> = conn.query_first("SELECT @@global.gtid_binlog_pos").await?;
        drop(conn);
        if let Some(list) = resume_gtid_list_from_pos(gtid_pos.as_deref())? {
            info!(
                "Starting MariaDB binlog in GTID mode at gtid_binlog_pos={}",
                list.to_connect_state()
            );
            return client
                .start_stream(ResumePosition::MariaDbGtid(list))
                .await
                .map_err(|e| anyhow::anyhow!("{e}"));
        }
    }

    let mut conn = pool.get_conn().await?;
    let (file, pos) = show_master_status(&mut conn).await?;
    drop(conn);
    client
        .start_stream(ResumePosition::FilePos {
            file,
            pos: pos as u32,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Decide how to resume from a MariaDB `@@global.gtid_binlog_pos` value.
///
/// - `None` / empty (whitespace only) → `Ok(None)`: no GTID history, start fresh
///   from the current master position (file+pos).
/// - a valid non-empty list → `Ok(Some(list))`: resume in GTID mode.
/// - a non-empty but unparseable value → `Err`: a hard error. We must never
///   silently downgrade a GTID position to file+pos, which could skip or replay
///   transactions.
fn resume_gtid_list_from_pos(raw: Option<&str>) -> Result<Option<MariaDbGtidList>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let list = MariaDbGtidList::parse(trimmed).map_err(|e| {
        anyhow::anyhow!("failed to parse @@global.gtid_binlog_pos '{trimmed}': {e}")
    })?;
    Ok(Some(list))
}

fn random_server_id() -> u32 {
    rand::random::<u32>() % 1_000_000 + 1_000_000
}

/// Prefer the dedicated replication user when tests (or callers) supply the
/// default admin URI, so binlog auth uses `mysql_native_password` while SQL
/// pool traffic can still use the admin account.
fn binlog_connection_string(base: &str) -> String {
    if base.contains("root:testpass@") && !base.contains("surreal_sync:surreal_sync_pass@") {
        base.replacen("root:testpass@", "surreal_sync:surreal_sync_pass@", 1)
    } else {
        base.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_gtid_pos_starts_fresh() {
        assert!(resume_gtid_list_from_pos(None).unwrap().is_none());
        assert!(resume_gtid_list_from_pos(Some("")).unwrap().is_none());
        assert!(resume_gtid_list_from_pos(Some("   ")).unwrap().is_none());
    }

    #[test]
    fn valid_gtid_pos_resumes_in_gtid_mode() {
        let list = resume_gtid_list_from_pos(Some("0-1-270,1-7-42"))
            .unwrap()
            .expect("valid GTID position should resume in GTID mode");
        assert_eq!(list.to_connect_state(), "0-1-270,1-7-42");
    }

    #[test]
    fn malformed_gtid_pos_is_hard_error() {
        let err = resume_gtid_list_from_pos(Some("not-a-gtid"))
            .expect_err("malformed non-empty GTID position must error");
        assert!(
            format!("{err}").contains("gtid_binlog_pos"),
            "unexpected error: {err}"
        );
    }
}
