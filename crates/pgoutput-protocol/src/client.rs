//! PostgreSQL logical replication client wrapping [`pg_walstream`].

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use pg_walstream::{
    EventType, LogicalReplicationStream, ReplicationSlotOptions, ReplicationStreamConfig,
    RetryConfig, RowData, SharedLsnFeedback, StreamingMode,
};
use tokio_util::sync::CancellationToken;

use crate::Lsn;

/// Default poll timeout when draining WAL events (mirrors binlog `blocking_poll`).
pub const DEFAULT_WAL_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Connection options for [`PgWalClient`].
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// Replication connection string (`?replication=database` required).
    pub connection_string: String,
    pub slot_name: String,
    pub publication_name: String,
    /// When true, slot creation uses `EXPORT_SNAPSHOT` for consistent initial reads.
    pub export_snapshot: bool,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            slot_name: "surreal_sync_slot".to_string(),
            publication_name: "surreal_sync_pub".to_string(),
            export_snapshot: false,
        }
    }
}

/// Cached relation metadata from pgoutput `Relation` events.
#[derive(Debug, Clone)]
pub struct RelationMeta {
    pub relation_oid: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<pg_walstream::RelationColumn>,
    pub replica_identity: pg_walstream::ReplicaIdentity,
}

/// Row-level change decoded from pgoutput.
#[derive(Debug, Clone)]
pub enum RowChange {
    Insert {
        new: RowData,
    },
    Update {
        old: Option<RowData>,
        new: Box<RowData>,
    },
    Delete {
        old: RowData,
    },
}

/// A single CDC change with stream position and table identity.
#[derive(Debug, Clone)]
pub struct CdcChange {
    pub position: Lsn,
    pub schema: String,
    pub table: String,
    pub relation_oid: u32,
    pub operation: RowChange,
}

/// Raw stream event before relation resolution.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Change(Box<CdcChange>),
    Relation(RelationMeta),
    /// Custom type definition (e.g. after `ALTER TYPE`).
    Type {
        type_oid: u32,
        schema: String,
        name: String,
    },
    Truncate {
        tables: Vec<String>,
    },
    /// Transaction committed — use to defer DDL schema refresh until commit.
    Commit,
    /// Transaction boundaries and other control events (ignored by default).
    Control,
}

/// High-level pgoutput replication client.
pub struct PgWalClient {
    stream: LogicalReplicationStream,
    cancel: CancellationToken,
    /// Live read-head LSN (advanced on every polled WAL event).
    stream_lsn: Lsn,
    /// Highest LSN successfully sunk (advanced only via [`Self::commit`]).
    applied_lsn: Lsn,
    relations: std::collections::HashMap<u32, RelationMeta>,
    feedback: Arc<SharedLsnFeedback>,
}

impl PgWalClient {
    pub async fn connect(opts: ConnectOptions) -> Result<Self> {
        let mut config = ReplicationStreamConfig::new(
            opts.slot_name,
            opts.publication_name,
            2,
            StreamingMode::On,
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(60),
            RetryConfig::default(),
        );
        if opts.export_snapshot {
            config = config.with_slot_options(ReplicationSlotOptions {
                snapshot: Some("export".to_string()),
                ..Default::default()
            });
        }
        let stream = LogicalReplicationStream::new(&opts.connection_string, config)
            .await
            .map_err(|e| anyhow!("pg wal connect failed: {e}"))?;
        let feedback = stream.shared_lsn_feedback.clone();
        Ok(Self {
            stream,
            cancel: CancellationToken::new(),
            stream_lsn: Lsn::new(0),
            applied_lsn: Lsn::new(0),
            relations: std::collections::HashMap::new(),
            feedback,
        })
    }

    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Ensure the replication slot exists. Returns exported snapshot name when created with export.
    pub async fn ensure_replication_slot(&mut self) -> Result<Option<String>> {
        self.stream
            .ensure_replication_slot()
            .await
            .map_err(|e| anyhow!("{e}"))?;
        Ok(self.stream.exported_snapshot_name().map(str::to_string))
    }

    pub fn exported_snapshot_name(&self) -> Option<&str> {
        self.stream.exported_snapshot_name()
    }

    /// Start streaming from `start_lsn` (None = current head).
    pub async fn start(&mut self, start_lsn: Option<Lsn>) -> Result<()> {
        let lsn = start_lsn.map(|l| l.value());
        self.stream
            .start(lsn)
            .await
            .map_err(|e| anyhow!("start replication failed: {e}"))?;
        if let Some(lsn) = start_lsn {
            self.stream_lsn = lsn;
            self.applied_lsn = lsn;
        }
        Ok(())
    }

    /// Live read-head LSN (highest WAL position observed while polling).
    pub fn current_position(&self) -> Lsn {
        self.stream_lsn
    }

    /// Highest LSN successfully sunk (does not rewind the read-head).
    pub fn applied_position(&self) -> Lsn {
        self.applied_lsn
    }

    pub fn commit(&mut self, position: Lsn) {
        // Advance applied watermark only — never clobber stream_lsn used for
        // filtered catch-up via current_position().
        if position > self.applied_lsn {
            self.applied_lsn = position;
        }
        self.feedback.update_applied_lsn(position.value());
        self.feedback.update_flushed_lsn(position.value());
    }

    pub fn shared_lsn_feedback(&self) -> Arc<SharedLsnFeedback> {
        self.feedback.clone()
    }

    pub fn relation_cache(&self) -> &std::collections::HashMap<u32, RelationMeta> {
        &self.relations
    }

    pub fn relation_cache_mut(&mut self) -> &mut std::collections::HashMap<u32, RelationMeta> {
        &mut self.relations
    }

    /// Poll up to `limit` stream events (changes + relation/truncate control).
    ///
    /// Returns as soon as `poll_timeout` elapses without another event, mirroring
    /// binlog `next_events` behaviour (non-blocking tail polling).
    pub async fn next_events(&mut self, limit: usize) -> Result<Vec<StreamEvent>> {
        self.next_events_with_timeout(limit, DEFAULT_WAL_POLL_TIMEOUT)
            .await
    }

    /// Poll up to `limit` events, waiting at most `poll_timeout` per call.
    pub async fn next_events_with_timeout(
        &mut self,
        limit: usize,
        poll_timeout: Duration,
    ) -> Result<Vec<StreamEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(limit.min(32));
        let deadline = tokio::time::Instant::now() + poll_timeout;

        loop {
            if out.len() >= limit {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if out.is_empty() {
                if remaining.is_zero() {
                    break;
                }
            } else if remaining.is_zero() {
                break;
            }

            let wait = if out.is_empty() {
                poll_timeout
            } else {
                remaining
            };
            match tokio::time::timeout(wait, self.next_raw_event()).await {
                Ok(Ok(Some(event))) => out.push(event),
                Ok(Ok(None)) => break,
                Ok(Err(e)) => return Err(e),
                Err(_) => break,
            }
        }

        if !out.is_empty() {
            self.flush_feedback().await?;
        }
        Ok(out)
    }

    /// Send standby status update so PostgreSQL can advance the replication cursor.
    pub async fn flush_feedback(&mut self) -> Result<()> {
        self.stream
            .send_feedback()
            .await
            .map_err(|e| anyhow!("wal feedback failed: {e}"))
    }

    fn upsert_relation_stub(&mut self, relation_oid: u32, schema: &str, table: &str) {
        self.relations
            .entry(relation_oid)
            .or_insert_with(|| RelationMeta {
                relation_oid,
                schema: schema.to_string(),
                table: table.to_string(),
                columns: vec![],
                replica_identity: pg_walstream::ReplicaIdentity::Default,
            });
    }

    /// Poll a single decoded stream event.
    pub async fn next_raw_event(&mut self) -> Result<Option<StreamEvent>> {
        let event = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                return Err(anyhow!("wal stream cancelled"));
            }
            result = self.stream.next_event_with_retry(&self.cancel) => {
                result.map_err(|e| anyhow!("wal read failed: {e}"))?
            }
        };

        let position = Lsn::from(event.lsn);
        self.stream_lsn = position;
        self.feedback.update_flushed_lsn(position.value());
        self.stream
            .send_feedback()
            .await
            .map_err(|e| anyhow!("wal feedback failed: {e}"))?;

        Ok(Some(match event.event_type {
            EventType::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                let meta = RelationMeta {
                    relation_oid: relation_id,
                    schema: namespace.to_string(),
                    table: relation_name.to_string(),
                    columns,
                    replica_identity,
                };
                self.relations.insert(relation_id, meta.clone());
                StreamEvent::Relation(meta)
            }
            EventType::Type {
                type_id,
                namespace,
                type_name,
            } => StreamEvent::Type {
                type_oid: type_id,
                schema: namespace.to_string(),
                name: type_name.to_string(),
            },
            EventType::Insert {
                schema,
                table,
                relation_oid,
                data,
            } => {
                self.upsert_relation_stub(relation_oid, &schema, &table);
                StreamEvent::Change(Box::new(CdcChange {
                    position,
                    schema: schema.to_string(),
                    table: table.to_string(),
                    relation_oid,
                    operation: RowChange::Insert { new: data },
                }))
            }
            EventType::Update {
                schema,
                table,
                relation_oid,
                old_data,
                new_data,
                ..
            } => {
                self.upsert_relation_stub(relation_oid, &schema, &table);
                StreamEvent::Change(Box::new(CdcChange {
                    position,
                    schema: schema.to_string(),
                    table: table.to_string(),
                    relation_oid,
                    operation: RowChange::Update {
                        old: old_data,
                        new: Box::new(new_data),
                    },
                }))
            }
            EventType::Delete {
                schema,
                table,
                relation_oid,
                old_data,
                ..
            } => {
                self.upsert_relation_stub(relation_oid, &schema, &table);
                StreamEvent::Change(Box::new(CdcChange {
                    position,
                    schema: schema.to_string(),
                    table: table.to_string(),
                    relation_oid,
                    operation: RowChange::Delete { old: old_data },
                }))
            }
            EventType::Truncate(tables) => StreamEvent::Truncate {
                tables: tables.iter().map(|t| t.to_string()).collect(),
            },
            EventType::Commit { .. } => StreamEvent::Commit,
            _ => StreamEvent::Control,
        }))
    }

    /// Consume the inner stream (e.g. after wrapping for watermark work).
    pub fn into_inner(self) -> LogicalReplicationStream {
        self.stream
    }

    /// Re-wrap an existing stream while preserving relation cache and position.
    pub fn from_stream(
        stream: LogicalReplicationStream,
        stream_lsn: Lsn,
        relations: std::collections::HashMap<u32, RelationMeta>,
        cancel: CancellationToken,
    ) -> Self {
        let feedback = stream.shared_lsn_feedback.clone();
        Self {
            stream,
            cancel,
            stream_lsn,
            applied_lsn: stream_lsn,
            relations,
            feedback,
        }
    }
}

/// Build a replication connection string from a regular PostgreSQL URI.
pub fn replication_connection_string(base: &str) -> String {
    if base.contains("replication=") {
        return base.to_string();
    }
    if base.starts_with("postgresql://") || base.starts_with("postgres://") {
        if base.contains('?') {
            format!("{base}&replication=database")
        } else {
            format!("{base}?replication=database")
        }
    } else {
        // libpq keyword/value form: append as a separate parameter, not a URL query.
        format!("{base} replication=database")
    }
}

/// Parse host, port, user, password, database from a postgresql:// URI.
pub fn parse_postgresql_uri(uri: &str) -> Result<(String, u16, String, String, String)> {
    let rest = uri
        .strip_prefix("postgresql://")
        .or_else(|| uri.strip_prefix("postgres://"))
        .ok_or_else(|| anyhow!("invalid PostgreSQL connection string"))?;
    let (auth, hostpart) = rest
        .split_once('@')
        .ok_or_else(|| anyhow!("invalid PostgreSQL connection string (missing @)"))?;
    let (username, password) = match auth.split_once(':') {
        Some((u, p)) => (u.to_string(), p.to_string()),
        None => (auth.to_string(), String::new()),
    };
    let hostpart = hostpart.split('?').next().unwrap_or(hostpart);
    let (hostport, dbpart) = match hostpart.split_once('/') {
        Some((hp, db)) => (hp, db.to_string()),
        None => (hostpart, "postgres".to_string()),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(5432)),
        None => (hostport.to_string(), 5432),
    };
    Ok((host, port, username, password, dbpart))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_orders_numerically() {
        let a = Lsn::parse("0/9").unwrap();
        let b = Lsn::parse("0/10").unwrap();
        assert!(a < b);
    }

    #[test]
    fn replication_uri_appends_query_param() {
        let uri = replication_connection_string("postgresql://u:p@localhost:5432/db");
        assert!(uri.contains("replication=database"));
    }

    #[test]
    fn replication_libpq_appends_keyword() {
        let uri = replication_connection_string(
            "host=localhost port=5432 user=postgres password=postgres dbname=mydb",
        );
        assert_eq!(
            uri,
            "host=localhost port=5432 user=postgres password=postgres dbname=mydb replication=database"
        );
    }
}
