//! Thin API over [`pg_walstream`] for surreal-sync PostgreSQL pgoutput CDC.

mod client;

pub use client::parse_postgresql_uri;
pub use client::{
    replication_connection_string, CdcChange, ConnectOptions, PgWalClient, RelationMeta, RowChange,
    StreamEvent,
};
pub use pg_walstream::{format_lsn, parse_lsn, SharedLsnFeedback, StreamingMode};

/// Ordered PostgreSQL WAL position (Log Sequence Number).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Lsn {
    segment: u64,
    offset: u64,
}

impl Lsn {
    pub fn new(value: u64) -> Self {
        Self {
            segment: value >> 32,
            offset: value & 0xFFFF_FFFF,
        }
    }

    pub fn from_parts(segment: u64, offset: u64) -> Self {
        Self { segment, offset }
    }

    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let value = parse_lsn(s.strip_prefix("lsn:").unwrap_or(s))?;
        Ok(Self::new(value))
    }

    pub fn value(&self) -> u64 {
        (self.segment << 32) | self.offset
    }

    pub fn to_pg_string(&self) -> String {
        format_lsn(self.value())
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_pg_string())
    }
}

impl From<pg_walstream::Lsn> for Lsn {
    fn from(lsn: pg_walstream::Lsn) -> Self {
        Self::new(lsn.value())
    }
}
