//! WAL checkpoint management.

use crate::pgoutput_protocol::Lsn;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// PostgreSQL WAL CDC checkpoint persisted by surreal-sync.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PgoutputCheckpoint {
    pub lsn: Lsn,
    pub timestamp: DateTime<Utc>,
}

impl surreal_sync_core::Checkpoint for PgoutputCheckpoint {
    const DATABASE_TYPE: &'static str = "postgresql-pgoutput";

    fn to_cli_string(&self) -> String {
        self.lsn.to_string()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        let s = s
            .strip_prefix("postgresql-pgoutput:")
            .or_else(|| s.strip_prefix("lsn:"))
            .unwrap_or(s);
        Ok(Self {
            lsn: Lsn::parse(s)?,
            timestamp: Utc::now(),
        })
    }
}

pub fn get_current_checkpoint(
    client: &crate::pgoutput_protocol::PgWalClient,
) -> Result<PgoutputCheckpoint> {
    Ok(PgoutputCheckpoint {
        lsn: client.current_position(),
        timestamp: Utc::now(),
    })
}

/// Ordered reconciliation position wrapper for interleaved snapshot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PgoutputReconciliationPos {
    pub lsn: Lsn,
}

impl PgoutputReconciliationPos {
    pub fn new(lsn: Lsn) -> Self {
        Self { lsn }
    }
}

impl From<Lsn> for PgoutputReconciliationPos {
    fn from(lsn: Lsn) -> Self {
        Self { lsn }
    }
}

impl From<PgoutputCheckpoint> for PgoutputReconciliationPos {
    fn from(checkpoint: PgoutputCheckpoint) -> Self {
        Self {
            lsn: checkpoint.lsn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surreal_sync_core::{Checkpoint, SyncManager, SyncPhase};

    use surreal_sync_runtime::checkpoint_fs::FilesystemStore;
    use tempfile::TempDir;

    #[test]
    fn lsn_checkpoint_cli_roundtrip() {
        let original = PgoutputCheckpoint {
            lsn: Lsn::parse("0/1949850").unwrap(),
            timestamp: Utc::now(),
        };
        let cli = original.to_cli_string();
        let decoded = PgoutputCheckpoint::from_cli_string(&cli).unwrap();
        assert_eq!(decoded.lsn, original.lsn);
    }

    #[tokio::test]
    async fn checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = FilesystemStore::new(tmp.path());
        let manager = SyncManager::new(store);
        let original = PgoutputCheckpoint {
            lsn: Lsn::parse("0/100").unwrap(),
            timestamp: Utc::now(),
        };
        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncEnd)
            .await
            .unwrap();
        let loaded: PgoutputCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncEnd)
            .await
            .unwrap();
        assert_eq!(loaded.lsn, original.lsn);
    }
}
