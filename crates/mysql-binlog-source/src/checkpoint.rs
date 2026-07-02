//! Binlog checkpoint management.

use anyhow::{anyhow, Result};
use binlog_protocol::{BinlogPosition, Flavor, MariaDbGtidList, MySqlGtidSet};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Binlog CDC checkpoint persisted by surreal-sync.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BinlogCheckpoint {
    pub flavor: Flavor,
    pub position: BinlogPosition,
    pub timestamp: DateTime<Utc>,
}

impl checkpoint::Checkpoint for BinlogCheckpoint {
    const DATABASE_TYPE: &'static str = "mysql-binlog";

    fn to_cli_string(&self) -> String {
        match &self.position {
            BinlogPosition::FilePos { file, pos } => format!("file:{file}:{pos}"),
            BinlogPosition::MySqlGtid { executed } => format!("gtid:{executed}"),
            BinlogPosition::MariaDbGtid { executed } => format!("gtid:{executed}"),
        }
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        let s = s.strip_prefix("mysql-binlog:").unwrap_or(s);
        let rest = s
            .strip_prefix("file:")
            .map(|body| (Flavor::MySql, body, "file"))
            .or_else(|| {
                s.strip_prefix("gtid:")
                    .map(|body| (Flavor::MySql, body, "gtid"))
            })
            .ok_or_else(|| anyhow!("invalid mysql-binlog checkpoint '{s}'"))?;

        let (default_flavor, body, kind) = rest;
        match kind {
            "file" => {
                let (file, pos) = body
                    .rsplit_once(':')
                    .ok_or_else(|| anyhow!("invalid file checkpoint '{s}'"))?;
                let pos = pos.parse::<u64>()?;
                Ok(Self {
                    flavor: default_flavor,
                    position: BinlogPosition::FilePos {
                        file: file.to_string(),
                        pos,
                    },
                    timestamp: Utc::now(),
                })
            }
            "gtid" => {
                if body.contains('-') && !body.contains(':') && body.split('-').count() == 3 {
                    let executed = MariaDbGtidList::parse(body)
                        .map_err(|e| anyhow!("invalid MariaDB GTID checkpoint: {e}"))?;
                    Ok(Self {
                        flavor: Flavor::MariaDb,
                        position: BinlogPosition::MariaDbGtid { executed },
                        timestamp: Utc::now(),
                    })
                } else {
                    let executed = MySqlGtidSet::parse(body)
                        .map_err(|e| anyhow!("invalid MySQL GTID checkpoint: {e}"))?;
                    Ok(Self {
                        flavor: Flavor::MySql,
                        position: BinlogPosition::MySqlGtid { executed },
                        timestamp: Utc::now(),
                    })
                }
            }
            _ => Err(anyhow!("invalid mysql-binlog checkpoint '{s}'")),
        }
    }
}

/// Capture the current binlog client position as a checkpoint.
pub fn get_current_checkpoint(client: &binlog_protocol::BinlogClient) -> Result<BinlogCheckpoint> {
    Ok(BinlogCheckpoint {
        flavor: client.flavor(),
        position: client.current_position(),
        timestamp: Utc::now(),
    })
}

/// Ordered stream position wrapper for interleaved snapshot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BinlogStreamPosition {
    pub position: BinlogPosition,
}

impl BinlogStreamPosition {
    pub fn new(position: BinlogPosition) -> Self {
        Self { position }
    }
}

impl From<BinlogPosition> for BinlogStreamPosition {
    fn from(position: BinlogPosition) -> Self {
        Self { position }
    }
}

impl From<BinlogCheckpoint> for BinlogStreamPosition {
    fn from(checkpoint: BinlogCheckpoint) -> Self {
        Self {
            position: checkpoint.position,
        }
    }
}

pub fn checkpoint_to_resume(
    checkpoint: &BinlogCheckpoint,
) -> Result<binlog_protocol::ResumePosition> {
    use binlog_protocol::ResumePosition;
    Ok(match &checkpoint.position {
        BinlogPosition::FilePos { file, pos } => ResumePosition::FilePos {
            file: file.clone(),
            pos: *pos as u32,
        },
        BinlogPosition::MySqlGtid { executed } => ResumePosition::MySqlGtid(executed.clone()),
        BinlogPosition::MariaDbGtid { executed } => ResumePosition::MariaDbGtid(executed.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, FilesystemStore, SyncManager, SyncPhase};
    use tempfile::TempDir;

    #[test]
    fn file_checkpoint_cli_roundtrip() {
        let original = BinlogCheckpoint {
            flavor: Flavor::MySql,
            position: BinlogPosition::file_pos("mysql-bin.000003", 195),
            timestamp: Utc::now(),
        };
        let cli = original.to_cli_string();
        assert_eq!(cli, "file:mysql-bin.000003:195");
        let decoded = BinlogCheckpoint::from_cli_string(&cli).unwrap();
        assert_eq!(decoded.position, original.position);
    }

    #[test]
    fn mysql_gtid_checkpoint_cli_roundtrip() {
        let original = BinlogCheckpoint {
            flavor: Flavor::MySql,
            position: BinlogPosition::MySqlGtid {
                executed: MySqlGtidSet::parse("d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107")
                    .unwrap(),
            },
            timestamp: Utc::now(),
        };
        let cli = original.to_cli_string();
        assert!(cli.starts_with("gtid:"));
        let decoded = BinlogCheckpoint::from_cli_string(&cli).unwrap();
        assert_eq!(decoded.flavor, Flavor::MySql);
        assert_eq!(decoded.position, original.position);
    }

    #[test]
    fn mariadb_gtid_checkpoint_cli_roundtrip() {
        let original = BinlogCheckpoint {
            flavor: Flavor::MariaDb,
            position: BinlogPosition::MariaDbGtid {
                executed: MariaDbGtidList::parse("0-1-270").unwrap(),
            },
            timestamp: Utc::now(),
        };
        let cli = original.to_cli_string();
        assert_eq!(cli, "gtid:0-1-270");
        let decoded = BinlogCheckpoint::from_cli_string(&cli).unwrap();
        assert_eq!(decoded.flavor, Flavor::MariaDb);
        assert_eq!(decoded.position, original.position);
    }

    #[test]
    fn checkpoint_file_roundtrip() {
        let original = BinlogCheckpoint {
            flavor: Flavor::MySql,
            position: BinlogPosition::file_pos("mysql-bin.000001", 4),
            timestamp: Utc::now(),
        };
        let file = CheckpointFile::new(&original, SyncPhase::FullSyncStart).unwrap();
        assert_eq!(file.database_type(), BinlogCheckpoint::DATABASE_TYPE);
        let decoded: BinlogCheckpoint = file.parse().unwrap();
        assert_eq!(decoded.position, original.position);
    }

    #[tokio::test]
    async fn checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = FilesystemStore::new(tmp.path());
        let manager = SyncManager::new(store);
        let original = BinlogCheckpoint {
            flavor: Flavor::MySql,
            position: BinlogPosition::file_pos("mysql-bin.000002", 100),
            timestamp: Utc::now(),
        };
        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncEnd)
            .await
            .unwrap();
        let loaded: BinlogCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncEnd)
            .await
            .unwrap();
        assert_eq!(loaded.position, original.position);
    }

    #[test]
    fn file_checkpoint_accepts_mysql_binlog_prefix() {
        let decoded =
            BinlogCheckpoint::from_cli_string("mysql-binlog:file:mysql-bin.000003:195").unwrap();
        assert_eq!(
            decoded.position,
            BinlogPosition::file_pos("mysql-bin.000003", 195)
        );
    }

    #[test]
    fn gtid_checkpoint_accepts_mysql_binlog_prefix() {
        let decoded = BinlogCheckpoint::from_cli_string(
            "mysql-binlog:gtid:d4c17f0c-8c11-11e1-9ed1-0800270a0001:1-107",
        )
        .unwrap();
        assert_eq!(decoded.flavor, Flavor::MySql);
        assert!(matches!(decoded.position, BinlogPosition::MySqlGtid { .. }));
    }

    #[test]
    fn invalid_checkpoint_is_rejected() {
        assert!(BinlogCheckpoint::from_cli_string("not-a-checkpoint").is_err());
    }

    #[test]
    fn stream_position_orders_file_pos() {
        let a = BinlogStreamPosition::new(BinlogPosition::file_pos("mysql-bin.000001", 10));
        let b = BinlogStreamPosition::new(BinlogPosition::file_pos("mysql-bin.000001", 20));
        assert!(a < b);
    }
}
