//! PostgreSQL logical replication checkpoint management
//!
//! This module provides utilities for managing LSN-based checkpoints
//! for WAL-based logical replication synchronization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// PostgreSQL logical replication checkpoint containing LSN and timestamp
///
/// Uses LSN (Log Sequence Number) for positioning in the WAL stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostgreSQLLogicalCheckpoint {
    /// LSN position in the WAL stream (e.g., "0/1949850")
    pub lsn: String,
    /// Timestamp when checkpoint was created
    pub timestamp: DateTime<Utc>,
}

impl checkpoint::Checkpoint for PostgreSQLLogicalCheckpoint {
    const DATABASE_TYPE: &'static str = "postgresql-wal2json";

    fn to_cli_string(&self) -> String {
        // Just the LSN - timestamp is optional metadata
        self.lsn.clone()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        // Validate LSN format (should be something like "0/1949850")
        if !s.contains('/') {
            return Err(anyhow::anyhow!(
                "Invalid PostgreSQL logical checkpoint: expected LSN format like '0/1949850', got '{s}'"
            ));
        }

        Ok(Self {
            lsn: s.to_string(),
            timestamp: Utc::now(),
        })
    }
}

/// Get current LSN from PostgreSQL
///
/// This is a GENERATION operation - it queries the current WAL LSN position
/// and creates a new checkpoint from that position.
pub async fn get_current_checkpoint(
    client: &tokio_postgres::Client,
) -> Result<PostgreSQLLogicalCheckpoint> {
    let query = "SELECT pg_current_wal_lsn()::text";
    let rows = client.query(query, &[]).await?;

    if rows.is_empty() {
        return Err(anyhow::anyhow!(
            "No result returned from pg_current_wal_lsn()"
        ));
    }

    let lsn: String = rows[0].try_get(0)?;

    Ok(PostgreSQLLogicalCheckpoint {
        lsn,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, SyncConfig, SyncManager, SyncPhase};
    use tempfile::TempDir;

    #[test]
    fn test_postgresql_logical_checkpoint_cli_string_roundtrip() {
        let original = PostgreSQLLogicalCheckpoint {
            lsn: "0/1949850".to_string(),
            timestamp: Utc::now(),
        };

        // CLI string format: just the LSN
        let cli_string = original.to_cli_string();
        assert_eq!(cli_string, "0/1949850");

        let decoded = PostgreSQLLogicalCheckpoint::from_cli_string(&cli_string).unwrap();

        assert_eq!(original.lsn, decoded.lsn);
    }

    #[test]
    fn test_postgresql_logical_checkpoint_file_roundtrip() {
        let original = PostgreSQLLogicalCheckpoint {
            lsn: "0/ABCD1234".to_string(),
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&original, SyncPhase::FullSyncStart).unwrap();

        assert_eq!(
            file.database_type(),
            PostgreSQLLogicalCheckpoint::DATABASE_TYPE
        );

        let decoded: PostgreSQLLogicalCheckpoint = file.parse().unwrap();

        assert_eq!(original.lsn, decoded.lsn);
        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
    }

    #[tokio::test]
    async fn test_postgresql_logical_checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let config = SyncConfig {
            emit_checkpoints: true,
            checkpoint_storage: checkpoint::CheckpointStorage::Filesystem {
                dir: tmp.path().to_string_lossy().to_string(),
            },
            incremental: false,
        };

        let manager = SyncManager::new(config, None);
        let original = PostgreSQLLogicalCheckpoint {
            lsn: "0/12345678".to_string(),
            timestamp: Utc::now(),
        };

        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        let loaded: PostgreSQLLogicalCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        assert_eq!(original.lsn, loaded.lsn);
    }

    #[test]
    fn test_postgresql_logical_checkpoint_invalid_lsn() {
        // LSN should contain a '/'
        let result = PostgreSQLLogicalCheckpoint::from_cli_string("not-an-lsn");
        assert!(result.is_err());
    }

    #[test]
    fn test_postgresql_logical_checkpoint_valid_lsn_formats() {
        // Various valid LSN formats
        let valid_lsns = ["0/0", "0/1949850", "0/ABCDEF12", "1234/56789ABC"];

        for lsn in valid_lsns {
            let checkpoint = PostgreSQLLogicalCheckpoint::from_cli_string(lsn).unwrap();
            assert_eq!(checkpoint.lsn, lsn);
        }
    }

    #[test]
    fn test_postgresql_logical_checkpoint_database_type() {
        assert_eq!(
            PostgreSQLLogicalCheckpoint::DATABASE_TYPE,
            "postgresql-wal2json"
        );
    }

    #[test]
    fn test_postgresql_logical_checkpoint_file_type_mismatch() {
        let checkpoint = PostgreSQLLogicalCheckpoint {
            lsn: "0/100".to_string(),
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncStart).unwrap();

        // Serialize to JSON then modify database_type
        let mut json_value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&file).unwrap()).unwrap();
        json_value["database_type"] = serde_json::Value::String("mysql".to_string());
        let modified_json = serde_json::to_string(&json_value).unwrap();
        let modified_file: CheckpointFile = serde_json::from_str(&modified_json).unwrap();

        // Should fail to parse as PostgreSQL logical
        let result: Result<PostgreSQLLogicalCheckpoint> = modified_file.parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }
}
