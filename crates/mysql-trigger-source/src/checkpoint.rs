//! MySQL checkpoint management
//!
//! This module provides utilities for obtaining and managing MySQL sequence-based checkpoints
//! for trigger-based incremental synchronization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};

/// MySQL-specific checkpoint containing sequence_id and timestamp
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MySQLCheckpoint {
    /// Sequence ID from the audit table
    pub sequence_id: i64,
    /// Timestamp when checkpoint was created
    pub timestamp: DateTime<Utc>,
}

impl checkpoint::Checkpoint for MySQLCheckpoint {
    const DATABASE_TYPE: &'static str = "mysql";

    fn to_cli_string(&self) -> String {
        // Just the sequence_id - timestamp is optional metadata
        self.sequence_id.to_string()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        let sequence_id = s.parse::<i64>().map_err(|e| {
            anyhow::anyhow!("Invalid MySQL checkpoint: expected number, got '{s}': {e}")
        })?;

        Ok(Self {
            sequence_id,
            timestamp: Utc::now(),
        })
    }
}

/// Get current checkpoint from MySQL audit table
///
/// This is a GENERATION operation - it queries the audit table for the current
/// maximum sequence_id and creates a new checkpoint from that position.
pub async fn get_current_checkpoint(conn: &mut mysql_async::Conn) -> Result<MySQLCheckpoint> {
    // Check if audit table exists first
    let table_check: Vec<mysql_async::Row> = conn
        .query("SELECT 1 FROM information_schema.tables WHERE table_name = 'surreal_sync_changes' AND table_schema = DATABASE()")
        .await?;

    if table_check.is_empty() {
        // Audit table doesn't exist yet, return 0
        return Ok(MySQLCheckpoint {
            sequence_id: 0,
            timestamp: Utc::now(),
        });
    }

    // Get current max sequence_id from the audit table
    let result: Vec<mysql_async::Row> = conn
        .query("SELECT COALESCE(MAX(sequence_id), 0) FROM surreal_sync_changes")
        .await?;

    let current_sequence = result
        .first()
        .and_then(|row| row.get::<i64, _>(0))
        .unwrap_or(0);

    Ok(MySQLCheckpoint {
        sequence_id: current_sequence,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, SyncConfig, SyncManager, SyncPhase};
    use tempfile::TempDir;

    #[test]
    fn test_mysql_checkpoint_cli_string_roundtrip() {
        let original = MySQLCheckpoint {
            sequence_id: 12345,
            timestamp: Utc::now(),
        };

        // CLI string format: just the sequence_id
        let cli_string = original.to_cli_string();
        assert_eq!(cli_string, "12345");

        let decoded = MySQLCheckpoint::from_cli_string(&cli_string).unwrap();

        assert_eq!(original.sequence_id, decoded.sequence_id);
    }

    #[test]
    fn test_mysql_checkpoint_file_roundtrip() {
        let original = MySQLCheckpoint {
            sequence_id: 999,
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&original, SyncPhase::FullSyncStart).unwrap();

        assert_eq!(file.database_type(), MySQLCheckpoint::DATABASE_TYPE);

        let decoded: MySQLCheckpoint = file.parse().unwrap();

        assert_eq!(original.sequence_id, decoded.sequence_id);
        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
    }

    #[tokio::test]
    async fn test_mysql_checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let config = SyncConfig {
            emit_checkpoints: true,
            checkpoint_storage: checkpoint::CheckpointStorage::Filesystem {
                dir: tmp.path().to_string_lossy().to_string(),
            },
            incremental: false,
        };

        let manager = SyncManager::new(config, None);
        let original = MySQLCheckpoint {
            sequence_id: 42,
            timestamp: Utc::now(),
        };

        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        let loaded: MySQLCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        assert_eq!(original.sequence_id, loaded.sequence_id);
    }

    #[test]
    fn test_mysql_checkpoint_invalid_sequence_id() {
        let result = MySQLCheckpoint::from_cli_string("not-a-number");
        assert!(result.is_err());
    }

    #[test]
    fn test_mysql_checkpoint_negative_sequence_id() {
        let result = MySQLCheckpoint::from_cli_string("-1");
        // Negative sequence IDs should be allowed
        assert!(result.is_ok());
        assert_eq!(result.unwrap().sequence_id, -1);
    }

    #[test]
    fn test_mysql_checkpoint_zero_sequence_id() {
        let checkpoint = MySQLCheckpoint::from_cli_string("0").unwrap();
        assert_eq!(checkpoint.sequence_id, 0);
    }

    #[test]
    fn test_mysql_checkpoint_large_sequence_id() {
        let large_id = i64::MAX;
        let checkpoint = MySQLCheckpoint {
            sequence_id: large_id,
            timestamp: Utc::now(),
        };

        let cli_string = checkpoint.to_cli_string();
        let decoded = MySQLCheckpoint::from_cli_string(&cli_string).unwrap();

        assert_eq!(large_id, decoded.sequence_id);
    }

    #[test]
    fn test_mysql_checkpoint_database_type() {
        assert_eq!(MySQLCheckpoint::DATABASE_TYPE, "mysql");
    }

    #[test]
    fn test_mysql_checkpoint_file_type_mismatch() {
        let checkpoint = MySQLCheckpoint {
            sequence_id: 100,
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncStart).unwrap();

        // Serialize to JSON then modify database_type
        let mut json_value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&file).unwrap()).unwrap();
        json_value["database_type"] = serde_json::Value::String("postgresql".to_string());
        let modified_json = serde_json::to_string(&json_value).unwrap();
        let modified_file: CheckpointFile = serde_json::from_str(&modified_json).unwrap();

        // Should fail to parse as MySQL
        let result: Result<MySQLCheckpoint> = modified_file.parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }
}
