//! PostgreSQL checkpoint management
//!
//! This module provides utilities for obtaining and managing PostgreSQL sequence-based checkpoints
//! for trigger-based incremental synchronization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

/// PostgreSQL-specific checkpoint containing sequence_id and timestamp
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostgreSQLCheckpoint {
    /// Sequence ID from the audit table
    pub sequence_id: i64,
    /// Timestamp when checkpoint was created
    pub timestamp: DateTime<Utc>,
}

impl checkpoint::Checkpoint for PostgreSQLCheckpoint {
    const DATABASE_TYPE: &'static str = "postgresql";

    fn to_cli_string(&self) -> String {
        // Just the sequence_id - timestamp is optional metadata
        self.sequence_id.to_string()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        let sequence_id = s.parse::<i64>().map_err(|e| {
            anyhow::anyhow!("Invalid PostgreSQL checkpoint: expected number, got '{s}': {e}")
        })?;

        Ok(Self {
            sequence_id,
            timestamp: Utc::now(),
        })
    }
}

/// Get current checkpoint from PostgreSQL audit table
///
/// This is a GENERATION operation - it queries the audit table for the current
/// maximum sequence_id and creates a new checkpoint from that position.
pub async fn get_current_checkpoint(client: Arc<Mutex<Client>>) -> Result<PostgreSQLCheckpoint> {
    let client = client.lock().await;

    // Check if audit table exists first
    let table_exists: Vec<tokio_postgres::Row> = client
        .query(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'surreal_sync_changes' AND table_schema = 'public'",
            &[]
        )
        .await?;

    if table_exists.is_empty() {
        // Audit table doesn't exist yet, return 0
        return Ok(PostgreSQLCheckpoint {
            sequence_id: 0,
            timestamp: Utc::now(),
        });
    }

    let rows = client
        .query(
            "SELECT COALESCE(MAX(sequence_id), 0) FROM surreal_sync_changes",
            &[],
        )
        .await?;

    let sequence_id: i64 = if rows.is_empty() { 0 } else { rows[0].get(0) };

    Ok(PostgreSQLCheckpoint {
        sequence_id,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, FilesystemStore, SyncManager, SyncPhase};
    use tempfile::TempDir;

    #[test]
    fn test_postgresql_checkpoint_cli_string_roundtrip() {
        let original = PostgreSQLCheckpoint {
            sequence_id: 12345,
            timestamp: Utc::now(),
        };

        // CLI string format: just the sequence_id
        let cli_string = original.to_cli_string();
        assert_eq!(cli_string, "12345");

        let decoded = PostgreSQLCheckpoint::from_cli_string(&cli_string).unwrap();

        assert_eq!(original.sequence_id, decoded.sequence_id);
    }

    #[test]
    fn test_postgresql_checkpoint_file_roundtrip() {
        let original = PostgreSQLCheckpoint {
            sequence_id: 999,
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&original, SyncPhase::FullSyncStart).unwrap();

        assert_eq!(file.database_type(), PostgreSQLCheckpoint::DATABASE_TYPE);

        let decoded: PostgreSQLCheckpoint = file.parse().unwrap();

        assert_eq!(original.sequence_id, decoded.sequence_id);
        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
    }

    #[tokio::test]
    async fn test_postgresql_checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = FilesystemStore::new(tmp.path());
        let manager = SyncManager::new(store);
        let original = PostgreSQLCheckpoint {
            sequence_id: 42,
            timestamp: Utc::now(),
        };

        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        let loaded: PostgreSQLCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        assert_eq!(original.sequence_id, loaded.sequence_id);
    }

    #[test]
    fn test_postgresql_checkpoint_invalid_sequence_id() {
        let result = PostgreSQLCheckpoint::from_cli_string("not-a-number");
        assert!(result.is_err());
    }

    #[test]
    fn test_postgresql_checkpoint_negative_sequence_id() {
        let result = PostgreSQLCheckpoint::from_cli_string("-1");
        // Negative sequence IDs should be allowed
        assert!(result.is_ok());
        assert_eq!(result.unwrap().sequence_id, -1);
    }

    #[test]
    fn test_postgresql_checkpoint_zero_sequence_id() {
        let checkpoint = PostgreSQLCheckpoint::from_cli_string("0").unwrap();
        assert_eq!(checkpoint.sequence_id, 0);
    }

    #[test]
    fn test_postgresql_checkpoint_large_sequence_id() {
        let large_id = i64::MAX;
        let checkpoint = PostgreSQLCheckpoint {
            sequence_id: large_id,
            timestamp: Utc::now(),
        };

        let cli_string = checkpoint.to_cli_string();
        let decoded = PostgreSQLCheckpoint::from_cli_string(&cli_string).unwrap();

        assert_eq!(large_id, decoded.sequence_id);
    }

    #[test]
    fn test_postgresql_checkpoint_database_type() {
        assert_eq!(PostgreSQLCheckpoint::DATABASE_TYPE, "postgresql");
    }

    #[test]
    fn test_postgresql_checkpoint_file_type_mismatch() {
        let checkpoint = PostgreSQLCheckpoint {
            sequence_id: 100,
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncStart).unwrap();

        // Serialize to JSON then modify database_type
        let mut json_value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&file).unwrap()).unwrap();
        json_value["database_type"] = serde_json::Value::String("mysql".to_string());
        let modified_json = serde_json::to_string(&json_value).unwrap();
        let modified_file: CheckpointFile = serde_json::from_str(&modified_json).unwrap();

        // Should fail to parse as PostgreSQL
        let result: Result<PostgreSQLCheckpoint> = modified_file.parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }
}
