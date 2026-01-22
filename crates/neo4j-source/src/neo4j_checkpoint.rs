//! Neo4j checkpoint management
//!
//! This module provides utilities for obtaining and managing Neo4j timestamp-based checkpoints
//! for incremental synchronization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Neo4j-specific checkpoint containing timestamp
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Neo4jCheckpoint {
    /// Timestamp for checkpoint
    pub timestamp: DateTime<Utc>,
}

impl checkpoint::Checkpoint for Neo4jCheckpoint {
    const DATABASE_TYPE: &'static str = "neo4j";

    fn to_cli_string(&self) -> String {
        self.timestamp.to_rfc3339()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        let timestamp = DateTime::parse_from_rfc3339(s)
            .map_err(|e| anyhow::anyhow!("Invalid Neo4j checkpoint timestamp format: {e}"))?
            .with_timezone(&Utc);

        Ok(Self { timestamp })
    }
}

/// Get current Neo4j checkpoint with timestamp
///
/// This is a GENERATION operation - it creates a new checkpoint from the current time.
pub fn get_current_checkpoint() -> Neo4jCheckpoint {
    Neo4jCheckpoint {
        timestamp: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, SyncConfig, SyncManager, SyncPhase};
    use chrono::{Datelike, Timelike};
    use tempfile::TempDir;

    #[test]
    fn test_neo4j_checkpoint_cli_string_roundtrip() {
        let original = Neo4jCheckpoint {
            timestamp: Utc::now(),
        };

        // Encode to CLI string (RFC3339 timestamp)
        let cli_string = original.to_cli_string();

        // Decode from CLI string
        let decoded = Neo4jCheckpoint::from_cli_string(&cli_string).unwrap();

        // Verify roundtrip (compare seconds to avoid nanosecond precision issues)
        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
    }

    #[test]
    fn test_neo4j_checkpoint_file_roundtrip() {
        let original = Neo4jCheckpoint {
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&original, SyncPhase::FullSyncEnd).unwrap();

        assert_eq!(file.database_type(), Neo4jCheckpoint::DATABASE_TYPE);
        assert_eq!(file.phase(), &SyncPhase::FullSyncEnd);

        let decoded: Neo4jCheckpoint = file.parse().unwrap();

        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
    }

    #[tokio::test]
    async fn test_neo4j_checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let config = SyncConfig {
            emit_checkpoints: true,
            checkpoint_dir: Some(tmp.path().to_string_lossy().to_string()),
            incremental: false,
        };

        let manager = SyncManager::new(config, None);
        let original = Neo4jCheckpoint {
            timestamp: Utc::now(),
        };

        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncStart)
            .await
            .unwrap();

        let loaded: Neo4jCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncStart)
            .await
            .unwrap();

        assert_eq!(original.timestamp.timestamp(), loaded.timestamp.timestamp());
    }

    #[test]
    fn test_neo4j_checkpoint_invalid_timestamp() {
        let result = Neo4jCheckpoint::from_cli_string("not-a-timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_neo4j_checkpoint_specific_timestamp() {
        // Test with a specific known timestamp
        let cli_string = "2024-06-15T14:30:00Z";
        let checkpoint = Neo4jCheckpoint::from_cli_string(cli_string).unwrap();

        assert_eq!(checkpoint.timestamp.year(), 2024);
        assert_eq!(checkpoint.timestamp.month(), 6);
        assert_eq!(checkpoint.timestamp.day(), 15);
        assert_eq!(checkpoint.timestamp.hour(), 14);
        assert_eq!(checkpoint.timestamp.minute(), 30);
    }

    #[test]
    fn test_neo4j_checkpoint_database_type() {
        assert_eq!(Neo4jCheckpoint::DATABASE_TYPE, "neo4j");
    }

    #[test]
    fn test_get_current_checkpoint() {
        let before = Utc::now();
        let checkpoint = get_current_checkpoint();
        let after = Utc::now();

        // Checkpoint timestamp should be between before and after
        assert!(checkpoint.timestamp >= before);
        assert!(checkpoint.timestamp <= after);
    }

    #[test]
    fn test_neo4j_checkpoint_file_type_mismatch() {
        let checkpoint = Neo4jCheckpoint {
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncStart).unwrap();

        // Serialize to JSON then modify database_type
        let mut json_value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&file).unwrap()).unwrap();
        json_value["database_type"] = serde_json::Value::String("mongodb".to_string());
        let modified_json = serde_json::to_string(&json_value).unwrap();
        let modified_file: CheckpointFile = serde_json::from_str(&modified_json).unwrap();

        // Should fail to parse as Neo4j
        let result: Result<Neo4jCheckpoint> = modified_file.parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }
}
