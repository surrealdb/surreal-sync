//! Neo4j checkpoint management
//!
//! This module provides utilities for obtaining and managing Neo4j timestamp-based checkpoints
//! for incremental synchronization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn i64_min() -> i64 {
    i64::MIN
}

/// Neo4j-specific checkpoint containing timestamp and optional keyset tie-breaks.
///
/// `after_node_id` / `after_rel_id` advance within a dense timestamp bucket so
/// crash-resume does not re-process same-timestamp siblings already sunk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Neo4jCheckpoint {
    /// Timestamp for checkpoint
    pub timestamp: DateTime<Utc>,
    /// Last sunk Neo4j node id at `timestamp` (keyset tie-break). Default `i64::MIN`.
    #[serde(default = "i64_min")]
    pub after_node_id: i64,
    /// Last sunk Neo4j relationship id at `timestamp` (keyset tie-break). Default `i64::MIN`.
    #[serde(default = "i64_min")]
    pub after_rel_id: i64,
}

impl Neo4jCheckpoint {
    /// Timestamp-only checkpoint (tie-breaks at the start of the bucket).
    pub fn at(timestamp: DateTime<Utc>) -> Self {
        Self {
            timestamp,
            after_node_id: i64::MIN,
            after_rel_id: i64::MIN,
        }
    }
}

impl checkpoint::Checkpoint for Neo4jCheckpoint {
    const DATABASE_TYPE: &'static str = "neo4j";

    fn to_cli_string(&self) -> String {
        if self.after_node_id == i64::MIN && self.after_rel_id == i64::MIN {
            self.timestamp.to_rfc3339()
        } else {
            format!(
                "{}|n:{}|r:{}",
                self.timestamp.to_rfc3339(),
                self.after_node_id,
                self.after_rel_id
            )
        }
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        // Optional suffix: `<rfc3339>|n:<node_id>|r:<rel_id>`
        let (ts_part, after_node_id, after_rel_id) = if let Some((ts, rest)) = s.split_once("|n:") {
            let (node_s, rel_s) = rest
                .split_once("|r:")
                .ok_or_else(|| anyhow::anyhow!("Invalid Neo4j checkpoint tie-break suffix"))?;
            let node_id: i64 = node_s
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid Neo4j after_node_id: {e}"))?;
            let rel_id: i64 = rel_s
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid Neo4j after_rel_id: {e}"))?;
            (ts, node_id, rel_id)
        } else {
            (s, i64::MIN, i64::MIN)
        };

        let timestamp = DateTime::parse_from_rfc3339(ts_part)
            .map_err(|e| anyhow::anyhow!("Invalid Neo4j checkpoint timestamp format: {e}"))?
            .with_timezone(&Utc);

        Ok(Self {
            timestamp,
            after_node_id,
            after_rel_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, FilesystemStore, SyncManager, SyncPhase};
    use chrono::{Datelike, Timelike};
    use tempfile::TempDir;

    #[test]
    fn test_neo4j_checkpoint_cli_string_roundtrip() {
        let original = Neo4jCheckpoint::at(Utc::now());

        // Encode to CLI string (RFC3339 timestamp)
        let cli_string = original.to_cli_string();

        // Decode from CLI string
        let decoded = Neo4jCheckpoint::from_cli_string(&cli_string).unwrap();

        // Verify roundtrip (compare seconds to avoid nanosecond precision issues)
        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
        assert_eq!(decoded.after_node_id, i64::MIN);
        assert_eq!(decoded.after_rel_id, i64::MIN);
    }

    #[test]
    fn test_neo4j_checkpoint_tie_break_cli_roundtrip() {
        let original = Neo4jCheckpoint {
            timestamp: DateTime::parse_from_rfc3339("2024-06-15T14:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
            after_node_id: 42,
            after_rel_id: 7,
        };
        let cli = original.to_cli_string();
        let decoded = Neo4jCheckpoint::from_cli_string(&cli).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_neo4j_checkpoint_file_roundtrip() {
        let original = Neo4jCheckpoint::at(Utc::now());

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
        let store = FilesystemStore::new(tmp.path());
        let manager = SyncManager::new(store);
        let original = Neo4jCheckpoint {
            timestamp: Utc::now(),
            after_node_id: 99,
            after_rel_id: 3,
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
        assert_eq!(original.after_node_id, loaded.after_node_id);
        assert_eq!(original.after_rel_id, loaded.after_rel_id);
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
    fn test_neo4j_checkpoint_file_type_mismatch() {
        let checkpoint = Neo4jCheckpoint::at(Utc::now());

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

    #[test]
    fn test_legacy_checkpoint_json_without_tie_breaks() {
        let json = r#"{"timestamp":"2024-06-15T14:30:00Z"}"#;
        let cp: Neo4jCheckpoint = serde_json::from_str(json).unwrap();
        assert_eq!(cp.after_node_id, i64::MIN);
        assert_eq!(cp.after_rel_id, i64::MIN);
    }
}
