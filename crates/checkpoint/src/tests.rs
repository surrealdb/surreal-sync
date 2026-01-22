//! Unit tests for the checkpoint crate.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::{Checkpoint, CheckpointFile, SyncConfig, SyncManager, SyncPhase};

/// Test checkpoint type for unit tests.
///
/// This is a simple checkpoint type that exercises the trait implementation
/// without needing actual database connections.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TestCheckpoint {
    value: i64,
    timestamp: DateTime<Utc>,
}

impl Checkpoint for TestCheckpoint {
    const DATABASE_TYPE: &'static str = "test";

    fn to_cli_string(&self) -> String {
        format!("{}:{}", self.value, self.timestamp.to_rfc3339())
    }

    fn from_cli_string(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid test checkpoint format: expected 'value:timestamp'");
        }
        Ok(Self {
            value: parts[0].parse()?,
            timestamp: DateTime::parse_from_rfc3339(parts[1])?.with_timezone(&Utc),
        })
    }
}

// ============================================================================
// CheckpointFile Tests
// ============================================================================

#[test]
fn test_checkpoint_file_serialization() {
    let cp = TestCheckpoint {
        value: 42,
        timestamp: Utc::now(),
    };
    let file = CheckpointFile::new(&cp, SyncPhase::FullSyncStart).unwrap();

    assert_eq!(file.database_type, "test");
    assert_eq!(file.phase, SyncPhase::FullSyncStart);

    let parsed: TestCheckpoint = file.parse().unwrap();
    assert_eq!(parsed.value, cp.value);
}

#[test]
fn test_checkpoint_file_roundtrip() {
    let original = TestCheckpoint {
        value: 12345,
        timestamp: Utc::now(),
    };

    // Create file
    let file = CheckpointFile::new(&original, SyncPhase::FullSyncEnd).unwrap();

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&file).unwrap();

    // Deserialize back
    let loaded: CheckpointFile = serde_json::from_str(&json).unwrap();

    // Parse checkpoint
    let parsed: TestCheckpoint = loaded.parse().unwrap();

    assert_eq!(original.value, parsed.value);
    assert_eq!(original.timestamp.timestamp(), parsed.timestamp.timestamp());
}

#[test]
fn test_checkpoint_type_mismatch() {
    let cp = TestCheckpoint {
        value: 42,
        timestamp: Utc::now(),
    };
    let mut file = CheckpointFile::new(&cp, SyncPhase::FullSyncStart).unwrap();
    file.database_type = "wrong".to_string();

    let result: anyhow::Result<TestCheckpoint> = file.parse();
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("type mismatch"));
    assert!(err_msg.contains("expected 'test'"));
    assert!(err_msg.contains("found 'wrong'"));
}

#[test]
fn test_checkpoint_file_accessors() {
    let cp = TestCheckpoint {
        value: 99,
        timestamp: Utc::now(),
    };
    let file = CheckpointFile::new(&cp, SyncPhase::FullSyncEnd).unwrap();

    assert_eq!(file.database_type(), "test");
    assert_eq!(file.phase(), &SyncPhase::FullSyncEnd);
    // created_at should be close to now
    let diff = Utc::now() - file.created_at();
    assert!(diff.num_seconds() < 5);
}

// ============================================================================
// SyncPhase Tests
// ============================================================================

#[test]
fn test_sync_phase_as_str() {
    assert_eq!(SyncPhase::FullSyncStart.as_str(), "full_sync_start");
    assert_eq!(SyncPhase::FullSyncEnd.as_str(), "full_sync_end");
}

#[test]
fn test_sync_phase_display() {
    assert_eq!(format!("{}", SyncPhase::FullSyncStart), "full_sync_start");
    assert_eq!(format!("{}", SyncPhase::FullSyncEnd), "full_sync_end");
}

#[test]
fn test_sync_phase_serialization() {
    let phase = SyncPhase::FullSyncStart;
    let json = serde_json::to_string(&phase).unwrap();
    let parsed: SyncPhase = serde_json::from_str(&json).unwrap();
    assert_eq!(phase, parsed);
}

// ============================================================================
// SyncConfig Tests
// ============================================================================

#[test]
fn test_sync_config_default() {
    let config = SyncConfig::default();
    assert!(!config.incremental);
    assert!(config.emit_checkpoints);
    assert!(matches!(
        config.checkpoint_storage,
        crate::CheckpointStorage::Filesystem { ref dir } if dir == ".surreal-sync-checkpoints"
    ));
}

#[test]
fn test_sync_config_full_sync_with_checkpoints() {
    let config = SyncConfig::full_sync_with_checkpoints("/custom/path".to_string());
    assert!(!config.incremental);
    assert!(config.emit_checkpoints);
    assert!(matches!(
        config.checkpoint_storage,
        crate::CheckpointStorage::Filesystem { ref dir } if dir == "/custom/path"
    ));
}

#[test]
fn test_sync_config_incremental() {
    let config = SyncConfig::incremental();
    assert!(config.incremental);
    assert!(!config.emit_checkpoints);
    assert!(matches!(
        config.checkpoint_storage,
        crate::CheckpointStorage::Disabled
    ));
}

#[test]
fn test_sync_config_should_emit_checkpoints() {
    // Both enabled and dir configured
    let config1 = SyncConfig {
        incremental: false,
        emit_checkpoints: true,
        checkpoint_storage: crate::CheckpointStorage::Filesystem {
            dir: "/tmp".to_string(),
        },
    };
    assert!(config1.should_emit_checkpoints());

    // Emit disabled
    let config2 = SyncConfig {
        incremental: false,
        emit_checkpoints: false,
        checkpoint_storage: crate::CheckpointStorage::Filesystem {
            dir: "/tmp".to_string(),
        },
    };
    assert!(!config2.should_emit_checkpoints());

    // Dir not configured
    let config3 = SyncConfig {
        incremental: false,
        emit_checkpoints: true,
        checkpoint_storage: crate::CheckpointStorage::Disabled,
    };
    assert!(!config3.should_emit_checkpoints());
}

// ============================================================================
// SyncManager Tests
// ============================================================================

#[tokio::test]
async fn test_sync_manager_emit_and_read() {
    let tmp = TempDir::new().unwrap();
    let config = SyncConfig {
        emit_checkpoints: true,
        checkpoint_storage: crate::CheckpointStorage::Filesystem {
            dir: tmp.path().to_string_lossy().to_string(),
        },
        incremental: false,
    };

    let manager = SyncManager::new(config, None);
    let cp = TestCheckpoint {
        value: 123,
        timestamp: Utc::now(),
    };

    manager
        .emit_checkpoint(&cp, SyncPhase::FullSyncStart)
        .await
        .unwrap();

    let read: TestCheckpoint = manager
        .read_checkpoint(SyncPhase::FullSyncStart)
        .await
        .unwrap();

    assert_eq!(read.value, 123);
}

#[tokio::test]
async fn test_sync_manager_emit_disabled() {
    let tmp = TempDir::new().unwrap();
    let config = SyncConfig {
        emit_checkpoints: false, // Disabled
        checkpoint_storage: crate::CheckpointStorage::Filesystem {
            dir: tmp.path().to_string_lossy().to_string(),
        },
        incremental: false,
    };

    let manager = SyncManager::new(config, None);
    let cp = TestCheckpoint {
        value: 456,
        timestamp: Utc::now(),
    };

    // Should succeed but not write anything
    manager
        .emit_checkpoint(&cp, SyncPhase::FullSyncStart)
        .await
        .unwrap();

    // Should fail to read (no file written)
    let result: anyhow::Result<TestCheckpoint> =
        manager.read_checkpoint(SyncPhase::FullSyncStart).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_sync_manager_no_checkpoint_dir() {
    let config = SyncConfig {
        emit_checkpoints: true,
        checkpoint_storage: crate::CheckpointStorage::Disabled, // Checkpoints not configured
        incremental: false,
    };

    let manager = SyncManager::new(config, None);
    let cp = TestCheckpoint {
        value: 789,
        timestamp: Utc::now(),
    };

    // Should fail because no dir configured
    let result = manager.emit_checkpoint(&cp, SyncPhase::FullSyncStart).await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("No checkpoint directory"));
}

#[tokio::test]
async fn test_sync_manager_reads_latest_checkpoint() {
    let tmp = TempDir::new().unwrap();
    let config = SyncConfig {
        emit_checkpoints: true,
        checkpoint_storage: crate::CheckpointStorage::Filesystem {
            dir: tmp.path().to_string_lossy().to_string(),
        },
        incremental: false,
    };

    let manager = SyncManager::new(config, None);

    // Write first checkpoint
    let cp1 = TestCheckpoint {
        value: 100,
        timestamp: Utc::now(),
    };
    manager
        .emit_checkpoint(&cp1, SyncPhase::FullSyncStart)
        .await
        .unwrap();

    // Wait a bit to ensure different modification times
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Write second checkpoint
    let cp2 = TestCheckpoint {
        value: 200,
        timestamp: Utc::now(),
    };
    manager
        .emit_checkpoint(&cp2, SyncPhase::FullSyncStart)
        .await
        .unwrap();

    // Should read the second (latest) checkpoint
    let read: TestCheckpoint = manager
        .read_checkpoint(SyncPhase::FullSyncStart)
        .await
        .unwrap();

    assert_eq!(read.value, 200);
}

#[tokio::test]
async fn test_sync_manager_separate_phases() {
    let tmp = TempDir::new().unwrap();
    let config = SyncConfig {
        emit_checkpoints: true,
        checkpoint_storage: crate::CheckpointStorage::Filesystem {
            dir: tmp.path().to_string_lossy().to_string(),
        },
        incremental: false,
    };

    let manager = SyncManager::new(config, None);

    // Write start checkpoint
    let cp_start = TestCheckpoint {
        value: 1000,
        timestamp: Utc::now(),
    };
    manager
        .emit_checkpoint(&cp_start, SyncPhase::FullSyncStart)
        .await
        .unwrap();

    // Write end checkpoint
    let cp_end = TestCheckpoint {
        value: 2000,
        timestamp: Utc::now(),
    };
    manager
        .emit_checkpoint(&cp_end, SyncPhase::FullSyncEnd)
        .await
        .unwrap();

    // Read each phase separately
    let read_start: TestCheckpoint = manager
        .read_checkpoint(SyncPhase::FullSyncStart)
        .await
        .unwrap();
    let read_end: TestCheckpoint = manager
        .read_checkpoint(SyncPhase::FullSyncEnd)
        .await
        .unwrap();

    assert_eq!(read_start.value, 1000);
    assert_eq!(read_end.value, 2000);
}

// ============================================================================
// Checkpoint CLI String Tests
// ============================================================================

#[test]
fn test_checkpoint_cli_string_roundtrip() {
    let cp = TestCheckpoint {
        value: 999,
        timestamp: Utc::now(),
    };
    let cli_str = cp.to_cli_string();
    let parsed = TestCheckpoint::from_cli_string(&cli_str).unwrap();
    assert_eq!(parsed.value, cp.value);
    // Compare seconds to avoid nanosecond precision issues
    assert_eq!(parsed.timestamp.timestamp(), cp.timestamp.timestamp());
}

#[test]
fn test_checkpoint_cli_string_invalid_format() {
    // Missing separator
    let result = TestCheckpoint::from_cli_string("123");
    assert!(result.is_err());

    // Invalid value
    let result = TestCheckpoint::from_cli_string("notanumber:2024-01-01T00:00:00Z");
    assert!(result.is_err());

    // Invalid timestamp
    let result = TestCheckpoint::from_cli_string("123:invalid-timestamp");
    assert!(result.is_err());
}

#[test]
fn test_checkpoint_cli_string_specific_value() {
    let cli_str = "42:2024-06-15T14:30:00Z";
    let cp = TestCheckpoint::from_cli_string(cli_str).unwrap();

    assert_eq!(cp.value, 42);
    assert_eq!(cp.timestamp.year(), 2024);
    assert_eq!(cp.timestamp.month(), 6);
    assert_eq!(cp.timestamp.day(), 15);
    assert_eq!(cp.timestamp.hour(), 14);
    assert_eq!(cp.timestamp.minute(), 30);
}

// Need Datelike and Timelike traits for year(), month(), etc.
use chrono::{Datelike, Timelike};
