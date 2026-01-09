//! Universal sync module for surreal-sync
//!
//! This module provides a universal design for incremental and full synchronization
//! that can be used across different source databases (Neo4j, MongoDB, PostgreSQL, MySQL).
//!
//! # Design Overview
//!
//! The incremental sync design allows combining full sync with incremental updates:
//! 1. Full sync emits a checkpoint (t1) before starting
//! 2. Full sync completes and emits another checkpoint (t2)
//! 3. Incremental sync can start from t1 with --incremental-from flag
//! 4. Incremental sync may process some changes already in full sync (between t1 and t2)
//! 5. Once incremental sync catches up to t2, the target is consistent
//! 6. Incremental sync continues to apply new changes after t2
//!
//! This design ensures no data loss while allowing parallel full and incremental syncs.
//!
//! # Incremental Source Implementations
//!
//! Each database has its own incremental source implementation that implements
//! the [`IncrementalSource`] trait:
//!
//! - Neo4j: [`crate::neo4j_incremental::Neo4jIncrementalSource`] - Timestamp-based sync
//! - MongoDB: [`crate::mongodb_incremental::MongodbIncrementalSource`] - Change streams
//! - PostgreSQL: [`crate::postgresql_incremental::PostgresIncrementalSource`] - Trigger-based tracking
//! - MySQL: [`crate::mysql_incremental::MySQLIncrementalSource`] - Audit table tracking
//!
//! ## Implementation Approaches
//!
//! ### Native CDC (Change Data Capture)
//! - Neo4j: Uses timestamp-based change detection
//! - MongoDB: Uses native change streams (requires MongoDB 3.6+)
//!
//! ### Trigger-Based Tracking (Production-Ready Fallback)
//! - PostgreSQL: Uses database triggers + audit tables with sequence-based checkpointing
//! - MySQL: Uses database triggers + audit tables with sequence-based checkpointing
//! - Neo4j: Timestamp-based approach for all versions
//!
//! The trigger-based approaches are production-ready and used by many enterprise
//! systems. They provide reliable change capture without requiring special database
//! configurations or privileges.
//!
//! # Checkpoint Management
//!
//! The [`SyncCheckpoint`] enum supports different checkpoint formats for each database.
//! Checkpoints are managed by [`SyncManager`] and can be persisted to disk for
//! reliable resumption after failures.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use surreal_sync_surreal::Change;

/// Represents different phases of the synchronization process for checkpoint management
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncPhase {
    /// Checkpoint emitted before full sync begins (t1)
    FullSyncStart,
    /// Checkpoint emitted after full sync completes (t2)
    FullSyncEnd,
}

impl SyncPhase {
    /// Convert to string representation for file naming and storage
    pub fn as_str(&self) -> &str {
        match self {
            SyncPhase::FullSyncStart => "full_sync_start",
            SyncPhase::FullSyncEnd => "full_sync_end",
        }
    }
}

impl From<&str> for SyncPhase {
    fn from(s: &str) -> Self {
        match s {
            "full_sync_start" => SyncPhase::FullSyncStart,
            "full_sync_end" => SyncPhase::FullSyncEnd,
            other => panic!("Unknown sync phase: {other}"),
        }
    }
}

impl std::fmt::Display for SyncPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Represents a point in time for synchronization checkpointing.
/// Different databases may use different checkpoint formats:
/// - Neo4j: Timestamp only (timestamp-based sync)
/// - MongoDB: Resume token from change streams or timestamp
/// - PostgreSQL: Sequence ID from trigger-based audit table
/// - MySQL: Sequence ID from trigger-based audit table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncCheckpoint {
    /// Neo4j timestamp-based checkpoint
    Neo4j(DateTime<Utc>),
    /// MongoDB checkpoint with timestamp and resume token
    MongoDB {
        resume_token: Vec<u8>,
        timestamp: DateTime<Utc>,
    },
    /// PostgreSQL checkpoint with sequence_id for trigger-based sync
    PostgreSQL {
        sequence_id: i64,
        timestamp: DateTime<Utc>,
    },
    /// MySQL checkpoint with sequence_id for trigger-based sync
    MySQL {
        sequence_id: i64,
        timestamp: DateTime<Utc>,
    },
}

impl SyncCheckpoint {
    /// Convert checkpoint to a string representation for CLI arguments
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        match self {
            SyncCheckpoint::Neo4j(ts) => format!("neo4j:{}", ts.to_rfc3339()),
            SyncCheckpoint::MongoDB {
                resume_token,
                timestamp,
            } => format!(
                "mongodb:{}:{}",
                general_purpose::STANDARD.encode(resume_token),
                timestamp.to_rfc3339()
            ),
            SyncCheckpoint::PostgreSQL { sequence_id, .. } => {
                format!("postgresql:sequence:{sequence_id}")
            }
            SyncCheckpoint::MySQL { sequence_id, .. } => {
                format!("mysql:sequence:{sequence_id}")
            }
        }
    }

    /// Extract PostgreSQL sequence_id from checkpoint
    pub fn to_postgresql_sequence_id(&self) -> anyhow::Result<i64> {
        match self {
            SyncCheckpoint::PostgreSQL { sequence_id, .. } => Ok(*sequence_id),
            _ => Err(anyhow::anyhow!(
                "Cannot extract PostgreSQL sequence_id from {self:?} checkpoint",
            )),
        }
    }

    /// Extract MySQL sequence_id from checkpoint
    pub fn to_mysql_sequence_id(&self) -> anyhow::Result<i64> {
        match self {
            SyncCheckpoint::MySQL { sequence_id, .. } => Ok(*sequence_id),
            _ => Err(anyhow::anyhow!(
                "Cannot extract MySQL sequence_id from {self:?} checkpoint",
            )),
        }
    }

    /// Extract Neo4j timestamp from checkpoint
    pub fn to_neo4j_timestamp(&self) -> anyhow::Result<DateTime<Utc>> {
        match self {
            SyncCheckpoint::Neo4j(timestamp) => Ok(*timestamp),
            _ => Err(anyhow::anyhow!(
                "Cannot extract Neo4j timestamp from {self:?} checkpoint",
            )),
        }
    }

    /// Extract MongoDB resume token from checkpoint
    pub fn to_mongodb_resume_token(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            SyncCheckpoint::MongoDB { resume_token, .. } => Ok(resume_token.clone()),
            _ => Err(anyhow::anyhow!(
                "Cannot extract MongoDB resume token from {self:?} checkpoint",
            )),
        }
    }

    pub fn to_mongodb_checkpoint_string(&self) -> anyhow::Result<String> {
        match self {
            SyncCheckpoint::MongoDB {
                timestamp,
                resume_token,
            } => Ok(format!(
                "mongodb:{}:{}",
                general_purpose::STANDARD.encode(resume_token),
                timestamp.to_rfc3339()
            )),
            _ => Err(anyhow::anyhow!(
                "Cannot extract MongoDB checkpoint string from {self:?} checkpoint",
            )),
        }
    }

    /// Parse checkpoint from a string representation
    pub fn from_string(s: &str) -> anyhow::Result<Self> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid checkpoint format: {s}"));
        }

        match parts[0] {
            "neo4j" => {
                let ts = DateTime::parse_from_rfc3339(parts[1])?.with_timezone(&Utc);
                Ok(SyncCheckpoint::Neo4j(ts))
            }
            "mongodb" => {
                let subparts: Vec<&str> = parts[1].splitn(2, ':').collect();
                if subparts.len() == 2 && !subparts[0].is_empty() {
                    let token = general_purpose::STANDARD.decode(subparts[0])?;
                    let ts = DateTime::parse_from_rfc3339(subparts[1])?.with_timezone(&Utc);
                    Ok(SyncCheckpoint::MongoDB {
                        resume_token: token,
                        timestamp: ts,
                    })
                } else {
                    Err(anyhow::anyhow!("Invalid MongoDB checkpoint format"))
                }
            }
            "postgresql" => {
                let subparts: Vec<&str> = parts[1].splitn(2, ':').collect();
                if subparts.len() == 2 && subparts[0] == "sequence" {
                    let seq_id = subparts[1].parse::<i64>()?;
                    Ok(SyncCheckpoint::PostgreSQL {
                        sequence_id: seq_id,
                        timestamp: Utc::now(),
                    })
                } else {
                    Err(anyhow::anyhow!(
                        "Invalid PostgreSQL checkpoint format: expected 'postgresql:sequence:N', got 'postgresql:{}'",
                        parts[1]
                    ))
                }
            }
            "mysql" => {
                let subparts: Vec<&str> = parts[1].splitn(2, ':').collect();
                if subparts.len() == 2 {
                    match subparts[0] {
                        "sequence" => {
                            let seq_id = subparts[1].parse::<i64>()?;
                            Ok(SyncCheckpoint::MySQL {
                                sequence_id: seq_id,
                                timestamp: Utc::now(),
                            })
                        }
                        _ => Err(anyhow::anyhow!("Invalid MySQL checkpoint subtype")),
                    }
                } else {
                    Err(anyhow::anyhow!("Invalid MySQL checkpoint format"))
                }
            }
            _ => Err(anyhow::anyhow!("Unknown checkpoint type: {}", parts[0])),
        }
    }
}

/// Source database types
#[derive(Debug, Clone, PartialEq)]
pub enum SourceDatabase {
    Neo4j,
    MongoDB,
    PostgreSQL,
    MySQL,
}

/// Trait for source databases that support incremental sync
///
/// Each source database (Neo4j, MongoDB, PostgreSQL, MySQL) implements this trait
/// to provide incremental sync capabilities specific to that database.
#[async_trait::async_trait]
pub trait IncrementalSource: Send + Sync {
    /// Get the source database type
    fn source_type(&self) -> SourceDatabase;

    /// Initialize the incremental source (setup tasks like schema collection)
    async fn initialize(&mut self) -> anyhow::Result<()>;

    /// Get a stream of changes
    async fn get_changes(&mut self) -> anyhow::Result<Box<dyn ChangeStream>>;

    /// Get the current checkpoint
    async fn get_checkpoint(&self) -> anyhow::Result<SyncCheckpoint>;

    /// Cleanup resources
    async fn cleanup(self) -> anyhow::Result<()>;
}

/// Trait for a stream of changes from the source database
#[async_trait::async_trait]
pub trait ChangeStream: Send + Sync {
    /// Get the next change event from the stream
    /// Returns None when no more changes are available
    async fn next(&mut self) -> Option<anyhow::Result<Change>>;

    /// Get the current checkpoint of the stream
    /// This can be used to resume from this position later
    fn checkpoint(&self) -> Option<SyncCheckpoint>;
}

/// Configuration for sync operations
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Whether to run in incremental mode
    pub incremental: bool,
    /// Checkpoint to start incremental sync from
    pub incremental_from: Option<SyncCheckpoint>,
    /// Whether to emit checkpoints during sync
    pub emit_checkpoints: bool,
    /// Path to write checkpoint files
    pub checkpoint_dir: Option<String>,
}

impl Default for SyncConfig {
    fn default() -> Self {
        SyncConfig {
            incremental: false,
            incremental_from: None,
            emit_checkpoints: true,
            checkpoint_dir: Some(".surreal-sync-checkpoints".to_string()),
        }
    }
}

/// Manager for handling sync operations with checkpoint tracking
pub struct SyncManager {
    config: SyncConfig,
}

impl SyncManager {
    pub fn new(config: SyncConfig) -> Self {
        SyncManager { config }
    }

    /// Emit a checkpoint to file for later use
    /// This is called at the start (t1) and end (t2) of full sync
    pub async fn emit_checkpoint(
        &self,
        checkpoint: &SyncCheckpoint,
        phase: SyncPhase,
    ) -> anyhow::Result<()> {
        if !self.config.emit_checkpoints {
            return Ok(());
        }

        let dir = self
            .config
            .checkpoint_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No checkpoint directory configured"))?;

        // Create checkpoint directory if it doesn't exist
        std::fs::create_dir_all(dir)?;

        let timestamp = Utc::now().to_rfc3339();
        let filename = format!("{}/checkpoint_{}_{}.json", dir, phase.as_str(), timestamp);

        let checkpoint_data = serde_json::json!({
            "checkpoint": checkpoint,
            "phase": phase.as_str(),
            "timestamp": timestamp,
        });

        std::fs::write(&filename, serde_json::to_string_pretty(&checkpoint_data)?)?;

        tracing::info!(
            "Emitted {} checkpoint to {}: {}",
            phase,
            filename,
            checkpoint.to_string()
        );

        Ok(())
    }

    /// Read the latest checkpoint for a given phase
    pub async fn read_latest_checkpoint(&self, phase: SyncPhase) -> anyhow::Result<SyncCheckpoint> {
        let dir = self
            .config
            .checkpoint_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No checkpoint directory configured"))?;

        let mut latest_file = None;
        let mut latest_time = None;

        // Find the latest checkpoint file for the given phase
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(filename) = path.file_name() {
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with(&format!("checkpoint_{}_", phase.as_str())) {
                    let metadata = entry.metadata()?;
                    let modified = metadata.modified()?;
                    if latest_time.is_none() || modified > latest_time.unwrap() {
                        latest_time = Some(modified);
                        latest_file = Some(path);
                    }
                }
            }
        }

        let file_path =
            latest_file.ok_or_else(|| anyhow::anyhow!("No checkpoint found for phase: {phase}"))?;

        let content = std::fs::read_to_string(file_path)?;
        let data: serde_json::Value = serde_json::from_str(&content)?;
        let checkpoint = serde_json::from_value(data["checkpoint"].clone())?;

        Ok(checkpoint)
    }
}

// Re-export base64 for checkpoint serialization
pub use base64::{engine::general_purpose, Engine as _};
