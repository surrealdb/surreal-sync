//! Sync types for MongoDB incremental sync
//!
//! These types are used for incremental synchronization from MongoDB to SurrealDB.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surreal_sync_surreal::Change;

/// Source database types
#[derive(Debug, Clone, PartialEq)]
pub enum SourceDatabase {
    Neo4j,
    MongoDB,
    PostgreSQL,
    MySQL,
}

/// Sync checkpoint for MongoDB
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
    /// Convert checkpoint to a string representation
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
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

    /// Parse checkpoint from a string representation
    pub fn from_string(s: &str) -> anyhow::Result<Self> {
        use base64::{engine::general_purpose, Engine as _};

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
                if subparts.len() == 2 && subparts[0] == "sequence" {
                    let seq_id = subparts[1].parse::<i64>()?;
                    Ok(SyncCheckpoint::MySQL {
                        sequence_id: seq_id,
                        timestamp: Utc::now(),
                    })
                } else {
                    Err(anyhow::anyhow!("Invalid MySQL checkpoint format"))
                }
            }
            _ => Err(anyhow::anyhow!("Unknown checkpoint type: {}", parts[0])),
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
}

/// Trait for source databases that support incremental sync
#[async_trait::async_trait]
pub trait IncrementalSource: Send + Sync {
    /// Get the source database type
    fn source_type(&self) -> SourceDatabase;

    /// Initialize the incremental source
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
    async fn next(&mut self) -> Option<anyhow::Result<Change>>;

    /// Get the current checkpoint of the stream
    fn checkpoint(&self) -> Option<SyncCheckpoint>;
}

/// Represents different phases of the synchronization process
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SyncPhase {
    /// Checkpoint emitted before full sync begins (t1)
    FullSyncStart,
    /// Checkpoint emitted after full sync completes (t2)
    FullSyncEnd,
}

impl SyncPhase {
    pub fn as_str(&self) -> &str {
        match self {
            SyncPhase::FullSyncStart => "full_sync_start",
            SyncPhase::FullSyncEnd => "full_sync_end",
        }
    }
}

impl std::fmt::Display for SyncPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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
}
