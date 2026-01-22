//! MongoDB checkpoint management
//!
//! This module provides utilities for obtaining and managing MongoDB resume tokens
//! for change stream-based incremental synchronization.

use anyhow::Result;
use chrono::{DateTime, Utc};
use mongodb::{
    options::{ChangeStreamOptions, FullDocumentType},
    Client as MongoClient,
};
use serde::{Deserialize, Serialize};

/// MongoDB-specific checkpoint containing resume token and timestamp
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MongoDBCheckpoint {
    /// Resume token for MongoDB change stream
    pub resume_token: Vec<u8>,
    /// Timestamp when checkpoint was created
    pub timestamp: DateTime<Utc>,
}

impl checkpoint::Checkpoint for MongoDBCheckpoint {
    const DATABASE_TYPE: &'static str = "mongodb";

    fn to_cli_string(&self) -> String {
        use base64::{engine::general_purpose, Engine as _};
        format!(
            "{}:{}",
            general_purpose::STANDARD.encode(&self.resume_token),
            self.timestamp.to_rfc3339()
        )
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        use base64::{engine::general_purpose, Engine as _};

        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "Invalid MongoDB checkpoint format: expected 'base64token:timestamp', got '{s}'"
            );
        }

        if parts[0].is_empty() {
            anyhow::bail!("Invalid MongoDB checkpoint: resume token cannot be empty");
        }

        let resume_token = general_purpose::STANDARD
            .decode(parts[0])
            .map_err(|e| anyhow::anyhow!("Invalid base64 resume token: {e}"))?;

        let timestamp = DateTime::parse_from_rfc3339(parts[1])
            .map_err(|e| anyhow::anyhow!("Invalid timestamp format: {e}"))?
            .with_timezone(&Utc);

        Ok(Self {
            resume_token,
            timestamp,
        })
    }
}

/// Get current resume token from MongoDB change stream using existing client
pub async fn get_resume_token(client: &MongoClient, database: &str) -> Result<Vec<u8>> {
    let database = client.database(database);

    // Create a change stream with no pipeline to get current token
    let options = ChangeStreamOptions::builder()
        .full_document(Some(FullDocumentType::UpdateLookup))
        .build();

    let change_stream = database.watch().with_options(options).await?;

    // Get the resume token from the stream
    if let Some(token) = change_stream.resume_token() {
        let bytes = bson::to_vec(&token)?;
        return Ok(bytes);
    }

    // If no token is immediately available, return an error
    Err(anyhow::anyhow!(
        "No resume token available from change stream"
    ))
}

/// Get current checkpoint from MongoDB
///
/// This is a GENERATION operation - it queries MongoDB for the current resume token
/// and creates a new checkpoint from that position.
pub async fn get_current_checkpoint(
    client: &MongoClient,
    database: &str,
) -> Result<MongoDBCheckpoint> {
    let resume_token = get_resume_token(client, database).await?;
    Ok(MongoDBCheckpoint {
        resume_token,
        timestamp: Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use checkpoint::{Checkpoint, CheckpointFile, SyncConfig, SyncManager, SyncPhase};
    use tempfile::TempDir;

    #[test]
    fn test_mongodb_checkpoint_cli_string_roundtrip() {
        // Create a checkpoint with realistic data
        let original = MongoDBCheckpoint {
            resume_token: vec![1, 2, 3, 4, 5, 6, 7, 8],
            timestamp: Utc::now(),
        };

        // Encode to CLI string
        let cli_string = original.to_cli_string();

        // Decode from CLI string
        let decoded = MongoDBCheckpoint::from_cli_string(&cli_string).unwrap();

        // Verify roundtrip
        assert_eq!(original.resume_token, decoded.resume_token);
        // Timestamps may lose nanoseconds precision in RFC3339, so compare seconds
        assert_eq!(
            original.timestamp.timestamp(),
            decoded.timestamp.timestamp()
        );
    }

    #[test]
    fn test_mongodb_checkpoint_file_roundtrip() {
        // Create a checkpoint
        let original = MongoDBCheckpoint {
            resume_token: vec![10, 20, 30, 40],
            timestamp: Utc::now(),
        };

        // Create checkpoint file
        let file = CheckpointFile::new(&original, SyncPhase::FullSyncStart).unwrap();

        // Verify database type
        assert_eq!(file.database_type(), MongoDBCheckpoint::DATABASE_TYPE);
        assert_eq!(file.phase(), &SyncPhase::FullSyncStart);

        // Parse back to checkpoint
        let decoded: MongoDBCheckpoint = file.parse().unwrap();

        // Verify roundtrip
        assert_eq!(original.resume_token, decoded.resume_token);
    }

    #[test]
    fn test_mongodb_checkpoint_file_type_mismatch() {
        let checkpoint = MongoDBCheckpoint {
            resume_token: vec![1, 2, 3],
            timestamp: Utc::now(),
        };

        let file = CheckpointFile::new(&checkpoint, SyncPhase::FullSyncEnd).unwrap();

        // Serialize to JSON then modify database_type
        let mut json_value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&file).unwrap()).unwrap();
        json_value["database_type"] = serde_json::Value::String("neo4j".to_string());
        let modified_json = serde_json::to_string(&json_value).unwrap();
        let modified_file: CheckpointFile = serde_json::from_str(&modified_json).unwrap();

        // Should fail to parse as MongoDB
        let result: Result<MongoDBCheckpoint> = modified_file.parse();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }

    #[tokio::test]
    async fn test_mongodb_checkpoint_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let config = SyncConfig {
            emit_checkpoints: true,
            checkpoint_storage: checkpoint::CheckpointStorage::Filesystem {
                dir: tmp.path().to_string_lossy().to_string(),
            },
            incremental: false,
        };

        let manager = SyncManager::new(config, None);
        let original = MongoDBCheckpoint {
            resume_token: vec![100, 200, 255],
            timestamp: Utc::now(),
        };

        // Save checkpoint
        manager
            .emit_checkpoint(&original, SyncPhase::FullSyncStart)
            .await
            .unwrap();

        // Load checkpoint
        let loaded: MongoDBCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncStart)
            .await
            .unwrap();

        // Verify roundtrip
        assert_eq!(original.resume_token, loaded.resume_token);
    }

    #[test]
    fn test_mongodb_checkpoint_empty_resume_token() {
        // Empty resume token should be rejected
        let result = MongoDBCheckpoint::from_cli_string(":2024-01-01T00:00:00Z");
        assert!(result.is_err());
    }

    #[test]
    fn test_mongodb_checkpoint_invalid_base64() {
        let result = MongoDBCheckpoint::from_cli_string("not-valid-base64!!!:2024-01-01T00:00:00Z");
        assert!(result.is_err());
    }

    #[test]
    fn test_mongodb_checkpoint_invalid_timestamp() {
        use base64::{engine::general_purpose, Engine as _};
        let valid_base64 = general_purpose::STANDARD.encode([1, 2, 3]);
        let result = MongoDBCheckpoint::from_cli_string(&format!("{valid_base64}:not-a-timestamp"));
        assert!(result.is_err());
    }

    #[test]
    fn test_mongodb_checkpoint_database_type() {
        assert_eq!(MongoDBCheckpoint::DATABASE_TYPE, "mongodb");
    }
}
