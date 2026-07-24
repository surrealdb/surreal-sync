//! Resumable checkpoint for the watermark-based interleaved snapshot strategy.
//!
//! Unlike the scalar full-sync checkpoints (which carry a single stream
//! position), an interleaved snapshot copies tables in primary-key-ordered
//! chunks while concurrently consuming the change stream. To resume after a
//! crash it must remember both the current stream position and how far each
//! table has been copied.
//!
//! Both the stream position and the per-table last primary key are stored as
//! opaque JSON values so this type stays independent of any particular source
//! backend (an LSN string, an integer sequence id, a single- or composite-key
//! tuple, etc. all serialize to JSON).

use serde::{Deserialize, Serialize};

/// Progress for a single table within an interleaved snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotTableProgress {
    /// Table name.
    pub name: String,
    /// Last primary key copied for this table, serialized as JSON.
    ///
    /// `None` means no chunk has been copied yet; resume starts from the
    /// beginning of the table.
    pub last_pk: Option<serde_json::Value>,
    /// Whether the table has been fully copied.
    pub done: bool,
}

/// Resumable checkpoint describing the state of an in-progress interleaved
/// snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterleavedSnapshotCheckpoint {
    /// Current reconciliation stream position, serialized as JSON.
    ///
    /// On resume the snapshot continues consuming the change stream from this
    /// position; on completion this is the position handed off to downstream
    /// replication-tail processing.
    #[serde(alias = "stream_pos")]
    pub reconciliation_pos: serde_json::Value,
    /// Per-table copy progress.
    pub tables: Vec<SnapshotTableProgress>,
}

impl InterleavedSnapshotCheckpoint {
    /// Create a new snapshot checkpoint.
    pub fn new(reconciliation_pos: serde_json::Value, tables: Vec<SnapshotTableProgress>) -> Self {
        Self {
            reconciliation_pos,
            tables,
        }
    }

    /// Whether every table has been fully copied.
    pub fn all_done(&self) -> bool {
        self.tables.iter().all(|t| t.done)
    }
}

impl crate::checkpoint::Checkpoint for InterleavedSnapshotCheckpoint {
    const DATABASE_TYPE: &'static str = "interleaved_snapshot";

    fn to_cli_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn from_cli_string(s: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{Checkpoint, CheckpointFile, SyncPhase};

    fn sample() -> InterleavedSnapshotCheckpoint {
        InterleavedSnapshotCheckpoint::new(
            serde_json::json!("0/16B3748"),
            vec![
                SnapshotTableProgress {
                    name: "users".to_string(),
                    last_pk: Some(serde_json::json!([{ "type": "Int64", "value": 42 }])),
                    done: false,
                },
                SnapshotTableProgress {
                    name: "orders".to_string(),
                    last_pk: None,
                    done: true,
                },
            ],
        )
    }

    #[test]
    fn cli_string_roundtrip() {
        let original = sample();
        let s = original.to_cli_string();
        let decoded = InterleavedSnapshotCheckpoint::from_cli_string(&s).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn checkpoint_file_roundtrip() {
        let original = sample();
        let file = CheckpointFile::new(&original, SyncPhase::SnapshotProgress).unwrap();
        assert_eq!(
            file.database_type(),
            InterleavedSnapshotCheckpoint::DATABASE_TYPE
        );
        let decoded: InterleavedSnapshotCheckpoint = file.parse().unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn all_done_reflects_table_state() {
        let mut cp = sample();
        assert!(!cp.all_done());
        for t in &mut cp.tables {
            t.done = true;
        }
        assert!(cp.all_done());
    }
}
