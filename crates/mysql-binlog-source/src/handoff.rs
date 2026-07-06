//! Snapshot handoff registry for binlog sync restart semantics.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use binlog_protocol::BinlogPosition;
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// How a table entered the handoff registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffKind {
    /// Copied during the initial interleaved snapshot.
    Initial,
    /// Copied via ad-hoc execute-snapshot signal during incremental sync.
    AdHoc,
}

/// One table recorded as snapshot-complete.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableHandoffEntry {
    pub name: String,
    pub kind: HandoffKind,
    pub completed_at: DateTime<Utc>,
    /// Stream position when this table finished (debugging / alignment).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_pos_at_completion: Option<BinlogPosition>,
}

/// Registry of snapshotted tables and the stream position they align with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncHandoffMetadata {
    pub version: u32,
    /// Current reference stream position (refreshed periodically during incremental sync).
    pub stream_pos: BinlogPosition,
    pub snapshotted_tables: Vec<TableHandoffEntry>,
}

impl SyncHandoffMetadata {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(stream_pos: BinlogPosition) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            stream_pos,
            snapshotted_tables: Vec::new(),
        }
    }

    pub fn snapshotted_names(&self) -> HashSet<&str> {
        self.snapshotted_tables
            .iter()
            .map(|entry| entry.name.as_str())
            .collect()
    }

    /// Merge newly completed tables, replacing any prior entry for the same name.
    pub fn merge_tables(
        &mut self,
        table_names: &[String],
        kind: HandoffKind,
        stream_pos: BinlogPosition,
    ) {
        self.stream_pos = stream_pos.clone();
        let now = Utc::now();
        let mut by_name: HashMap<String, TableHandoffEntry> = self
            .snapshotted_tables
            .drain(..)
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        for name in table_names {
            by_name.insert(
                name.clone(),
                TableHandoffEntry {
                    name: name.clone(),
                    kind,
                    completed_at: now,
                    stream_pos_at_completion: Some(stream_pos.clone()),
                },
            );
        }
        let mut names: Vec<_> = by_name.keys().cloned().collect();
        names.sort();
        self.snapshotted_tables = names
            .into_iter()
            .map(|name| by_name.remove(&name).expect("entry present"))
            .collect();
    }

    pub fn refresh_stream_pos(&mut self, stream_pos: BinlogPosition) {
        self.stream_pos = stream_pos;
    }
}

impl Checkpoint for SyncHandoffMetadata {
    const DATABASE_TYPE: &'static str = "mysql-binlog";

    fn to_cli_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

/// Tables in `requested` that are not yet recorded in handoff metadata.
pub fn tables_pending_snapshot(
    requested: &[String],
    metadata: &SyncHandoffMetadata,
) -> Vec<String> {
    let done = metadata.snapshotted_names();
    requested
        .iter()
        .filter(|name| !done.contains(name.as_str()))
        .cloned()
        .collect()
}

pub async fn read_handoff_metadata<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> Result<Option<SyncHandoffMetadata>> {
    match manager
        .read_checkpoint(SyncPhase::SyncHandoffMetadata)
        .await
    {
        Ok(metadata) => Ok(Some(metadata)),
        Err(_) => Ok(None),
    }
}

pub async fn emit_handoff_metadata<St: CheckpointStore>(
    manager: &SyncManager<St>,
    metadata: &SyncHandoffMetadata,
) -> Result<()> {
    manager
        .emit_checkpoint(metadata, SyncPhase::SyncHandoffMetadata)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use binlog_protocol::BinlogPosition;
    use checkpoint::FilesystemStore;
    use tempfile::TempDir;

    #[test]
    fn merge_tables_replaces_and_sorts() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 4);
        let mut metadata = SyncHandoffMetadata::new(pos.clone());
        metadata.merge_tables(
            &["users".to_string(), "orders".to_string()],
            HandoffKind::Initial,
            pos.clone(),
        );
        metadata.merge_tables(&["orders".to_string()], HandoffKind::AdHoc, pos);
        assert_eq!(metadata.snapshotted_tables.len(), 2);
        assert_eq!(metadata.snapshotted_tables[0].name, "orders");
        assert_eq!(metadata.snapshotted_tables[0].kind, HandoffKind::AdHoc);
        assert_eq!(metadata.snapshotted_tables[1].name, "users");
    }

    #[test]
    fn expanded_tables_restart_skips_already_handshotted() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 100);
        let mut metadata = SyncHandoffMetadata::new(pos.clone());
        metadata.merge_tables(&["users".to_string()], HandoffKind::Initial, pos);
        let requested = vec!["users".to_string(), "orders".to_string()];
        let pending = tables_pending_snapshot(&requested, &metadata);
        assert_eq!(pending, vec!["orders".to_string()]);
        assert!(tables_pending_snapshot(&["users".to_string()], &metadata).is_empty());
    }

    #[test]
    fn tables_pending_snapshot_excludes_done() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 4);
        let mut metadata = SyncHandoffMetadata::new(pos.clone());
        metadata.merge_tables(&["a".to_string()], HandoffKind::Initial, pos);
        let pending = tables_pending_snapshot(&["a".to_string(), "b".to_string()], &metadata);
        assert_eq!(pending, vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn handoff_metadata_roundtrip_via_manager() {
        let tmp = TempDir::new().unwrap();
        let manager = SyncManager::new(FilesystemStore::new(tmp.path()));
        let pos = BinlogPosition::file_pos("mysql-bin.000002", 100);
        let mut original = SyncHandoffMetadata::new(pos.clone());
        original.merge_tables(&["widgets".to_string()], HandoffKind::Initial, pos);
        emit_handoff_metadata(&manager, &original).await.unwrap();
        let loaded = read_handoff_metadata(&manager).await.unwrap().unwrap();
        assert_eq!(loaded, original);
    }
}
