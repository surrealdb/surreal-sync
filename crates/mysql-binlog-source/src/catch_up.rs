//! Catch-up progress checkpoint for binlog sync restart semantics.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use binlog_protocol::{BinlogPosition, Flavor};
use checkpoint::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::checkpoint::{BinlogCheckpoint, BinlogReconciliationPos};

/// How a table entered the coverage registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageKind {
    /// Copied during the initial interleaved snapshot.
    Initial,
    /// Copied via ad-hoc execute-snapshot signal during incremental sync.
    AdHoc,
}

/// One table recorded as snapshot-complete.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableCoverageEntry {
    pub name: String,
    pub kind: CoverageKind,
    pub completed_at: DateTime<Utc>,
    /// Stream position when this table finished (optional audit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_at_completion: Option<BinlogPosition>,
}

/// Stream position plus table coverage for catch-up and restart planning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatchUpProgress {
    pub version: u32,
    pub position: BinlogCheckpoint,
    pub covered_tables: Vec<TableCoverageEntry>,
}

impl CatchUpProgress {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(position: BinlogCheckpoint) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            position,
            covered_tables: Vec::new(),
        }
    }

    pub fn covered_names(&self) -> HashSet<&str> {
        self.covered_tables
            .iter()
            .map(|entry| entry.name.as_str())
            .collect()
    }

    /// Merge newly completed tables, replacing any prior entry for the same name.
    pub fn merge_tables(
        &mut self,
        table_names: &[String],
        kind: CoverageKind,
        checkpoint: &BinlogCheckpoint,
    ) {
        self.position = checkpoint.clone();
        let now = Utc::now();
        let stream_pos = checkpoint.position.clone();
        let mut by_name: HashMap<String, TableCoverageEntry> = self
            .covered_tables
            .drain(..)
            .map(|entry| (entry.name.clone(), entry))
            .collect();
        for name in table_names {
            by_name.insert(
                name.clone(),
                TableCoverageEntry {
                    name: name.clone(),
                    kind,
                    completed_at: now,
                    position_at_completion: Some(stream_pos.clone()),
                },
            );
        }
        let mut names: Vec<_> = by_name.keys().cloned().collect();
        names.sort();
        self.covered_tables = names
            .into_iter()
            .map(|name| by_name.remove(&name).expect("entry present"))
            .collect();
    }

    pub fn update_position(&mut self, checkpoint: BinlogCheckpoint) {
        self.position = checkpoint;
    }

    /// Drop coverage entries for tables no longer in the requested sync set.
    pub fn prune_to_requested(&mut self, requested: &[String]) {
        let requested: HashSet<_> = requested.iter().map(String::as_str).collect();
        self.covered_tables
            .retain(|entry| requested.contains(entry.name.as_str()));
    }
}

impl Checkpoint for CatchUpProgress {
    const DATABASE_TYPE: &'static str = "mysql-binlog";

    fn to_cli_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

/// Tables in `requested` that are not yet recorded in catch-up progress.
pub fn tables_pending_snapshot(requested: &[String], progress: &CatchUpProgress) -> Vec<String> {
    let done = progress.covered_names();
    requested
        .iter()
        .filter(|name| !done.contains(name.as_str()))
        .cloned()
        .collect()
}

/// Pick the checkpoint whose position is furthest ahead in the binlog stream.
pub fn max_binlog_checkpoint(candidates: &[BinlogCheckpoint]) -> BinlogCheckpoint {
    debug_assert!(!candidates.is_empty());
    candidates
        .iter()
        .max_by_key(|cp| BinlogReconciliationPos::from(cp.position.clone()))
        .expect("non-empty")
        .clone()
}

pub async fn read_catch_up_progress<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> Result<Option<CatchUpProgress>> {
    match manager.read_checkpoint(SyncPhase::CatchUpProgress).await {
        Ok(progress) => Ok(Some(progress)),
        Err(_) => read_legacy_handoff_metadata(manager).await,
    }
}

async fn read_legacy_handoff_metadata<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> Result<Option<CatchUpProgress>> {
    #[derive(Debug, Deserialize)]
    struct LegacyHandoffMetadata {
        version: u32,
        stream_pos: BinlogPosition,
        snapshotted_tables: Vec<LegacyTableEntry>,
    }

    #[derive(Debug, Deserialize)]
    struct LegacyTableEntry {
        name: String,
        kind: CoverageKind,
        completed_at: DateTime<Utc>,
        #[serde(default)]
        stream_pos_at_completion: Option<BinlogPosition>,
    }

    let id = checkpoint::CheckpointID {
        database_type: CatchUpProgress::DATABASE_TYPE.to_string(),
        phase: "sync_handoff_metadata".to_string(),
    };
    let Some(stored) = manager.store().read_checkpoint(&id).await? else {
        return Ok(None);
    };
    let legacy: LegacyHandoffMetadata = serde_json::from_str(&stored.checkpoint_data)?;
    let flavor = flavor_from_position(&legacy.stream_pos);
    Ok(Some(CatchUpProgress {
        version: legacy.version,
        position: BinlogCheckpoint {
            flavor,
            position: legacy.stream_pos,
            timestamp: Utc::now(),
        },
        covered_tables: legacy
            .snapshotted_tables
            .into_iter()
            .map(|entry| TableCoverageEntry {
                name: entry.name,
                kind: entry.kind,
                completed_at: entry.completed_at,
                position_at_completion: entry.stream_pos_at_completion,
            })
            .collect(),
    }))
}

fn flavor_from_position(position: &BinlogPosition) -> Flavor {
    match position {
        BinlogPosition::MariaDbGtid { .. } => Flavor::MariaDb,
        _ => Flavor::MySql,
    }
}

pub async fn emit_catch_up_progress<St: CheckpointStore>(
    manager: &SyncManager<St>,
    progress: &CatchUpProgress,
) -> Result<()> {
    manager
        .emit_checkpoint(progress, SyncPhase::CatchUpProgress)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use binlog_protocol::BinlogPosition;
    use checkpoint::FilesystemStore;
    use tempfile::TempDir;

    fn test_checkpoint(pos: BinlogPosition) -> BinlogCheckpoint {
        BinlogCheckpoint {
            flavor: Flavor::MySql,
            position: pos,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn merge_tables_replaces_and_sorts() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 4);
        let checkpoint = test_checkpoint(pos.clone());
        let mut progress = CatchUpProgress::new(checkpoint.clone());
        progress.merge_tables(
            &["users".to_string(), "orders".to_string()],
            CoverageKind::Initial,
            &checkpoint,
        );
        progress.merge_tables(
            &["orders".to_string()],
            CoverageKind::AdHoc,
            &test_checkpoint(BinlogPosition::file_pos("mysql-bin.000001", 8)),
        );
        assert_eq!(progress.covered_tables.len(), 2);
        assert_eq!(progress.covered_tables[0].name, "orders");
        assert_eq!(progress.covered_tables[0].kind, CoverageKind::AdHoc);
        assert_eq!(progress.covered_tables[1].name, "users");
    }

    #[test]
    fn expanded_tables_restart_skips_already_covered() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 100);
        let checkpoint = test_checkpoint(pos);
        let mut progress = CatchUpProgress::new(checkpoint.clone());
        progress.merge_tables(&["users".to_string()], CoverageKind::Initial, &checkpoint);
        let requested = vec!["users".to_string(), "orders".to_string()];
        let pending = tables_pending_snapshot(&requested, &progress);
        assert_eq!(pending, vec!["orders".to_string()]);
        assert!(tables_pending_snapshot(&["users".to_string()], &progress).is_empty());
    }

    #[test]
    fn tables_pending_snapshot_excludes_done() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 4);
        let checkpoint = test_checkpoint(pos);
        let mut progress = CatchUpProgress::new(checkpoint.clone());
        progress.merge_tables(&["a".to_string()], CoverageKind::Initial, &checkpoint);
        let pending = tables_pending_snapshot(&["a".to_string(), "b".to_string()], &progress);
        assert_eq!(pending, vec!["b".to_string()]);
    }

    #[test]
    fn prune_to_requested_drops_stale_tables() {
        let pos = BinlogPosition::file_pos("mysql-bin.000001", 4);
        let checkpoint = test_checkpoint(pos);
        let mut progress = CatchUpProgress::new(checkpoint.clone());
        progress.merge_tables(
            &["users".to_string(), "orders".to_string()],
            CoverageKind::Initial,
            &checkpoint,
        );
        progress.prune_to_requested(&["users".to_string()]);
        assert_eq!(progress.covered_names(), HashSet::from(["users"]));
    }

    #[test]
    fn max_binlog_checkpoint_picks_latest_position() {
        let older = test_checkpoint(BinlogPosition::file_pos("mysql-bin.000001", 10));
        let newer = test_checkpoint(BinlogPosition::file_pos("mysql-bin.000001", 20));
        let max = max_binlog_checkpoint(&[older.clone(), newer.clone()]);
        assert_eq!(max.position, newer.position);
    }

    #[tokio::test]
    async fn catch_up_progress_roundtrip_via_manager() {
        let tmp = TempDir::new().unwrap();
        let manager = SyncManager::new(FilesystemStore::new(tmp.path()));
        let pos = BinlogPosition::file_pos("mysql-bin.000002", 100);
        let checkpoint = test_checkpoint(pos);
        let mut original = CatchUpProgress::new(checkpoint.clone());
        original.merge_tables(&["widgets".to_string()], CoverageKind::Initial, &checkpoint);
        emit_catch_up_progress(&manager, &original).await.unwrap();
        let loaded = read_catch_up_progress(&manager).await.unwrap().unwrap();
        assert_eq!(loaded, original);
    }

    #[tokio::test]
    async fn periodic_checkpoint_does_not_overwrite_full_sync_end() {
        use checkpoint::SyncPhase;

        let tmp = TempDir::new().unwrap();
        let manager = SyncManager::new(FilesystemStore::new(tmp.path()));
        let t2 = test_checkpoint(BinlogPosition::file_pos("mysql-bin.000001", 100));
        let stream = test_checkpoint(BinlogPosition::file_pos("mysql-bin.000001", 200));

        manager
            .emit_checkpoint(&t2, SyncPhase::FullSyncEnd)
            .await
            .unwrap();

        let mut progress = CatchUpProgress::new(t2.clone());
        progress.update_position(stream.clone());
        emit_catch_up_progress(&manager, &progress).await.unwrap();

        let end: BinlogCheckpoint = manager
            .read_checkpoint(SyncPhase::FullSyncEnd)
            .await
            .unwrap();
        assert_eq!(
            end.position, t2.position,
            "FullSyncEnd must stay at t2 boundary"
        );

        let loaded = read_catch_up_progress(&manager).await.unwrap().unwrap();
        assert_eq!(loaded.position.position, stream.position);
    }
}
