//! Catch-up progress checkpoint for WAL sync restart semantics.

use std::collections::{HashMap, HashSet};

use crate::pgoutput_protocol::Lsn;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surreal_sync_core::{Checkpoint, CheckpointStore, SyncManager, SyncPhase};

use crate::from_pgoutput::checkpoint::{PgoutputCheckpoint, PgoutputReconciliationPos};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageKind {
    Initial,
    AdHoc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableCoverageEntry {
    pub name: String,
    pub kind: CoverageKind,
    pub completed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_at_completion: Option<Lsn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatchUpProgress {
    pub version: u32,
    pub position: PgoutputCheckpoint,
    pub covered_tables: Vec<TableCoverageEntry>,
}

impl CatchUpProgress {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(position: PgoutputCheckpoint) -> Self {
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

    pub fn merge_tables(
        &mut self,
        table_names: &[String],
        kind: CoverageKind,
        checkpoint: &PgoutputCheckpoint,
    ) {
        self.position = checkpoint.clone();
        let now = Utc::now();
        let stream_pos = checkpoint.lsn;
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
                    position_at_completion: Some(stream_pos),
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

    pub fn update_position(&mut self, checkpoint: PgoutputCheckpoint) {
        self.position = checkpoint;
    }

    pub fn prune_to_requested(&mut self, requested: &[String]) {
        let requested: HashSet<_> = requested.iter().map(String::as_str).collect();
        self.covered_tables
            .retain(|entry| requested.contains(entry.name.as_str()));
    }
}

impl Checkpoint for CatchUpProgress {
    const DATABASE_TYPE: &'static str = "postgresql-pgoutput";

    fn to_cli_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}

pub fn effective_sync_tables(
    requested: &[String],
    catch_up: Option<&CatchUpProgress>,
) -> Option<Vec<String>> {
    if requested.is_empty() {
        return None;
    }
    let mut names: HashSet<String> = requested.iter().cloned().collect();
    if let Some(progress) = catch_up {
        for entry in &progress.covered_tables {
            names.insert(entry.name.clone());
        }
    }
    let mut out: Vec<String> = names.into_iter().collect();
    out.sort();
    Some(out)
}

pub fn tables_pending_snapshot(requested: &[String], progress: &CatchUpProgress) -> Vec<String> {
    let done = progress.covered_names();
    requested
        .iter()
        .filter(|name| !done.contains(name.as_str()))
        .cloned()
        .collect()
}

pub fn max_pgoutput_checkpoint(candidates: &[PgoutputCheckpoint]) -> PgoutputCheckpoint {
    debug_assert!(!candidates.is_empty());
    candidates
        .iter()
        .max_by_key(|cp| PgoutputReconciliationPos::from(cp.lsn))
        .expect("non-empty")
        .clone()
}

pub async fn read_catch_up_progress<St: CheckpointStore>(
    manager: &SyncManager<St>,
) -> Result<Option<CatchUpProgress>> {
    match manager.read_checkpoint(SyncPhase::CatchUpProgress).await {
        Ok(progress) => Ok(Some(progress)),
        Err(_) => Ok(None),
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
