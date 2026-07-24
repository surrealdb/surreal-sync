//! Standalone helpers for reading checkpoint files from a directory.

use surreal_sync_core::{CheckpointFile, StoredCheckpoint, SyncPhase};

/// Read a checkpoint file for a specific phase from a directory.
pub async fn get_checkpoint_for_phase<P: AsRef<std::path::Path>>(
    checkpoint_dir: P,
    phase: SyncPhase,
) -> anyhow::Result<CheckpointFile> {
    let phase_str = phase.as_str();

    let mut checkpoint_files = Vec::new();
    let mut entries = tokio::fs::read_dir(checkpoint_dir.as_ref()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.contains(phase_str) && name.ends_with(".json"))
            .unwrap_or(false)
        {
            checkpoint_files.push(path);
        }
    }

    if checkpoint_files.is_empty() {
        return Err(anyhow::anyhow!(
            "No checkpoint files found for phase: {phase_str}"
        ));
    }

    let checkpoint_content = tokio::fs::read_to_string(&checkpoint_files[0]).await?;
    let stored: StoredCheckpoint = serde_json::from_str(&checkpoint_content)?;

    let checkpoint: serde_json::Value = serde_json::from_str(&stored.checkpoint_data)?;
    let checkpoint_file = CheckpointFile {
        database_type: stored.database_type,
        checkpoint,
        phase,
        created_at: stored.created_at,
    };

    Ok(checkpoint_file)
}

/// Read the first checkpoint file from a directory containing checkpoint files.
pub async fn get_first_checkpoint_from_dir<P: AsRef<std::path::Path>>(
    checkpoint_dir: P,
) -> anyhow::Result<CheckpointFile> {
    let mut checkpoint_files = Vec::new();
    let mut entries = tokio::fs::read_dir(checkpoint_dir.as_ref()).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "json")
            .unwrap_or(false)
        {
            checkpoint_files.push(path);
        }
    }

    if checkpoint_files.is_empty() {
        return Err(anyhow::anyhow!("No checkpoint files found in directory"));
    }

    let checkpoint_content = tokio::fs::read_to_string(&checkpoint_files[0]).await?;
    let stored: StoredCheckpoint = serde_json::from_str(&checkpoint_content)?;

    let phase = parse_sync_phase(&stored.phase)?;

    let checkpoint: serde_json::Value = serde_json::from_str(&stored.checkpoint_data)?;
    let checkpoint_file = CheckpointFile {
        database_type: stored.database_type,
        checkpoint,
        phase,
        created_at: stored.created_at,
    };

    Ok(checkpoint_file)
}

fn parse_sync_phase(phase: &str) -> anyhow::Result<SyncPhase> {
    match phase {
        "full_sync_start" => Ok(SyncPhase::FullSyncStart),
        "full_sync_end" => Ok(SyncPhase::FullSyncEnd),
        "snapshot_progress" => Ok(SyncPhase::SnapshotProgress),
        "snapshot_handoff" => Ok(SyncPhase::SnapshotHandoff),
        "catch_up_progress" => Ok(SyncPhase::CatchUpProgress),
        // Dev fallback for checkpoint dirs written before the CatchUpProgress rename.
        "sync_handoff_metadata" => Ok(SyncPhase::CatchUpProgress),
        other => Err(anyhow::anyhow!("Unknown sync phase: {other}")),
    }
}
