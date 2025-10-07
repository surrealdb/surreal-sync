//! MySQL checkpoint management
//!
//! This module provides utilities for obtaining and managing MySQL sequence-based checkpoints
//! for trigger-based incremental synchronization.

use crate::sync::SyncCheckpoint;
use anyhow::Result;
use chrono::Utc;
use mysql_async::prelude::Queryable;

/// Get current checkpoint for trigger-based MySQL sync
pub async fn get_current_checkpoint(conn: &mut mysql_async::Conn) -> Result<SyncCheckpoint> {
    // Get current max sequence_id from the audit table (should exist after setup)
    let result: Vec<mysql_async::Row> = conn
        .query("SELECT COALESCE(MAX(sequence_id), 0) FROM surreal_sync_changes")
        .await?;

    let current_sequence = result
        .first()
        .and_then(|row| row.get::<i64, _>(0))
        .unwrap_or(0);

    Ok(SyncCheckpoint::MySQL {
        sequence_id: current_sequence,
        timestamp: Utc::now(),
    })
}
