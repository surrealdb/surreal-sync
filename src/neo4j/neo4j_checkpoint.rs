//! Neo4j checkpoint management
//!
//! This module provides utilities for obtaining and managing Neo4j timestamp-based checkpoints
//! for incremental synchronization.

use crate::sync::SyncCheckpoint;
use chrono::Utc;

/// Get current Neo4j checkpoint with timestamp
pub fn get_current_checkpoint() -> SyncCheckpoint {
    SyncCheckpoint::Neo4j(Utc::now())
}
