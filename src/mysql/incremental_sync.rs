//! MySQL incremental sync implementation for surreal-sync
//!
//! This module provides functionality to perform incremental database migration from MySQL
//! to SurrealDB.

use crate::surreal::surreal_connect;
use crate::{SourceOpts, SurrealOpts};
use anyhow::Result;
use tracing::{debug, info, warn};

/// Run incremental sync from MySQL to SurrealDB
pub async fn run_incremental_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    from_checkpoint: crate::sync::SyncCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    target_checkpoint: Option<crate::sync::SyncCheckpoint>,
) -> Result<()> {
    info!(
        "Starting MySQL incremental sync from checkpoint: {}",
        from_checkpoint.to_string()
    );

    // Extract sequence_id from checkpoint
    let sequence_id = from_checkpoint.to_mysql_sequence_id()?;

    // Create MySQL pool and incremental source
    let pool = super::client::new_mysql_pool(&from_opts.source_uri)?;
    let mut source = super::source::MySQLIncrementalSource::new(pool, sequence_id);

    // Initialize source (schema collection, connection check)
    source.initialize().await?;

    let surreal = surreal_connect(&to_opts, &to_namespace, &to_database).await?;

    // Get change stream
    let mut stream = source.get_changes().await?;

    info!("Starting to consume MySQL change stream...");

    let mut change_count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(change) => {
                debug!("Received change: {:?}", change);

                // Check if we've reached the deadline
                if chrono::Utc::now() >= deadline {
                    info!("Reached deadline: {deadline}, stopping incremental sync");
                    break;
                }

                // Check if we've reached the target checkpoint
                if let Some(ref target) = target_checkpoint {
                    // Compare checkpoints based on type
                    if let (Some(current), target) = (stream.checkpoint(), target) {
                        let reached = match (current, target) {
                            (
                                crate::sync::SyncCheckpoint::MySQL {
                                    sequence_id: current_seq,
                                    ..
                                },
                                crate::sync::SyncCheckpoint::MySQL {
                                    sequence_id: target_seq,
                                    ..
                                },
                            ) => current_seq >= *target_seq,
                            _ => false,
                        };

                        if reached {
                            info!(
                                "Reached target checkpoint: {}, stopping incremental sync",
                                target.to_string()
                            );
                            break;
                        }
                    }
                }

                crate::apply_change(&surreal, &change).await?;

                change_count += 1;
                if change_count % 100 == 0 {
                    info!("Processed {} changes", change_count);
                }
            }
            Err(e) => {
                warn!("Error reading change stream: {}", e);
            }
        }
    }

    info!(
        "MySQL incremental sync completed. Processed {} changes",
        change_count
    );

    // Cleanup
    source.cleanup().await?;

    Ok(())
}

use crate::sync::IncrementalSource;
