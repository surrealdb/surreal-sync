//! MySQL incremental sync implementation for surreal-sync
//!
//! This module provides functionality to perform incremental database migration from MySQL
//! to SurrealDB.

use super::checkpoint::MySQLCheckpoint;
use super::source::IncrementalSource;
use crate::SourceOpts;
use anyhow::Result;
use checkpoint::Checkpoint;
use surreal_sink::SurrealSink;
use tracing::{debug, info, warn};

/// Run incremental sync from MySQL to SurrealDB
pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: MySQLCheckpoint,
    deadline: chrono::DateTime<chrono::Utc>,
    target_checkpoint: Option<MySQLCheckpoint>,
) -> Result<()> {
    info!(
        "Starting MySQL incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    // sequence_id is directly available from the checkpoint
    let sequence_id = from_checkpoint.sequence_id;

    // Create MySQL pool and incremental source
    let pool = super::client::new_mysql_pool(&from_opts.source_uri)?;
    let mut source = super::source::MySQLIncrementalSource::new(pool, sequence_id);

    // Initialize source (schema collection, connection check)
    source.initialize().await?;

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
                    if let Some(current_checkpoint) = stream.checkpoint() {
                        if current_checkpoint.sequence_id >= target.sequence_id {
                            info!(
                                "Reached target checkpoint: {}, stopping incremental sync",
                                target.to_cli_string()
                            );
                            break;
                        }
                    }
                }

                surreal.apply_universal_change(&change).await?;

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
