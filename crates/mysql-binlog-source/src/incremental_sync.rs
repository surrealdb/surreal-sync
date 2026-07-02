//! MySQL binlog incremental sync (apply then commit).

use std::collections::HashMap;

use anyhow::Result;
use binlog_protocol::{CdcChange, EventBody, TableMapEvent};
use checkpoint::Checkpoint;
use chrono::{DateTime, Utc};
use surreal_sink::SurrealSink;
use tracing::{debug, info};

use crate::change::cdc_change_to_universal;
use crate::checkpoint::BinlogCheckpoint;
use crate::client::{
    connect_binlog_client, new_mysql_pool, resolve_database, start_binlog_from_checkpoint,
    use_database,
};
use crate::schema::{collect_mysql_database_schema, get_table_column_names_ordinal};
use crate::SourceOpts;

pub async fn run_incremental_sync<S: SurrealSink>(
    surreal: &S,
    from_opts: SourceOpts,
    from_checkpoint: BinlogCheckpoint,
    deadline: DateTime<Utc>,
    to_checkpoint: Option<BinlogCheckpoint>,
) -> Result<()> {
    info!(
        "Starting MySQL binlog incremental sync from checkpoint: {}",
        from_checkpoint.to_cli_string()
    );

    let pool = new_mysql_pool(&from_opts.connection_string)?;
    let database = resolve_database(&pool, &from_opts).await?;
    let mut conn = pool.get_conn().await?;
    use_database(&mut conn, &database).await?;

    let schema = collect_mysql_database_schema(&mut conn).await?;
    let json_columns =
        surreal_sync_mysql_trigger_source::json_columns::get_json_columns(&mut conn, &database)
            .await?;

    let mut column_names_cache: HashMap<String, Vec<String>> = HashMap::new();
    let table_filter: Option<Vec<String>> = if from_opts.tables.is_empty() {
        None
    } else {
        Some(from_opts.tables.clone())
    };

    let mut client = connect_binlog_client(&from_opts).await?;
    start_binlog_from_checkpoint(&mut client, &from_checkpoint).await?;

    let mut table_maps: HashMap<u64, TableMapEvent> = HashMap::new();
    let mut total_changes = 0u64;
    let mut idle_polls = 0u32;

    loop {
        if Utc::now() >= deadline {
            info!("Deadline reached, stopping incremental sync");
            break;
        }

        let events = client
            .next_events(32)
            .await
            .map_err(|e| anyhow::anyhow!("binlog read failed: {e}"))?;

        if events.is_empty() {
            idle_polls += 1;
            if idle_polls >= 100 && total_changes > 0 {
                info!("No binlog events after {total_changes} changes; stopping incremental sync");
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }
        idle_polls = 0;

        for event in events {
            match event.body {
                EventBody::TableMap(tm) => {
                    table_maps.insert(tm.table_id, tm);
                }
                EventBody::Rows(rows) => {
                    let Some(table_map) = table_maps.get(&rows.table_id).cloned() else {
                        continue;
                    };
                    if table_map.database != database {
                        continue;
                    }
                    if let Some(ref tables) = table_filter {
                        if !tables.contains(&table_map.table) {
                            continue;
                        }
                    }

                    for row_change in rows.rows {
                        let position = client.current_position();
                        if let Some(ref target) = to_checkpoint {
                            if position >= target.position {
                                info!("Reached target checkpoint, stopping incremental sync");
                                pool.disconnect().await?;
                                return Ok(());
                            }
                        }

                        let change = CdcChange {
                            position: position.clone(),
                            database: table_map.database.clone(),
                            table: table_map.table.clone(),
                            operation: row_change,
                            xid: None,
                            gtid: None,
                        };

                        let column_names = if let Some(names) =
                            column_names_cache.get(&change.table)
                        {
                            names.clone()
                        } else {
                            let names =
                                get_table_column_names_ordinal(&mut conn, &change.table).await?;
                            column_names_cache.insert(change.table.clone(), names.clone());
                            names
                        };

                        let universal = cdc_change_to_universal(
                            &change,
                            &table_map,
                            &column_names,
                            &schema,
                            &json_columns,
                        )?;

                        surreal.apply_universal_change(&universal).await?;
                        client.commit(position);
                        total_changes += 1;

                        if total_changes.is_multiple_of(100) {
                            debug!("Processed {total_changes} binlog changes");
                        }
                    }
                }
                _ => {}
            }
        }
    }

    info!("MySQL binlog incremental sync completed: {total_changes} changes applied");
    drop(conn);
    pool.disconnect().await?;
    Ok(())
}
