//! PostgreSQL full sync implementation for surreal-sync
//!
//! This module provides functionality to perform full database migration from PostgreSQL
//! to SurrealDB, including support for checkpoint emission for incremental sync coordination.

use crate::{SourceOpts, SurrealOpts, SurrealValue};
use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio_postgres::{Client, NoTls, Row};
use tracing::{debug, info, warn};

/// Main entry point for PostgreSQL to SurrealDB migration with checkpoint support
pub async fn run_full_sync(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    sync_config: Option<crate::sync::SyncConfig>,
) -> Result<()> {
    info!("Starting PostgreSQL migration to SurrealDB");

    // Connect to PostgreSQL
    let (client, connection) = tokio_postgres::connect(&from_opts.source_uri, NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    // Emit checkpoint t1 (before full sync starts) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = crate::sync::SyncManager::new(config.clone());

        // Set up triggers on user tables so changes during full sync are captured
        let tables = super::autoconf::get_user_tables(
            &client,
            from_opts.source_database.as_deref().unwrap_or("public"),
        )
        .await?;
        let incremental_client =
            super::client::new_postgresql_client(&from_opts.source_uri).await?;
        let incremental_source =
            super::incremental_sync::PostgresIncrementalSource::new(incremental_client, 0);
        incremental_source.setup_tracking(tables).await?;

        let current_sequence = incremental_source.get_current_sequence().await?;

        let checkpoint = crate::sync::SyncCheckpoint::PostgreSQL {
            sequence_id: current_sequence, // Store current sequence from audit table
            timestamp: Utc::now(),
        };

        sync_manager
            .emit_checkpoint(&checkpoint, crate::sync::SyncPhase::FullSyncStart)
            .await?;

        info!(
            "Emitted full sync start checkpoint (t1): {}",
            checkpoint.to_string()
        );
    }

    // Connect to SurrealDB
    let surreal_endpoint = to_opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let surreal = surrealdb::engine::any::connect(surreal_endpoint).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;

    surreal.use_ns(&to_namespace).use_db(&to_database).await?;

    // Get database name from connection string or options
    let database_name = from_opts
        .source_database
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL database name is required"))?;

    // Get list of tables to migrate (excluding system tables)
    let tables = super::autoconf::get_user_tables(&client, database_name).await?;

    info!("Found {} tables to migrate", tables.len());

    let mut total_migrated = 0;

    // Migrate each table
    for table_name in &tables {
        info!("Migrating table: {}", table_name);

        let count = migrate_table(&client, &surreal, table_name, &to_opts).await?;

        total_migrated += count;
        info!("Migrated {} records from table {}", count, table_name);
    }

    // Emit checkpoint t2 (after full sync completes) if configured
    if let Some(ref config) = sync_config {
        let sync_manager = crate::sync::SyncManager::new(config.clone());

        // For trigger-based sync, checkpoint reflects the current state
        let checkpoint = crate::sync::SyncCheckpoint::PostgreSQL {
            sequence_id: 0, // Full sync complete, incremental will start from audit table
            timestamp: Utc::now(),
        };

        sync_manager
            .emit_checkpoint(&checkpoint, crate::sync::SyncPhase::FullSyncEnd)
            .await?;

        info!(
            "Emitted full sync end checkpoint (t2): {}",
            checkpoint.to_string()
        );
    }

    info!(
        "PostgreSQL migration completed: {} total records migrated",
        total_migrated
    );
    Ok(())
}

/// Migrate a single table from PostgreSQL to SurrealDB
pub async fn migrate_table(
    client: &Client,
    surreal: &surrealdb::Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    to_opts: &SurrealOpts,
) -> Result<usize> {
    // Get primary key column(s)
    let pk_columns = get_primary_key_columns(client, table_name).await?;

    // Query all data from the table
    let query = format!("SELECT * FROM {table_name}");
    log::info!("Full sync querying table {table_name} with: {query}");
    let rows = client.query(&query, &[]).await?;
    log::info!(
        "Full sync found {} rows in table {}",
        rows.len(),
        table_name
    );

    if rows.is_empty() {
        log::info!("Table {table_name} is empty, skipping");
        return Ok(0);
    }

    let mut batch = Vec::new();
    let mut total_processed = 0;

    for row in &rows {
        let record = convert_row_to_record(table_name, row, &pk_columns)?;
        batch.push(record);

        // Process batch when it reaches the configured size
        if batch.len() >= to_opts.batch_size {
            let batch_size = batch.len();

            if !to_opts.dry_run {
                crate::surreal::write_records(surreal, table_name, &batch).await?;
            } else {
                debug!(
                    "Dry-run: Would insert {} records into {}",
                    batch_size, table_name
                );
            }
            batch.clear();

            total_processed += batch_size;
        }
    }

    // Process remaining records
    if !batch.is_empty() {
        let batch_size = batch.len();

        if !to_opts.dry_run {
            crate::surreal::write_records(surreal, table_name, &batch).await?;
        } else {
            debug!(
                "Dry-run: Would insert {} records into {}",
                batch_size, table_name
            );
        }

        total_processed += batch_size;
    }

    Ok(total_processed)
}

/// Get primary key columns for a table
async fn get_primary_key_columns(client: &Client, table_name: &str) -> Result<Vec<String>> {
    let query = format!(
        "
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table_name}'::regclass
        AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
    "
    );

    let rows = client.query(&query, &[]).await?;

    if rows.is_empty() {
        Err(anyhow::anyhow!(
            "Table '{table_name}' has no primary key defined - primary key is required for sync operations",
        ))
    } else {
        Ok(rows.iter().map(|row| row.get::<_, String>(0)).collect())
    }
}

fn convert_row_to_record(
    table: &str,
    row: &Row,
    pk_columns: &[String],
) -> anyhow::Result<crate::Record> {
    let (id, data) = convert_row_to_keys_and_surreal_values(row, pk_columns)?;
    let id = surrealdb::sql::Thing::from((table, id));

    Ok(crate::Record { id, data })
}

/// Convert a PostgreSQL row to a map of surreal values
fn convert_row_to_keys_and_surreal_values(
    row: &Row,
    pk_columns: &[String],
) -> Result<(surrealdb::sql::Id, HashMap<String, SurrealValue>)> {
    let mut record = HashMap::new();

    // Generate ID from primary key columns
    let id = if pk_columns.is_empty() {
        return Err(anyhow::anyhow!(
            "Table has no primary key defined - primary key is required for sync"
        ));
    } else if pk_columns.len() == 1 {
        // Single primary key column - extract its value
        let pk_col = &pk_columns[0];
        let id = if let Ok(id) = row.try_get::<_, i64>(pk_col.as_str()) {
            surrealdb::sql::Id::from(id)
        } else if let Ok(id) = row.try_get::<_, i32>(pk_col.as_str()) {
            surrealdb::sql::Id::from(id)
        } else if let Ok(id) = row.try_get::<_, String>(pk_col.as_str()) {
            surrealdb::sql::Id::from(id)
        } else if let Ok(id) = row.try_get::<_, uuid::Uuid>(pk_col.as_str()) {
            let uuid = surrealdb::sql::Uuid::from(id);
            surrealdb::sql::Id::from(uuid)
        } else {
            return Err(anyhow::anyhow!(
                "Failed to extract primary key value from column '{pk_col}' - unsupported data type",
            ));
        };
        id
    } else {
        let mut vs = Vec::new();
        for col in pk_columns {
            let v = if let Ok(val) = row.try_get::<_, String>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else if let Ok(val) = row.try_get::<_, uuid::Uuid>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else if let Ok(val) = row.try_get::<_, i64>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else if let Ok(val) = row.try_get::<_, i32>(col.as_str()) {
                surrealdb::sql::Value::from(val)
            } else {
                return Err(anyhow::anyhow!(
                    "Failed to extract composite primary key value from column '{col}' - unsupported data type",
                ));
            };
            vs.push(v);
        }
        let ary = surrealdb::sql::Array::from(vs);
        surrealdb::sql::Id::from(ary)
    };

    // Convert all columns
    for (i, column) in row.columns().iter().enumerate() {
        let column_name = column.name();

        if pk_columns.contains(&column_name.to_string()) {
            // Skip primary key columns as they are already used in the ID
            continue;
        }

        let value = convert_postgres_value(row, i)?;
        record.insert(column_name.to_string(), value);
    }

    Ok((id, record))
}

/// Convert a PostgreSQL value to a SurrealValue
fn convert_postgres_value(row: &Row, index: usize) -> Result<SurrealValue> {
    use tokio_postgres::types::Type;

    let column = &row.columns()[index];
    let pg_type = column.type_();

    match *pg_type {
        Type::BOOL => match row.try_get::<_, Option<bool>>(index)? {
            Some(b) => Ok(SurrealValue::Bool(b)),
            None => Ok(SurrealValue::Null),
        },
        Type::INT2 => match row.try_get::<_, Option<i16>>(index)? {
            Some(i) => Ok(SurrealValue::Int(i as i64)),
            None => Ok(SurrealValue::Null),
        },
        Type::INT4 => match row.try_get::<_, Option<i32>>(index)? {
            Some(i) => Ok(SurrealValue::Int(i as i64)),
            None => Ok(SurrealValue::Null),
        },
        Type::INT8 => match row.try_get::<_, Option<i64>>(index)? {
            Some(i) => Ok(SurrealValue::Int(i)),
            None => Ok(SurrealValue::Null),
        },
        Type::FLOAT4 => match row.try_get::<_, Option<f32>>(index)? {
            Some(f) => Ok(SurrealValue::Float(f as f64)),
            None => Ok(SurrealValue::Null),
        },
        Type::FLOAT8 => match row.try_get::<_, Option<f64>>(index)? {
            Some(f) => Ok(SurrealValue::Float(f)),
            None => Ok(SurrealValue::Null),
        },
        Type::NUMERIC => {
            // PostgreSQL NUMERIC type - ALWAYS maps to SurrealValue::Decimal (deterministic)
            match row.try_get::<_, Option<Decimal>>(index) {
                Ok(Some(decimal)) => {
                    // Convert rust_decimal::Decimal to surrealdb::sql::Number (preserves precision)
                    let decimal_str = decimal.to_string();
                    match surrealdb::sql::Number::try_from(decimal_str.as_str()) {
                        Ok(surreal_num) => Ok(SurrealValue::Decimal(surreal_num)),
                        Err(e) => {
                            warn!(
                                "Failed to convert NUMERIC decimal '{}' to SurrealDB Number: {:?}",
                                decimal, e
                            );
                            Err(anyhow::anyhow!("NUMERIC conversion failed: {e:?}"))
                        }
                    }
                }
                Ok(None) => Ok(SurrealValue::Null),
                Err(e) => {
                    warn!(
                        "Failed to get PostgreSQL NUMERIC as rust_decimal::Decimal: {}",
                        e
                    );
                    Err(anyhow::anyhow!("NUMERIC type conversion failed: {e}"))
                }
            }
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            match row.try_get::<_, Option<String>>(index)? {
                Some(s) => Ok(SurrealValue::String(s)),
                None => Ok(SurrealValue::Null),
            }
        }
        Type::TIMESTAMP => match row.try_get::<_, Option<NaiveDateTime>>(index)? {
            Some(ts) => {
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc);
                Ok(SurrealValue::DateTime(dt))
            }
            None => Ok(SurrealValue::Null),
        },
        Type::TIMESTAMPTZ => match row.try_get::<_, Option<DateTime<Utc>>>(index)? {
            Some(dt) => Ok(SurrealValue::DateTime(dt)),
            None => Ok(SurrealValue::Null),
        },
        Type::DATE => match row.try_get::<_, Option<NaiveDate>>(index)? {
            Some(date) => {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("Invalid date"))?;
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                Ok(SurrealValue::DateTime(dt))
            }
            None => Ok(SurrealValue::Null),
        },
        Type::TIME => match row.try_get::<_, Option<NaiveTime>>(index)? {
            Some(time) => Ok(SurrealValue::String(time.to_string())),
            None => Ok(SurrealValue::Null),
        },
        Type::JSON | Type::JSONB => match row.try_get::<_, Option<serde_json::Value>>(index)? {
            Some(json) => crate::json_to_surreal_without_schema(json),
            None => Ok(SurrealValue::Null),
        },
        Type::UUID => match row.try_get::<_, Option<uuid::Uuid>>(index)? {
            Some(uuid) => Ok(SurrealValue::String(uuid.to_string())),
            None => Ok(SurrealValue::Null),
        },
        Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(index)? {
            Some(bytes) => Ok(SurrealValue::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                bytes,
            ))),
            None => Ok(SurrealValue::Null),
        },
        Type::TEXT_ARRAY => match row.try_get::<_, Option<Vec<String>>>(index)? {
            Some(arr) => {
                let vals = arr.into_iter().map(SurrealValue::String).collect();
                Ok(SurrealValue::Array(vals))
            }
            None => Ok(SurrealValue::Null),
        },
        Type::POINT => match row.try_get::<_, Option<geo_types::Point<f64>>>(index)? {
            Some(p) => Ok(SurrealValue::Geometry(surrealdb::sql::Geometry::Point(p))),
            None => Ok(SurrealValue::Null),
        },
        _ => {
            // For unknown types, try to get as string
            if let Ok(val) = row.try_get::<_, String>(index) {
                Ok(SurrealValue::String(val))
            } else {
                Err(anyhow::anyhow!("Unsupported PostgreSQL type: {pg_type:?}",))
            }
        }
    }
}
