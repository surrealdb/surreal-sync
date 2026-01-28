use crate::Change;
use std::collections::HashMap;
use std::time::Duration;
use surreal3_types::{RecordWithSurrealValues as Record, Relation, SurrealValue};
use surrealdb::types::RecordId;
use surrealdb::Surreal;
use sync_core::{UniversalChange, UniversalChangeOp};
use tokio::time::sleep;

use crate::rows::universal_value_to_surreal_id;

/// Maximum number of retries for retriable transaction errors
const MAX_RETRIES: u32 = 5;
/// Base delay between retries (will be multiplied by retry attempt for backoff)
const RETRY_BASE_DELAY_MS: u64 = 100;

/// Check if an error is a retriable transaction conflict
fn is_retriable_transaction_error(error: &surrealdb::Error) -> bool {
    let error_str = error.to_string();
    error_str.contains("This transaction can be retried")
        || error_str.contains("Failed to commit transaction due to a read or write conflict")
}

// Apply a single change event to SurrealDB
pub async fn apply_change(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    change: &Change,
) -> anyhow::Result<()> {
    match change {
        Change::UpsertRecord(record) => {
            write_record(surreal, record).await?;

            tracing::trace!("Successfully upserted record: {record:?}");
        }
        Change::DeleteRecord(record_id) => {
            let query = "DELETE $record_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);
            log::info!("🔧 migrate_change executing: {query} for record: {record_id:?}");

            let mut q = surreal.query(query);
            q = q.bind(("record_id", record_id.clone()));

            q.await?;

            tracing::trace!("Successfully deleted record: {:?}", record_id);
        }
        Change::UpsertRelation(relation) => {
            write_relation(surreal, relation).await?;

            tracing::trace!("Successfully upserted relation: {relation:?}");
        }
        Change::DeleteRelation(record_id) => {
            let query = "DELETE $relation_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);
            log::info!("🔧 migrate_change executing: {query} for relation: {record_id:?}");

            let mut q = surreal.query(query);
            q = q.bind(("relation_id", record_id.clone()));
            q.await?;
            tracing::trace!("Successfully deleted relation: {record_id:?}");
        }
    }

    tracing::debug!("Successfully applied {change:?}");

    Ok(())
}

/// Apply a UniversalChange event to SurrealDB.
///
/// Converts UniversalChange to the appropriate SurrealDB operation and executes it.
pub async fn apply_universal_change(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    change: &UniversalChange,
) -> anyhow::Result<()> {
    // Convert ID from UniversalValue to SurrealDB ID
    let surreal_id = universal_value_to_surreal_id(&change.id)?;
    let record_id = RecordId::new(change.table.as_str(), surreal_id);

    match change.operation {
        UniversalChangeOp::Create | UniversalChangeOp::Update => {
            // Convert data from HashMap<String, UniversalValue> to HashMap<String, surrealdb::types::Value>
            let data = change.data.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Create/Update change must have data, but found None for table '{}'",
                    change.table
                )
            })?;

            let surreal_data: HashMap<String, surrealdb::types::Value> = data
                .iter()
                .map(|(k, v)| {
                    let sv = SurrealValue::from(v.clone());
                    (k.clone(), sv.into_inner())
                })
                .collect();

            let record = Record::new(record_id.clone(), surreal_data);
            write_record(surreal, &record).await?;

            tracing::trace!("Successfully upserted record: {record_id:?}");
        }
        UniversalChangeOp::Delete => {
            let query = "DELETE $record_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);

            let mut q = surreal.query(query);
            q = q.bind(("record_id", record_id.clone()));
            q.await?;

            tracing::trace!("Successfully deleted record: {record_id:?}");
        }
    }

    tracing::debug!("Successfully applied universal change for {}", change.table);

    Ok(())
}

// Write a single record to SurrealDB using UPSERT with retry for transaction conflicts
pub async fn write_record(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    document: &Record,
) -> anyhow::Result<()> {
    let upsert_content = document.get_upsert_content();
    let record_id = &document.id;

    // Build parameterized query using proper variable binding to prevent injection
    let query = "UPSERT $record_id CONTENT $content".to_string();

    tracing::trace!("Executing SurrealDB query with flattened fields: {}", query);

    log::info!("🔧 migrate_batch executing: {query} for record: {record_id:?}");

    // Add debug logging to see the document being bound
    if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
        tracing::debug!("Binding document to SurrealDB query for record {document:?}",);
    }

    // Retry loop for handling transaction conflicts
    let mut last_error: Option<surrealdb::Error> = None;
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            let delay_ms = RETRY_BASE_DELAY_MS * (1 << (attempt - 1).min(4)); // Exponential backoff, max 1.6s
            tracing::warn!(
                "Retrying write_record for {:?} (attempt {}/{}), waiting {}ms",
                record_id,
                attempt,
                MAX_RETRIES,
                delay_ms
            );
            sleep(Duration::from_millis(delay_ms)).await;
        }

        // Build query with proper parameter binding
        let mut q = surreal.query(query.clone());
        q = q.bind(("record_id", record_id.clone()));
        q = q.bind(("content", upsert_content.clone()));

        let response_result: Result<surrealdb::IndexedResults, surrealdb::Error> = q.await;

        match response_result {
            Ok(mut response) => {
                // In SurrealDB v3, Thing is now RecordId
                let result: Result<Vec<surrealdb::types::RecordId>, surrealdb::Error> =
                    response.take("id").map_err(|e| {
                        tracing::error!(
                            "SurrealDB response.take() failed for record {:?}: {}",
                            record_id,
                            e
                        );
                        e
                    });

                match result {
                    Ok(res) => {
                        if res.is_empty() {
                            tracing::warn!("Failed to create record: {:?}", record_id);
                        } else {
                            tracing::trace!("Successfully created record: {:?}", record_id);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        if is_retriable_transaction_error(&e) {
                            tracing::warn!(
                                "Retriable transaction error for record {:?}: {}",
                                record_id,
                                e
                            );
                            last_error = Some(e);
                            continue;
                        }
                        tracing::error!("Error creating record {:?}: {}", record_id, e);
                        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                            tracing::error!("Problematic document: {:?}", document);
                        }
                        return Err(e.into());
                    }
                }
            }
            Err(e) => {
                if is_retriable_transaction_error(&e) {
                    tracing::warn!(
                        "Retriable transaction error for record {:?}: {}",
                        record_id,
                        e
                    );
                    last_error = Some(e);
                    continue;
                }
                tracing::error!(
                    "SurrealDB query execution failed for record {:?}: {}",
                    record_id,
                    e
                );
                if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                    tracing::error!("Failed query content: {upsert_content:?}");
                }
                return Err(e.into());
            }
        }
    }

    // All retries exhausted
    let error_msg =
        format!("Failed to write record {record_id:?} after {MAX_RETRIES} retries. Last error: {last_error:?}");
    tracing::error!("{error_msg}");
    Err(anyhow::anyhow!(error_msg))
}

// Write a batch of records to SurrealDB using UPSERT
pub async fn write_records(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[Record],
) -> anyhow::Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for (i, r) in batch.iter().enumerate() {
        tracing::trace!("Processing record {}/{}", i + 1, batch.len());
        write_record(surreal, r).await?;
    }

    tracing::debug!(
        "Completed migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}

pub async fn write_relation(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    r: &Relation,
) -> anyhow::Result<()> {
    // Build parameterized query using proper variable binding to prevent injection
    // In v3, RecordId uses .table instead of .tb
    // In v3, RecordId uses .table field instead of .tb
    let query = format!("RELATE $in->{}->$out CONTENT $content", r.id.table);

    let record_id = &r.id;

    tracing::trace!("Executing SurrealDB query with flattened fields: {}", query);
    log::info!("🔧 migrate_batch executing: {query} for record: {record_id:?}");

    // Add debug logging to see the document being bound
    if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
        tracing::debug!(
            "Binding document to SurrealDB query for record {:?}: {:?}",
            record_id,
            r
        );
    }

    // Retry loop for handling transaction conflicts
    let mut last_error: Option<surrealdb::Error> = None;
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            let delay_ms = RETRY_BASE_DELAY_MS * (1 << (attempt - 1).min(4)); // Exponential backoff, max 1.6s
            tracing::warn!(
                "Retrying write_relation for {:?} (attempt {}/{}), waiting {}ms",
                record_id,
                attempt,
                MAX_RETRIES,
                delay_ms
            );
            sleep(Duration::from_millis(delay_ms)).await;
        }

        // Build query with proper parameter binding
        let mut q = surreal.query(query.clone());
        q = q.bind(("in", r.get_in()));
        q = q.bind(("out", r.get_out()));
        q = q.bind(("content", r.get_relate_content()));

        let response_result: Result<surrealdb::IndexedResults, surrealdb::Error> = q.await;

        match response_result {
            Ok(mut response) => {
                // In SurrealDB v3, Thing is now RecordId
                let result: Result<Vec<surrealdb::types::RecordId>, surrealdb::Error> =
                    response.take("id").map_err(|e| {
                        tracing::error!(
                            "SurrealDB response.take() failed for record {:?}: {}",
                            record_id,
                            e
                        );
                        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                            tracing::error!(
                                "Response take error content: {:?}",
                                r.get_relate_content()
                            );
                        }
                        e
                    });

                match result {
                    Ok(res) => {
                        if res.is_empty() {
                            tracing::warn!("Failed to create record: {:?}", record_id);
                        } else {
                            tracing::trace!("Successfully created record: {:?}", record_id);
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        if is_retriable_transaction_error(&e) {
                            tracing::warn!(
                                "Retriable transaction error for relation {:?}: {}",
                                record_id,
                                e
                            );
                            last_error = Some(e);
                            continue;
                        }
                        tracing::error!("Error creating record {:?}: {}", record_id, e);
                        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                            tracing::error!("Problematic document: {:?}", r);
                        }
                        return Err(e.into());
                    }
                }
            }
            Err(e) => {
                if is_retriable_transaction_error(&e) {
                    tracing::warn!(
                        "Retriable transaction error for relation {:?}: {}",
                        record_id,
                        e
                    );
                    last_error = Some(e);
                    continue;
                }
                tracing::error!(
                    "SurrealDB query execution failed for record {:?}: {}",
                    record_id,
                    e
                );
                if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                    tracing::error!("Failed query content: {:?}", r.get_relate_content());
                }
                return Err(e.into());
            }
        }
    }

    // All retries exhausted
    let error_msg =
        format!("Failed to write relation {record_id:?} after {MAX_RETRIES} retries. Last error: {last_error:?}");
    tracing::error!("{error_msg}");
    Err(anyhow::anyhow!(error_msg))
}

pub async fn write_relations(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[Relation],
) -> anyhow::Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for (i, r) in batch.iter().enumerate() {
        tracing::trace!("Processing record {}/{}", i + 1, batch.len());
        write_relation(surreal, r).await?;
    }

    tracing::debug!(
        "Completed migrating relations for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}
