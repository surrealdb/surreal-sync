use super::{Change, Record, Relation};
use surrealdb::Surreal;

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
        Change::DeleteRecord(thing) => {
            let query = "DELETE $record_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);
            log::info!("🔧 migrate_change executing: {query} for record: {thing:?}");

            let mut q = surreal.query(query);
            q = q.bind(("record_id", thing.clone()));

            q.await?;

            tracing::trace!("Successfully deleted record: {:?}", thing);
        }
        Change::UpsertRelation(relation) => {
            write_relation(surreal, relation).await?;

            tracing::trace!("Successfully upserted relation: {relation:?}");
        }
        Change::DeleteRelation(thing) => {
            let query = "DELETE $relation_id".to_string();
            tracing::trace!("Executing SurrealDB query: {}", query);
            log::info!("🔧 migrate_change executing: {query} for relation: {thing:?}");

            let mut q = surreal.query(query);
            q = q.bind(("relation_id", thing.clone()));
            q.await?;
            tracing::trace!("Successfully deleted relation: {thing:?}");
        }
    }

    tracing::debug!("Successfully applied {change:?}");

    Ok(())
}

// Write a single record to SurrealDB using UPSERT
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

    // Build query with proper parameter binding
    let mut q = surreal.query(query);
    q = q.bind(("record_id", record_id.clone()));
    q = q.bind(("content", upsert_content.clone()));

    let mut response: surrealdb::Response = q.await.map_err(|e| {
        tracing::error!(
            "SurrealDB query execution failed for record {:?}: {}",
            record_id,
            e
        );
        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
            tracing::error!("Failed query content: {upsert_content:?}");
        }
        e
    })?;

    let result: Result<Vec<surrealdb::sql::Thing>, surrealdb::Error> =
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
        }
        Err(e) => {
            tracing::error!("Error creating record {:?}: {}", record_id, e);
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::error!("Problematic document: {:?}", document);
            }
            return Err(e.into());
        }
    }

    Ok(())
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

async fn write_relation(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    r: &Relation,
) -> anyhow::Result<()> {
    // Build parameterized query using proper variable binding to prevent injection
    let query = format!("RELATE $in->{}->$out CONTENT $content", r.id.tb);

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

    // Build query with proper parameter binding
    let mut q = surreal.query(query);
    q = q.bind(("in", r.get_in()));
    q = q.bind(("out", r.get_out()));
    q = q.bind(("content", r.get_relate_content()));

    let mut response: surrealdb::Response = q.await.map_err(|e| {
        tracing::error!(
            "SurrealDB query execution failed for record {:?}: {}",
            record_id,
            e
        );
        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
            tracing::error!("Failed query content: {:?}", r.get_relate_content());
        }
        e
    })?;

    let result: Result<Vec<surrealdb::sql::Thing>, surrealdb::Error> =
        response.take("id").map_err(|e| {
            tracing::error!(
                "SurrealDB response.take() failed for record {:?}: {}",
                record_id,
                e
            );
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::error!("Response take error content: {:?}", r.get_relate_content());
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
        }
        Err(e) => {
            tracing::error!("Error creating record {:?}: {}", record_id, e);
            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::error!("Problematic document: {:?}", r);
            }
            return Err(e.into());
        }
    }

    Ok(())
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
