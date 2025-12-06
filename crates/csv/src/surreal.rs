//! SurrealDB utilities for CSV import

use anyhow::Result;
use std::collections::HashMap;
use surrealdb::sql::Thing;
use surrealdb::Surreal;
use surrealdb_types::SurrealValue;
use sync_core::TypedValue;

/// A record to be written to SurrealDB
#[derive(Debug, Clone)]
pub struct Record {
    pub id: Thing,
    pub data: HashMap<String, TypedValue>,
}

impl Record {
    fn get_upsert_content(&self) -> surrealdb::sql::Value {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            let sql_value: surrealdb::sql::Value = SurrealValue::from(v.clone()).into_inner();
            m.insert(k.clone(), sql_value);
        }
        surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
    }
}

/// SurrealDB connection options
#[derive(Clone, Debug)]
pub struct SurrealOpts {
    pub surreal_endpoint: String,
    pub surreal_username: String,
    pub surreal_password: String,
}

/// Connect to SurrealDB
pub async fn surreal_connect(
    opts: &SurrealOpts,
    ns: &str,
    db: &str,
) -> Result<Surreal<surrealdb::engine::any::Any>> {
    let surreal_endpoint = opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let surreal = surrealdb::engine::any::connect(surreal_endpoint).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &opts.surreal_username,
            password: &opts.surreal_password,
        })
        .await?;

    surreal.use_ns(ns).use_db(db).await?;

    Ok(surreal)
}

/// Write a single record to SurrealDB
async fn write_record(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    document: &Record,
) -> Result<()> {
    let upsert_content = document.get_upsert_content();
    let record_id = &document.id;

    let query = "UPSERT $record_id CONTENT $content".to_string();

    let mut q = surreal.query(query);
    q = q.bind(("record_id", record_id.clone()));
    q = q.bind(("content", upsert_content.clone()));

    let mut response: surrealdb::Response = q.await?;
    let _result: Result<Vec<Thing>, surrealdb::Error> = response.take("id");

    Ok(())
}

/// Write a batch of records to SurrealDB
pub async fn write_records(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[Record],
) -> Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for r in batch.iter() {
        write_record(surreal, r).await?;
    }

    tracing::debug!(
        "Completed migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}
