//! SurrealDB utilities for JSONL import

use anyhow::Result;
use std::collections::HashMap;
use surrealdb::sql::Thing;
use surrealdb::Surreal;
use surrealdb_types::SurrealValue;
use sync_core::TypedValue;

/// A value that can be stored in a JSONL record field.
///
/// This enum allows us to handle both regular TypedValues and SurrealDB Thing
/// references (created via ConversionRules).
#[derive(Debug, Clone)]
pub enum FieldValue {
    /// A typed value that will be converted via surrealdb_types
    Typed(TypedValue),
    /// A SurrealDB Thing reference (record link)
    Thing(Thing),
}

impl From<TypedValue> for FieldValue {
    fn from(tv: TypedValue) -> Self {
        FieldValue::Typed(tv)
    }
}

impl From<Thing> for FieldValue {
    fn from(thing: Thing) -> Self {
        FieldValue::Thing(thing)
    }
}

/// A record to be written to SurrealDB
#[derive(Debug, Clone)]
pub struct Record {
    pub id: Thing,
    pub data: HashMap<String, FieldValue>,
}

impl Record {
    fn get_upsert_content(&self) -> surrealdb::sql::Value {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            let sql_value: surrealdb::sql::Value = match v {
                FieldValue::Typed(tv) => SurrealValue::from(tv.clone()).into_inner(),
                FieldValue::Thing(thing) => surrealdb::sql::Value::Thing(thing.clone()),
            };
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
    pub batch_size: usize,
    pub dry_run: bool,
}

/// Source database connection options
#[derive(Clone, Debug)]
pub struct SourceOpts {
    pub source_uri: String,
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

/// Write multiple records to SurrealDB in a batch
pub async fn write_records(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table: &str,
    documents: &[Record],
) -> Result<()> {
    tracing::debug!("Writing {} records to table: {}", documents.len(), table);

    for document in documents {
        write_record(surreal, document).await?;
    }

    Ok(())
}
