// ! SurrealDB utilities for JSONL import

use anyhow::Result;
use std::collections::HashMap;
use surrealdb::sql::Thing;
use surrealdb::Surreal;

/// Represents a value that can be written to SurrealDB
#[derive(Debug, Clone, PartialEq)]
pub enum SurrealValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<SurrealValue>),
    Object(HashMap<String, SurrealValue>),
    Thing(Thing),
    Uuid(uuid::Uuid),
    DateTime(chrono::DateTime<chrono::Utc>),
    Null,
}

impl SurrealValue {
    /// Convert SurrealValue to SurrealDB Value for query binding
    pub fn to_surrealql_value(&self) -> surrealdb::sql::Value {
        match self {
            SurrealValue::Bool(b) => surrealdb::sql::Value::Bool(*b),
            SurrealValue::Int(i) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*i)),
            SurrealValue::Float(f) => {
                surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*f))
            }
            SurrealValue::String(s) => {
                surrealdb::sql::Value::Strand(surrealdb::sql::Strand::from(s.clone()))
            }
            SurrealValue::Array(vs) => {
                let mut arr = Vec::new();
                for item in vs {
                    arr.push(item.to_surrealql_value());
                }
                surrealdb::sql::Value::Array(surrealdb::sql::Array::from(arr))
            }
            SurrealValue::Object(obj) => {
                let mut map = std::collections::BTreeMap::new();
                for (key, value) in obj {
                    map.insert(key.clone(), value.to_surrealql_value());
                }
                surrealdb::sql::Value::Object(surrealdb::sql::Object::from(map))
            }
            SurrealValue::Thing(t) => surrealdb::sql::Value::Thing(t.clone()),
            SurrealValue::Uuid(u) => surrealdb::sql::Value::Uuid(surrealdb::sql::Uuid::from(*u)),
            SurrealValue::DateTime(dt) => {
                surrealdb::sql::Value::Datetime(surrealdb::sql::Datetime::from(*dt))
            }
            SurrealValue::Null => surrealdb::sql::Value::Null,
        }
    }
}

/// A record to be written to SurrealDB
#[derive(Debug, Clone)]
pub struct Record {
    pub id: Thing,
    pub data: HashMap<String, SurrealValue>,
}

impl Record {
    pub fn get_upsert_content(&self) -> surrealdb::sql::Value {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            m.insert(k.clone(), v.to_surrealql_value());
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
