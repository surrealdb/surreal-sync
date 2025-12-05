//! SurrealDB utilities for CSV import

use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use surrealdb::sql::Thing;
use surrealdb::Surreal;

/// Represents a value that can be written to SurrealDB
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum SurrealValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<SurrealValue>),
    Object(HashMap<String, SurrealValue>),
    Uuid(uuid::Uuid),
    DateTime(chrono::DateTime<chrono::Utc>),
    Null,
}

/// A record to be written to SurrealDB
#[derive(Debug, Clone)]
pub struct Record {
    pub id: Thing,
    pub data: HashMap<String, SurrealValue>,
}

impl Record {
    fn get_upsert_content(&self) -> surrealdb::sql::Value {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            let sql_value = surreal_value_to_sql(v);
            m.insert(k.clone(), sql_value);
        }
        surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
    }
}

/// Convert a SurrealValue to surrealdb::sql::Value
fn surreal_value_to_sql(v: &SurrealValue) -> surrealdb::sql::Value {
    match v {
        SurrealValue::Bool(b) => surrealdb::sql::Value::Bool(*b),
        SurrealValue::Int(i) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*i)),
        SurrealValue::Float(f) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*f)),
        SurrealValue::String(s) => {
            surrealdb::sql::Value::Strand(surrealdb::sql::Strand::from(s.clone()))
        }
        SurrealValue::Array(arr) => {
            let sql_arr: Vec<surrealdb::sql::Value> =
                arr.iter().map(surreal_value_to_sql).collect();
            surrealdb::sql::Value::Array(surrealdb::sql::Array::from(sql_arr))
        }
        SurrealValue::Object(obj) => {
            let mut m = std::collections::BTreeMap::new();
            for (k, v) in obj {
                m.insert(k.clone(), surreal_value_to_sql(v));
            }
            surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
        }
        SurrealValue::Uuid(u) => surrealdb::sql::Value::Uuid(surrealdb::sql::Uuid::from(*u)),
        SurrealValue::DateTime(dt) => {
            surrealdb::sql::Value::Datetime(surrealdb::sql::Datetime::from(*dt))
        }
        SurrealValue::Null => surrealdb::sql::Value::Null,
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
