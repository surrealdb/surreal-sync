//! SurrealSync Library
//!
//! A library for migrating data from Neo4j and MongoDB databases to SurrealDB.

use chrono::{DateTime, Utc};
use clap::Parser;
use std::collections::HashMap;
use surrealdb::{RecordId, Surreal};

pub mod jsonl;
pub mod mongodb;
pub mod neo4j;

#[derive(Parser)]
pub struct SourceOpts {
    /// Source database connection string/URI
    #[arg(long, env = "SOURCE_URI")]
    pub source_uri: String,

    /// Source database name
    #[arg(long, env = "SOURCE_DATABASE")]
    pub source_database: Option<String>,

    /// Source database username
    #[arg(long, env = "SOURCE_USERNAME")]
    pub source_username: Option<String>,

    /// Source database password
    #[arg(long, env = "SOURCE_PASSWORD")]
    pub source_password: Option<String>,

    /// Timezone to use for converting Neo4j local datetime and time values
    /// Format: IANA timezone name (e.g., "America/New_York", "Europe/London", "UTC")
    /// Default: "UTC"
    #[arg(long, default_value = "UTC", env = "NEO4J_TIMEZONE")]
    pub neo4j_timezone: String,
}

#[derive(Parser)]
pub struct SurrealOpts {
    /// SurrealDB endpoint URL
    #[arg(
        long,
        default_value = "http://localhost:8000",
        env = "SURREAL_ENDPOINT"
    )]
    pub surreal_endpoint: String,

    /// SurrealDB username
    #[arg(long, default_value = "root", env = "SURREAL_USERNAME")]
    pub surreal_username: String,

    /// SurrealDB password
    #[arg(long, default_value = "root", env = "SURREAL_PASSWORD")]
    pub surreal_password: String,

    /// Batch size for data migration
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Dry run mode - don't actually write data
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn migrate_from_mongodb(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
) -> anyhow::Result<()> {
    mongodb::migrate_from_mongodb(from_opts, to_namespace, to_database, to_opts).await
}

pub async fn migrate_batch(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    table_name: &str,
    batch: &[(String, std::collections::HashMap<String, BindableValue>)],
) -> anyhow::Result<()> {
    tracing::debug!(
        "Starting migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );

    for (i, (record_id, document)) in batch.iter().enumerate() {
        tracing::trace!("Processing record {}/{}: {}", i + 1, batch.len(), record_id);

        let is_relation = document.get("__is_relation__").is_some();
        // Extract fields from the bindable document
        let fields = document;
        let record: RecordId = record_id.parse()?;

        // Build flattened field list for the query
        let mut field_bindings: Vec<String> = fields
            .keys()
            .filter(|&key| key != "__is_relation__")
            .map(|key| format!("{key}: ${key}"))
            .collect();
        if is_relation {
            field_bindings.push(format!("id: {}:{}", record.table(), record.key()));
        }
        let content_fields = format!("{{{}}}", field_bindings.join(", "));

        let query = if is_relation {
            format!("INSERT RELATION INTO {} {}", record.table(), content_fields)
        } else {
            format!("CREATE {record_id} CONTENT {content_fields}")
        };

        tracing::trace!("Executing SurrealDB query with flattened fields: {}", query);

        // Add debug logging to see the document being bound
        if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
            tracing::debug!(
                "Binding document to SurrealDB query for record {}: {:?}",
                record_id,
                document
            );
        }

        // Build query with individual field bindings
        let mut q = surreal.query(query);
        for (field_name, field_value) in fields {
            tracing::debug!("Binding field: {} to value: {:?}", field_name, field_value);
            q = match field_value {
                BindableValue::Bool(b) => q.bind((field_name.clone(), *b)),
                BindableValue::Int(i) => q.bind((field_name.clone(), *i)),
                BindableValue::Float(f) => q.bind((field_name.clone(), *f)),
                BindableValue::String(s) => q.bind((field_name.clone(), s.clone())),
                BindableValue::DateTime(dt) => q.bind((field_name.clone(), *dt)),
                BindableValue::Array(bindables) => {
                    let mut arr = Vec::new();
                    for item in bindables {
                        let v = bindable_to_surrealdb_value(item);
                        arr.push(v);
                    }
                    let surreal_arr = surrealdb::sql::Array::from(arr);
                    q.bind((field_name.clone(), surreal_arr))
                }
                BindableValue::Object(obj) => {
                    let mut map = std::collections::BTreeMap::new();
                    for (key, value) in obj {
                        let v = bindable_to_surrealdb_value(value);
                        map.insert(key.clone(), v);
                    }
                    let surreal_obj = surrealdb::sql::Object::from(map);
                    q.bind((field_name.clone(), surreal_obj))
                }
                BindableValue::Duration(d) => q.bind((field_name.clone(), *d)),
                BindableValue::Bytes(b) => {
                    q.bind((field_name.clone(), surrealdb::sql::Bytes::from(b.clone())))
                }
                BindableValue::Decimal(d) => q.bind((field_name.clone(), *d)),
                BindableValue::Thing(t) => q.bind((field_name.clone(), t.clone())),
                BindableValue::Geometry(g) => q.bind((field_name.clone(), g.clone())),
                BindableValue::Null => q.bind((field_name.clone(), surrealdb::sql::Value::Null)),
                BindableValue::None => q.bind((field_name.clone(), surrealdb::sql::Value::None)),
            };
        }

        let mut response: surrealdb::Response = q.await?;
        let result: Result<Vec<surrealdb::sql::Thing>, surrealdb::Error> = response.take("id");

        match result {
            Ok(res) => {
                if res.is_empty() {
                    tracing::warn!("Failed to create record: {}", record_id);
                } else {
                    tracing::trace!("Successfully created record: {}", record_id);
                }
            }
            Err(e) => {
                tracing::error!("Error creating record {}: {}", record_id, e);
                if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                    tracing::error!("Problematic document: {:?}", document);
                }
                return Err(e.into());
            }
        }
    }

    tracing::debug!(
        "Completed migration batch for table '{}' with {} records",
        table_name,
        batch.len()
    );
    Ok(())
}

fn bindable_to_surrealdb_value(bindable: &BindableValue) -> surrealdb::sql::Value {
    match bindable {
        BindableValue::Bool(b) => surrealdb::sql::Value::Bool(*b),
        BindableValue::Int(i) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*i)),
        BindableValue::Float(f) => surrealdb::sql::Value::Number(surrealdb::sql::Number::from(*f)),
        BindableValue::String(s) => {
            surrealdb::sql::Value::Strand(surrealdb::sql::Strand::from(s.clone()))
        }
        BindableValue::DateTime(dt) => {
            surrealdb::sql::Value::Datetime(surrealdb::sql::Datetime::from(*dt))
        }
        BindableValue::Array(bindables) => {
            let mut arr = Vec::new();
            for item in bindables {
                let v = bindable_to_surrealdb_value(item);
                arr.push(v);
            }
            surrealdb::sql::Value::Array(surrealdb::sql::Array::from(arr))
        }
        BindableValue::Object(obj) => {
            let mut map = std::collections::BTreeMap::new();
            for (key, value) in obj {
                let v = bindable_to_surrealdb_value(value);
                map.insert(key.clone(), v);
            }
            surrealdb::sql::Value::Object(surrealdb::sql::Object::from(map))
        }
        BindableValue::Duration(d) => {
            surrealdb::sql::Value::Duration(surrealdb::sql::Duration::from(*d))
        }
        BindableValue::Bytes(b) => {
            surrealdb::sql::Value::Bytes(surrealdb::sql::Bytes::from(b.clone()))
        }
        BindableValue::Decimal(d) => surrealdb::sql::Value::Number(*d),
        BindableValue::Thing(t) => surrealdb::sql::Value::Thing(t.clone()),
        BindableValue::Geometry(g) => surrealdb::sql::Value::Geometry(g.clone()),
        BindableValue::Null => surrealdb::sql::Value::Null,
        BindableValue::None => surrealdb::sql::Value::None,
    }
}

pub async fn migrate_from_neo4j(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
) -> anyhow::Result<()> {
    neo4j::migrate_from_neo4j(from_opts, to_namespace, to_database, to_opts).await
}

pub async fn migrate_from_jsonl(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
    id_field: String,
    conversion_rules: Vec<String>,
) -> anyhow::Result<()> {
    jsonl::migrate_from_jsonl(
        from_opts,
        to_namespace,
        to_database,
        to_opts,
        id_field,
        conversion_rules,
    )
    .await
}

/// Enum to represent bindable values that can be bound to SurrealDB queries
#[derive(Debug, Clone)]
pub enum BindableValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Array(Vec<BindableValue>), // For arrays, we'll use JSON since SurrealDB accepts Vec<T> where T: Into<Value>
    Object(HashMap<String, BindableValue>), // For objects, use JSON
    Duration(std::time::Duration),
    Bytes(Vec<u8>),
    Decimal(surrealdb::sql::Number),
    Thing(surrealdb::sql::Thing),
    Geometry(surrealdb::sql::Geometry),
    Null,
    None,
}
