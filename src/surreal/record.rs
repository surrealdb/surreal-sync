pub use super::SurrealValue;

// Re-export RecordWithSurrealValues from surrealdb-types
pub use surrealdb_types::RecordWithSurrealValues;

#[derive(Debug, Clone)]
pub struct Record {
    pub id: surrealdb::sql::Thing,
    pub data: std::collections::HashMap<String, SurrealValue>,
}

impl Record {
    pub fn get_upsert_content(&self) -> surrealdb::sql::Value {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            tracing::debug!("Adding field to record: {k} -> {v:?}",);
            m.insert(k.clone(), v.to_surrealql_value());
        }
        surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
    }
}
