pub use super::SurrealValue;

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

/// Record type that uses native surrealdb::sql::Value instead of SurrealValue.
/// This is used by the kafka-types unified conversion path.
#[derive(Debug, Clone)]
pub struct RecordWithSurrealValues {
    pub id: surrealdb::sql::Thing,
    pub data: std::collections::HashMap<String, surrealdb::sql::Value>,
}

impl RecordWithSurrealValues {
    pub fn get_upsert_content(&self) -> surrealdb::sql::Value {
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            tracing::debug!("Adding field to record: {k} -> {v:?}",);
            m.insert(k.clone(), v.clone());
        }
        surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
    }
}
