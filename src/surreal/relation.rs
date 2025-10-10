pub use super::SurrealValue;

#[derive(Debug, Clone)]
pub struct Relation {
    pub id: surrealdb::sql::Thing,
    pub input: surrealdb::sql::Thing,
    pub output: surrealdb::sql::Thing,
    pub data: std::collections::HashMap<String, SurrealValue>,
}

impl Relation {
    pub fn get_in(&self) -> surrealdb::sql::Value {
        surrealdb::sql::Value::Thing(self.input.clone())
    }
    pub fn get_out(&self) -> surrealdb::sql::Value {
        surrealdb::sql::Value::Thing(self.output.clone())
    }
    pub fn get_relate_content(&self) -> surrealdb::sql::Value {
        // Build content object from all fields (excluding relation marker and in/out for relations)
        let mut m = std::collections::BTreeMap::new();
        for (k, v) in &self.data {
            tracing::debug!("Adding field to relation: {} -> {:?}", k, v);
            let surreal_value = v.to_surrealql_value();
            m.insert(k.clone(), surreal_value);
        }

        // Use the Thing directly for the relation id field
        m.insert(
            "id".to_string(),
            surrealdb::sql::Value::Thing(self.id.clone()),
        );

        surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
    }
}
