//! Relation type for SurrealDB graph relationships.
//!
//! This module provides the `Relation` struct for representing graph edges
//! between records in SurrealDB.

use crate::SurrealValue;
use std::collections::{BTreeMap, HashMap};

/// Represents a relation (edge) between two records in SurrealDB.
///
/// Relations are graph edges that connect an input record to an output record,
/// with optional data stored on the edge itself.
#[derive(Debug, Clone)]
pub struct Relation {
    /// The unique identifier of this relation (edge) as a Thing
    pub id: surrealdb::sql::Thing,
    /// The input (source) record of the relation
    pub input: surrealdb::sql::Thing,
    /// The output (target) record of the relation
    pub output: surrealdb::sql::Thing,
    /// Additional data stored on the relation edge
    pub data: HashMap<String, SurrealValue>,
}

impl Relation {
    /// Get the input record as a SurrealDB Value.
    pub fn get_in(&self) -> surrealdb::sql::Value {
        surrealdb::sql::Value::Thing(self.input.clone())
    }

    /// Get the output record as a SurrealDB Value.
    pub fn get_out(&self) -> surrealdb::sql::Value {
        surrealdb::sql::Value::Thing(self.output.clone())
    }

    /// Get the relation content for use in RELATE statements.
    ///
    /// Returns a SurrealDB Object containing:
    /// - All data fields from the relation
    /// - The relation `id` as a Thing
    pub fn get_relate_content(&self) -> surrealdb::sql::Value {
        // Build content object from all fields (excluding relation marker and in/out for relations)
        let mut m = BTreeMap::new();
        for (k, v) in &self.data {
            tracing::debug!("Adding field to relation: {} -> {:?}", k, v);
            m.insert(k.clone(), v.clone().into_inner());
        }

        // Use the Thing directly for the relation id field
        m.insert(
            "id".to_string(),
            surrealdb::sql::Value::Thing(self.id.clone()),
        );

        surrealdb::sql::Value::Object(surrealdb::sql::Object::from(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb::sql::{Id, Thing};
    use sync_core::TypedValue;

    fn create_test_relation() -> Relation {
        let mut data = HashMap::new();
        data.insert(
            "weight".to_string(),
            SurrealValue::from(TypedValue::int64(42)),
        );
        data.insert(
            "label".to_string(),
            SurrealValue::from(TypedValue::text("friend")),
        );

        Relation {
            id: Thing::from(("knows", Id::String("rel1".to_string()))),
            input: Thing::from(("users", Id::String("alice".to_string()))),
            output: Thing::from(("users", Id::String("bob".to_string()))),
            data,
        }
    }

    #[test]
    fn test_relation_get_in() {
        let relation = create_test_relation();
        let value = relation.get_in();

        match value {
            surrealdb::sql::Value::Thing(thing) => {
                assert_eq!(thing.tb, "users");
                assert_eq!(thing.id, Id::String("alice".to_string()));
            }
            _ => panic!("Expected Thing value"),
        }
    }

    #[test]
    fn test_relation_get_out() {
        let relation = create_test_relation();
        let value = relation.get_out();

        match value {
            surrealdb::sql::Value::Thing(thing) => {
                assert_eq!(thing.tb, "users");
                assert_eq!(thing.id, Id::String("bob".to_string()));
            }
            _ => panic!("Expected Thing value"),
        }
    }

    #[test]
    fn test_relation_get_relate_content() {
        let relation = create_test_relation();
        let content = relation.get_relate_content();

        match content {
            surrealdb::sql::Value::Object(obj) => {
                // Check that id is present and correct
                let id_val = obj.get("id").expect("id field should be present");
                match id_val {
                    surrealdb::sql::Value::Thing(thing) => {
                        assert_eq!(thing.tb, "knows");
                        assert_eq!(thing.id, Id::String("rel1".to_string()));
                    }
                    _ => panic!("id should be a Thing"),
                }

                // Check that data fields are present
                assert!(
                    obj.get("weight").is_some(),
                    "weight field should be present"
                );
                assert!(obj.get("label").is_some(), "label field should be present");
            }
            _ => panic!("Expected Object value"),
        }
    }
}
