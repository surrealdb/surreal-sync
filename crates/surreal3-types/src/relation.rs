//! Relation type for SurrealDB graph relationships.
//!
//! This module provides the `Relation` struct for representing graph edges
//! between records in SurrealDB.

use crate::SurrealValue;
use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use surrealdb::types::RecordIdKey;
use surrealdb::types::{Object, RecordId, Value};

/// Represents a relation (edge) between two records in SurrealDB.
///
/// Relations are graph edges that connect an input record to an output record,
/// with optional data stored on the edge itself.
#[derive(Debug, Clone)]
pub struct Relation {
    /// The unique identifier of this relation (edge) as a RecordId
    pub id: RecordId,
    /// The input (source) record of the relation
    pub input: RecordId,
    /// The output (target) record of the relation
    pub output: RecordId,
    /// Additional data stored on the relation edge
    pub data: HashMap<String, SurrealValue>,
}

impl Relation {
    /// Get the input record as a SurrealDB Value.
    pub fn get_in(&self) -> Value {
        Value::RecordId(self.input.clone())
    }

    /// Get the output record as a SurrealDB Value.
    pub fn get_out(&self) -> Value {
        Value::RecordId(self.output.clone())
    }

    /// Get the relation content for use in RELATE statements.
    ///
    /// Returns a SurrealDB Object containing:
    /// - All data fields from the relation
    /// - The relation `id` as a RecordId
    pub fn get_relate_content(&self) -> Value {
        // Build content object from all fields (excluding relation marker and in/out for relations)
        let mut m = BTreeMap::new();
        for (k, v) in &self.data {
            tracing::debug!("Adding field to relation: {} -> {:?}", k, v);
            m.insert(k.clone(), v.clone().into_inner());
        }

        // Use the RecordId directly for the relation id field
        m.insert("id".to_string(), Value::RecordId(self.id.clone()));

        Value::Object(Object::from(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            id: RecordId::new("knows", RecordIdKey::String("rel1".to_string())),
            input: RecordId::new("users", RecordIdKey::String("alice".to_string())),
            output: RecordId::new("users", RecordIdKey::String("bob".to_string())),
            data,
        }
    }

    #[test]
    fn test_relation_get_in() {
        let relation = create_test_relation();
        let value = relation.get_in();

        match value {
            Value::RecordId(record_id) => {
                assert_eq!(record_id.table.as_str(), "users");
                assert!(matches!(record_id.key, RecordIdKey::String(ref s) if s == "alice"));
            }
            _ => panic!("Expected RecordId value"),
        }
    }

    #[test]
    fn test_relation_get_out() {
        let relation = create_test_relation();
        let value = relation.get_out();

        match value {
            Value::RecordId(record_id) => {
                assert_eq!(record_id.table.as_str(), "users");
                assert!(matches!(record_id.key, RecordIdKey::String(ref s) if s == "bob"));
            }
            _ => panic!("Expected RecordId value"),
        }
    }

    #[test]
    fn test_relation_get_relate_content() {
        let relation = create_test_relation();
        let content = relation.get_relate_content();

        match content {
            Value::Object(obj) => {
                // Check that id is present and correct
                let id_val = obj.get("id").expect("id field should be present");
                match id_val {
                    Value::RecordId(record_id) => {
                        assert_eq!(record_id.table.as_str(), "knows");
                        assert!(matches!(record_id.key, RecordIdKey::String(ref s) if s == "rel1"));
                    }
                    _ => panic!("id should be a RecordId"),
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
