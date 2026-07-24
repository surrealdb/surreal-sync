//! Relation change type for incremental sync of graph edges.
//!
//! Provides [`RelationChange`] which represents a create, update, or
//! delete operation on a SurrealDB relation (graph edge).

use crate::values::{ChangeOp, Relation};
use serde::{Deserialize, Serialize};

/// A change event for a graph relation (edge).
///
/// Used when a PostgreSQL join/relation table row is inserted, updated, or
/// deleted and needs to be synced as a SurrealDB `RELATE` or `DELETE` operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationChange {
    /// The operation type (Create/Update/Delete)
    pub operation: ChangeOp,
    /// The relation data (contains relation_type, id, input, output, data)
    pub relation: Relation,
}

impl RelationChange {
    /// Create a new relation change.
    pub fn new(operation: ChangeOp, relation: Relation) -> Self {
        Self {
            operation,
            relation,
        }
    }

    /// Create a CREATE relation change.
    pub fn create(relation: Relation) -> Self {
        Self::new(ChangeOp::Create, relation)
    }

    /// Create an UPDATE relation change.
    pub fn update(relation: Relation) -> Self {
        Self::new(ChangeOp::Update, relation)
    }

    /// Create a DELETE relation change.
    pub fn delete(relation: Relation) -> Self {
        Self::new(ChangeOp::Delete, relation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::{ThingRef, Value};
    use std::collections::HashMap;

    #[test]
    fn test_create_relation_change() {
        let rel = Relation::new(
            "book_tags",
            Value::Int64(1),
            ThingRef::new("books", Value::Int64(10)),
            ThingRef::new("tags", Value::Int64(20)),
            HashMap::new(),
        );
        let change = RelationChange::create(rel);
        assert_eq!(change.operation, ChangeOp::Create);
        assert_eq!(change.relation.relation_type, "book_tags");
    }

    #[test]
    fn test_delete_relation_change() {
        let rel = Relation::new(
            "book_tags",
            Value::Int64(1),
            ThingRef::new("books", Value::Int64(10)),
            ThingRef::new("tags", Value::Int64(20)),
            HashMap::new(),
        );
        let change = RelationChange::delete(rel);
        assert_eq!(change.operation, ChangeOp::Delete);
    }
}
