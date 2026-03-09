//! Relation change type for incremental sync of graph edges.
//!
//! Provides [`UniversalRelationChange`] which represents a create, update, or
//! delete operation on a SurrealDB relation (graph edge).

use crate::values::{UniversalChangeOp, UniversalRelation};

/// A change event for a graph relation (edge).
///
/// Used when a PostgreSQL join/relation table row is inserted, updated, or
/// deleted and needs to be synced as a SurrealDB `RELATE` or `DELETE` operation.
#[derive(Debug, Clone)]
pub struct UniversalRelationChange {
    /// The operation type (Create/Update/Delete)
    pub operation: UniversalChangeOp,
    /// The relation data (contains relation_type, id, input, output, data)
    pub relation: UniversalRelation,
}

impl UniversalRelationChange {
    /// Create a new relation change.
    pub fn new(operation: UniversalChangeOp, relation: UniversalRelation) -> Self {
        Self {
            operation,
            relation,
        }
    }

    /// Create a CREATE relation change.
    pub fn create(relation: UniversalRelation) -> Self {
        Self::new(UniversalChangeOp::Create, relation)
    }

    /// Create an UPDATE relation change.
    pub fn update(relation: UniversalRelation) -> Self {
        Self::new(UniversalChangeOp::Update, relation)
    }

    /// Create a DELETE relation change.
    pub fn delete(relation: UniversalRelation) -> Self {
        Self::new(UniversalChangeOp::Delete, relation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::{UniversalThingRef, UniversalValue};
    use std::collections::HashMap;

    #[test]
    fn test_create_relation_change() {
        let rel = UniversalRelation::new(
            "book_tags",
            UniversalValue::Int64(1),
            UniversalThingRef::new("books", UniversalValue::Int64(10)),
            UniversalThingRef::new("tags", UniversalValue::Int64(20)),
            HashMap::new(),
        );
        let change = UniversalRelationChange::create(rel);
        assert_eq!(change.operation, UniversalChangeOp::Create);
        assert_eq!(change.relation.relation_type, "book_tags");
    }

    #[test]
    fn test_delete_relation_change() {
        let rel = UniversalRelation::new(
            "book_tags",
            UniversalValue::Int64(1),
            UniversalThingRef::new("books", UniversalValue::Int64(10)),
            UniversalThingRef::new("tags", UniversalValue::Int64(20)),
            HashMap::new(),
        );
        let change = UniversalRelationChange::delete(rel);
        assert_eq!(change.operation, UniversalChangeOp::Delete);
    }
}
