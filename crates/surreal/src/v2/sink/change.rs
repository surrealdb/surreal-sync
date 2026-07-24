use crate::v2::types::SurrealValue;
use crate::v2::types::{RecordWithSurrealValues as Record, Relation};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MutationOp {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    UpsertRecord(Record),
    DeleteRecord(surrealdb2::sql::Thing),
    UpsertRelation(Relation),
    DeleteRelation(surrealdb2::sql::Thing),
}

impl Mutation {
    /// Create a record change using native surrealdb2::sql::Value types.
    pub fn record(
        operation: MutationOp,
        id: surrealdb2::sql::Thing,
        data: HashMap<String, surrealdb2::sql::Value>,
    ) -> Self {
        match operation {
            MutationOp::Create | MutationOp::Update => {
                Mutation::UpsertRecord(Record::new(id, data))
            }
            MutationOp::Delete => Mutation::DeleteRecord(id),
        }
    }

    pub fn relation(
        operation: MutationOp,
        id: surrealdb2::sql::Thing,
        input: surrealdb2::sql::Thing,
        output: surrealdb2::sql::Thing,
        data: Option<HashMap<String, SurrealValue>>,
    ) -> Self {
        match operation {
            MutationOp::Create | MutationOp::Update => {
                let data = data.expect("Data must be provided for create/update relation");
                Mutation::UpsertRelation(Relation {
                    id,
                    input,
                    output,
                    data,
                })
            }
            MutationOp::Delete => Mutation::DeleteRelation(id.clone()),
        }
    }
}
