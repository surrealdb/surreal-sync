use crate::v3::types::SurrealValue;
use crate::v3::types::{RecordWithSurrealValues as Record, Relation};
use std::collections::HashMap;
use surrealdb3::types::RecordId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MutationOp {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    UpsertRecord(Record),
    DeleteRecord(RecordId),
    UpsertRelation(Relation),
    DeleteRelation(RecordId),
}

impl Mutation {
    /// Create a record change using native surrealdb3::types::Value types.
    pub fn record(
        operation: MutationOp,
        id: RecordId,
        data: HashMap<String, surrealdb3::types::Value>,
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
        id: RecordId,
        input: RecordId,
        output: RecordId,
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
