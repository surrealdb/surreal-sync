use std::collections::HashMap;
use surreal2_types::SurrealValue;
use surreal2_types::{RecordWithSurrealValues as Record, Relation};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MutationOp {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    UpsertRecord(Record),
    DeleteRecord(surrealdb::sql::Thing),
    UpsertRelation(Relation),
    DeleteRelation(surrealdb::sql::Thing),
}

impl Mutation {
    /// Create a record change using native surrealdb::sql::Value types.
    pub fn record(
        operation: MutationOp,
        id: surrealdb::sql::Thing,
        data: HashMap<String, surrealdb::sql::Value>,
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
        id: surrealdb::sql::Thing,
        input: surrealdb::sql::Thing,
        output: surrealdb::sql::Thing,
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
