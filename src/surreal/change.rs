pub use super::{Record, Relation};
use std::collections::HashMap;
use surrealdb_types::SurrealValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChangeOp {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum Change {
    UpsertRecord(Record),
    DeleteRecord(surrealdb::sql::Thing),
    UpsertRelation(Relation),
    DeleteRelation(surrealdb::sql::Thing),
}

impl Change {
    /// Create a record change using native surrealdb::sql::Value types.
    pub fn record(
        operation: ChangeOp,
        id: surrealdb::sql::Thing,
        data: HashMap<String, surrealdb::sql::Value>,
    ) -> Self {
        match operation {
            ChangeOp::Create | ChangeOp::Update => Change::UpsertRecord(Record::new(id, data)),
            ChangeOp::Delete => Change::DeleteRecord(id),
        }
    }

    pub fn relation(
        operation: ChangeOp,
        id: surrealdb::sql::Thing,
        input: surrealdb::sql::Thing,
        output: surrealdb::sql::Thing,
        data: Option<HashMap<String, SurrealValue>>,
    ) -> Self {
        match operation {
            ChangeOp::Create | ChangeOp::Update => {
                let data = data.expect("Data must be provided for create/update relation");
                Change::UpsertRelation(Relation {
                    id,
                    input,
                    output,
                    data,
                })
            }
            ChangeOp::Delete => Change::DeleteRelation(id.clone()),
        }
    }
}
