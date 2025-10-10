pub use super::{Record, Relation};
use std::collections::HashMap;

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
    pub fn record(
        operation: ChangeOp,
        id: surrealdb::sql::Thing,
        data: HashMap<String, crate::SurrealValue>,
    ) -> Self {
        match operation {
            ChangeOp::Create | ChangeOp::Update => Change::UpsertRecord(Record {
                id: id.clone(),
                data,
            }),
            ChangeOp::Delete => Change::DeleteRecord(id.clone()),
        }
    }

    pub fn relation(
        operation: ChangeOp,
        id: surrealdb::sql::Thing,
        input: surrealdb::sql::Thing,
        output: surrealdb::sql::Thing,
        data: Option<std::collections::HashMap<String, crate::SurrealValue>>,
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
