use std::collections::HashMap;
use surreal3_types::SurrealValue;
use surreal3_types::{RecordWithSurrealValues as Record, Relation};
use surrealdb::types::RecordId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChangeOp {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub enum Change {
    UpsertRecord(Record),
    DeleteRecord(RecordId),
    UpsertRelation(Relation),
    DeleteRelation(RecordId),
}

impl Change {
    /// Create a record change using native surrealdb::types::Value types.
    pub fn record(
        operation: ChangeOp,
        id: RecordId,
        data: HashMap<String, surrealdb::types::Value>,
    ) -> Self {
        match operation {
            ChangeOp::Create | ChangeOp::Update => Change::UpsertRecord(Record::new(id, data)),
            ChangeOp::Delete => Change::DeleteRecord(id),
        }
    }

    pub fn relation(
        operation: ChangeOp,
        id: RecordId,
        input: RecordId,
        output: RecordId,
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
