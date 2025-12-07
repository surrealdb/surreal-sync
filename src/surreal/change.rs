pub use super::{Record, RecordWithSurrealValues, Relation};
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
    UpsertRecordWithSurrealValues(RecordWithSurrealValues),
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

    /// Create a record change using native surrealdb::sql::Value types.
    /// This is used by the unified TypedValue conversion path.
    pub fn record_with_surreal_values(
        operation: ChangeOp,
        id: surrealdb::sql::Thing,
        data: HashMap<String, surrealdb::sql::Value>,
    ) -> Self {
        match operation {
            ChangeOp::Create | ChangeOp::Update => {
                Change::UpsertRecordWithSurrealValues(RecordWithSurrealValues { id, data })
            }
            ChangeOp::Delete => Change::DeleteRecord(id),
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
