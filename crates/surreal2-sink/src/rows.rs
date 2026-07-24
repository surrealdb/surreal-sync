//! Row-level operations for converting and writing Row to SurrealDB.

use crate::write::{write_record, write_relation};
use anyhow::{bail, Result};
use std::collections::HashMap;
use surreal2_types::{RecordWithSurrealValues, Relation as SurrealRelation, SurrealValue};
use surrealdb::sql::{Array, Id, Strand, Thing, Value as SqlValue};
use surrealdb::Surreal;
use sync_core::{Relation, Row, Value, ZeroTemporalPolicy};

/// Convert Value ID to SurrealDB Id.
///
/// Returns an error for unsupported ID types - never falls back silently.
/// Composite primary keys arrive as [`Value::Array`] and become
/// SurrealDB array record IDs (`table:[k1, k2]`).
pub fn value_to_surreal_id(value: &Value) -> Result<Id> {
    match value {
        Value::Text(s) => Ok(Id::String(s.clone())),
        Value::VarChar { value, .. } => Ok(Id::String(value.clone())),
        Value::Char { value, .. } => Ok(Id::String(value.clone())),
        Value::Int32(n) => Ok(Id::Number(*n as i64)),
        Value::Int64(n) => Ok(Id::Number(*n)),
        Value::Uuid(u) => Ok(Id::Uuid(surrealdb::sql::Uuid::from(*u))),
        Value::Ulid(u) => {
            // Convert ULID to string since SurrealDB doesn't have native ULID ID type
            Ok(Id::String(u.to_string()))
        }
        Value::Array { elements, .. } => {
            if elements.is_empty() {
                bail!("Composite SurrealDB ID Array must not be empty");
            }
            let mut vals = Vec::with_capacity(elements.len());
            for (i, el) in elements.iter().enumerate() {
                vals.push(
                    universal_value_to_id_array_element(el)
                        .map_err(|e| anyhow::anyhow!("composite ID element [{i}]: {e}"))?,
                );
            }
            Ok(Id::Array(Array::from(vals)))
        }
        other => bail!(
            "Unsupported Value type for SurrealDB ID: {other:?}. \
             Supported types: Text, VarChar, Char, Int32, Int64, Uuid, Ulid, Array"
        ),
    }
}

/// Convert one element of a composite (Array) record ID to a SurrealDB [`SqlValue`].
fn universal_value_to_id_array_element(value: &Value) -> Result<SqlValue> {
    match value {
        Value::Text(s) => Ok(SqlValue::Strand(Strand::from(s.clone()))),
        Value::VarChar { value, .. } | Value::Char { value, .. } => {
            Ok(SqlValue::Strand(Strand::from(value.clone())))
        }
        Value::Int8 { value, .. } => Ok(SqlValue::Number((*value as i64).into())),
        Value::Int16(n) => Ok(SqlValue::Number((*n as i64).into())),
        Value::Int32(n) => Ok(SqlValue::Number((*n as i64).into())),
        Value::Int64(n) => Ok(SqlValue::Number((*n).into())),
        Value::Bool(b) => Ok(SqlValue::Bool(*b)),
        Value::Null => Ok(SqlValue::None),
        Value::Uuid(u) => Ok(SqlValue::Uuid(surrealdb::sql::Uuid::from(*u))),
        Value::Ulid(u) => Ok(SqlValue::Strand(Strand::from(u.to_string()))),
        Value::Array { .. } => {
            bail!("Nested arrays are not supported in composite SurrealDB IDs")
        }
        other => bail!(
            "Unsupported type in composite SurrealDB ID: {other:?}. \
             Supported element types: Text, VarChar, Char, Int8/16/32/64, Bool, Null, Uuid, Ulid"
        ),
    }
}

/// Convert Row to RecordWithSurrealValues.
///
/// Returns an error if the row's ID type is not supported.
pub fn row_to_surreal_record(
    row: &Row,
    zero_temporal: ZeroTemporalPolicy,
) -> Result<RecordWithSurrealValues> {
    let id = value_to_surreal_id(&row.id)?;
    let thing = Thing::from((row.table.as_str(), id));

    let data: HashMap<String, SqlValue> = row
        .fields
        .iter()
        .map(|(k, v)| {
            let typed = v.clone().to_typed_value();
            let surreal_val = SurrealValue::from_typed_with_policy(typed, zero_temporal);
            (k.clone(), surreal_val.into_inner())
        })
        .collect();

    Ok(RecordWithSurrealValues::new(thing, data))
}

/// Write a batch of Rows to SurrealDB.
///
/// Returns an error if any row has an unsupported ID type.
pub async fn write_rows(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    rows: &[Row],
    zero_temporal: ZeroTemporalPolicy,
) -> Result<()> {
    for row in rows {
        let record = row_to_surreal_record(row, zero_temporal)?;
        write_record(surreal, &record).await?;
    }
    Ok(())
}

/// Convert Relation to SurrealDB Relation.
///
/// Returns an error if any ID type is not supported.
pub fn relation_to_surreal_relation(
    rel: &Relation,
    zero_temporal: ZeroTemporalPolicy,
) -> Result<SurrealRelation> {
    // Convert the relation's own ID
    let id = value_to_surreal_id(&rel.id)?;
    let thing_id = Thing::from((rel.relation_type.as_str(), id));

    // Convert input (source) Thing
    let input_id = value_to_surreal_id(&rel.input.id)?;
    let input_thing = Thing::from((rel.input.table.as_str(), input_id));

    // Convert output (target) Thing
    let output_id = value_to_surreal_id(&rel.output.id)?;
    let output_thing = Thing::from((rel.output.table.as_str(), output_id));

    // Convert data
    let data: HashMap<String, SurrealValue> = rel
        .data
        .iter()
        .map(|(k, v)| {
            let typed = v.clone().to_typed_value();
            let surreal_val = SurrealValue::from_typed_with_policy(typed, zero_temporal);
            (k.clone(), surreal_val)
        })
        .collect();

    Ok(SurrealRelation {
        id: thing_id,
        input: input_thing,
        output: output_thing,
        data,
    })
}

/// Write a batch of Relations to SurrealDB.
///
/// Returns an error if any relation has an unsupported ID type.
pub async fn write_relations(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    relations: &[Relation],
    zero_temporal: ZeroTemporalPolicy,
) -> Result<()> {
    for rel in relations {
        let surreal_rel = relation_to_surreal_relation(rel, zero_temporal)?;
        write_relation(surreal, &surreal_rel).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::Value;

    #[test]
    fn test_value_to_surreal_id_text() {
        let value = Value::Text("user123".to_string());
        let id = value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::String(s) if s == "user123"));
    }

    #[test]
    fn test_value_to_surreal_id_int32() {
        let value = Value::Int32(42);
        let id = value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Number(n) if n == 42));
    }

    #[test]
    fn test_value_to_surreal_id_int64() {
        let value = Value::Int64(9999999999);
        let id = value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Number(n) if n == 9999999999));
    }

    #[test]
    fn test_value_to_surreal_id_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let value = Value::Uuid(uuid);
        let id = value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Uuid(_)));
    }

    #[test]
    fn test_value_to_surreal_id_ulid() {
        let ulid = ulid::Ulid::new();
        let ulid_str = ulid.to_string();
        let value = Value::Ulid(ulid);
        let id = value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::String(s) if s == ulid_str));
    }

    #[test]
    fn test_value_to_surreal_id_unsupported() {
        let value = Value::Float64(1.23);
        let result = value_to_surreal_id(&value);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unsupported Value type"));
    }

    #[test]
    fn test_value_to_surreal_id_bool_unsupported() {
        let value = Value::Bool(true);
        let result = value_to_surreal_id(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_to_surreal_id_array_ints() {
        let value = Value::Array {
            elements: vec![Value::Int32(4), Value::Int64(2)],
            element_type: Box::new(sync_core::Type::Int32),
        };
        let id = value_to_surreal_id(&value).unwrap();
        match id {
            Id::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(&arr[0], SqlValue::Number(n) if n.to_string() == "4"));
                assert!(matches!(&arr[1], SqlValue::Number(n) if n.to_string() == "2"));
            }
            other => panic!("expected Id::Array, got {other:?}"),
        }
    }

    #[test]
    fn test_value_to_surreal_id_array_mixed() {
        let uuid = uuid::Uuid::new_v4();
        let value = Value::Array {
            elements: vec![Value::Text("a_b".to_string()), Value::Uuid(uuid)],
            element_type: Box::new(sync_core::Type::Text),
        };
        let id = value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Array(arr) if arr.len() == 2));
    }

    #[test]
    fn test_value_to_surreal_id_array_empty_rejected() {
        let value = Value::Array {
            elements: vec![],
            element_type: Box::new(sync_core::Type::Text),
        };
        let err = value_to_surreal_id(&value).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn test_value_to_surreal_id_nested_array_rejected() {
        let value = Value::Array {
            elements: vec![Value::Array {
                elements: vec![Value::Int32(1)],
                element_type: Box::new(sync_core::Type::Int32),
            }],
            element_type: Box::new(sync_core::Type::Text),
        };
        let err = value_to_surreal_id(&value).unwrap_err();
        assert!(err.to_string().contains("Nested arrays"));
    }
}
