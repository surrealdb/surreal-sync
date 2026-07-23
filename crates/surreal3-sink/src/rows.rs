//! Row-level operations for converting and writing UniversalRow to SurrealDB v3.

use crate::write::{write_record, write_relation};
use anyhow::{bail, Result};
use std::collections::HashMap;
use surreal3_types::{RecordWithSurrealValues, Relation, SurrealValue};
use surrealdb::types::{Array, Number, RecordId, RecordIdKey, Value};
use surrealdb::Surreal;
use sync_core::{UniversalRelation, UniversalRow, UniversalValue, ZeroTemporalPolicy};

/// Convert UniversalValue ID to SurrealDB RecordIdKey.
///
/// Returns an error for unsupported ID types - never falls back silently.
/// Composite primary keys arrive as [`UniversalValue::Array`] and become
/// SurrealDB array record IDs (`table:[k1, k2]`).
pub fn universal_value_to_surreal_id(value: &UniversalValue) -> Result<RecordIdKey> {
    match value {
        UniversalValue::Text(s) => Ok(RecordIdKey::String(s.clone())),
        UniversalValue::VarChar { value, .. } => Ok(RecordIdKey::String(value.clone())),
        UniversalValue::Char { value, .. } => Ok(RecordIdKey::String(value.clone())),
        UniversalValue::Int32(n) => Ok(RecordIdKey::Number(*n as i64)),
        UniversalValue::Int64(n) => Ok(RecordIdKey::Number(*n)),
        UniversalValue::Uuid(u) => Ok(RecordIdKey::Uuid(surrealdb::types::Uuid::from(*u))),
        UniversalValue::Ulid(u) => {
            // Convert ULID to string since SurrealDB doesn't have native ULID ID type
            Ok(RecordIdKey::String(u.to_string()))
        }
        UniversalValue::Array { elements, .. } => {
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
            Ok(RecordIdKey::Array(Array::from(vals)))
        }
        other => bail!(
            "Unsupported UniversalValue type for SurrealDB ID: {other:?}. \
             Supported types: Text, VarChar, Char, Int32, Int64, Uuid, Ulid, Array"
        ),
    }
}

/// Convert one element of a composite (Array) record ID to a SurrealDB [`Value`].
fn universal_value_to_id_array_element(value: &UniversalValue) -> Result<Value> {
    match value {
        UniversalValue::Text(s) => Ok(Value::String(s.clone())),
        UniversalValue::VarChar { value, .. } | UniversalValue::Char { value, .. } => {
            Ok(Value::String(value.clone()))
        }
        UniversalValue::Int8 { value, .. } => Ok(Value::Number(Number::Int(*value as i64))),
        UniversalValue::Int16(n) => Ok(Value::Number(Number::Int(*n as i64))),
        UniversalValue::Int32(n) => Ok(Value::Number(Number::Int(*n as i64))),
        UniversalValue::Int64(n) => Ok(Value::Number(Number::Int(*n))),
        UniversalValue::Bool(b) => Ok(Value::Bool(*b)),
        UniversalValue::Null => Ok(Value::None),
        UniversalValue::Uuid(u) => Ok(Value::Uuid(surrealdb::types::Uuid::from(*u))),
        UniversalValue::Ulid(u) => Ok(Value::String(u.to_string())),
        UniversalValue::Array { .. } => {
            bail!("Nested arrays are not supported in composite SurrealDB IDs")
        }
        other => bail!(
            "Unsupported type in composite SurrealDB ID: {other:?}. \
             Supported element types: Text, VarChar, Char, Int8/16/32/64, Bool, Null, Uuid, Ulid"
        ),
    }
}

/// Convert UniversalRow to RecordWithSurrealValues.
///
/// Returns an error if the row's ID type is not supported.
pub fn universal_row_to_surreal_record(
    row: &UniversalRow,
    zero_temporal: ZeroTemporalPolicy,
) -> Result<RecordWithSurrealValues> {
    let id = universal_value_to_surreal_id(&row.id)?;
    let record_id = RecordId::new(row.table.as_str(), id);

    let data: HashMap<String, Value> = row
        .fields
        .iter()
        .map(|(k, v)| {
            let typed = v.clone().to_typed_value();
            let surreal_val = SurrealValue::from_typed_with_policy(typed, zero_temporal);
            (k.clone(), surreal_val.into_inner())
        })
        .collect();

    Ok(RecordWithSurrealValues::new(record_id, data))
}

/// Write a batch of UniversalRows to SurrealDB.
///
/// Returns an error if any row has an unsupported ID type.
pub async fn write_universal_rows(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    rows: &[UniversalRow],
    zero_temporal: ZeroTemporalPolicy,
) -> Result<()> {
    for row in rows {
        let record = universal_row_to_surreal_record(row, zero_temporal)?;
        write_record(surreal, &record).await?;
    }
    Ok(())
}

/// Convert UniversalRelation to SurrealDB Relation.
///
/// Returns an error if any ID type is not supported.
pub fn universal_relation_to_surreal_relation(
    rel: &UniversalRelation,
    zero_temporal: ZeroTemporalPolicy,
) -> Result<Relation> {
    // Convert the relation's own ID
    let id = universal_value_to_surreal_id(&rel.id)?;
    let record_id = RecordId::new(rel.relation_type.as_str(), id);

    // Convert input (source) RecordId
    let input_id = universal_value_to_surreal_id(&rel.input.id)?;
    let input_record_id = RecordId::new(rel.input.table.as_str(), input_id);

    // Convert output (target) RecordId
    let output_id = universal_value_to_surreal_id(&rel.output.id)?;
    let output_record_id = RecordId::new(rel.output.table.as_str(), output_id);

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

    Ok(Relation {
        id: record_id,
        input: input_record_id,
        output: output_record_id,
        data,
    })
}

/// Write a batch of UniversalRelations to SurrealDB.
///
/// Returns an error if any relation has an unsupported ID type.
pub async fn write_universal_relations(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    relations: &[UniversalRelation],
    zero_temporal: ZeroTemporalPolicy,
) -> Result<()> {
    for rel in relations {
        let surreal_rel = universal_relation_to_surreal_relation(rel, zero_temporal)?;
        write_relation(surreal, &surreal_rel).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::UniversalValue;

    #[test]
    fn test_universal_value_to_surreal_id_text() {
        let value = UniversalValue::Text("user123".to_string());
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, RecordIdKey::String(s) if s == "user123"));
    }

    #[test]
    fn test_universal_value_to_surreal_id_int32() {
        let value = UniversalValue::Int32(42);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, RecordIdKey::Number(n) if n == 42));
    }

    #[test]
    fn test_universal_value_to_surreal_id_int64() {
        let value = UniversalValue::Int64(9999999999);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, RecordIdKey::Number(n) if n == 9999999999));
    }

    #[test]
    fn test_universal_value_to_surreal_id_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let value = UniversalValue::Uuid(uuid);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, RecordIdKey::Uuid(_)));
    }

    #[test]
    fn test_universal_value_to_surreal_id_ulid() {
        let ulid = ulid::Ulid::new();
        let ulid_str = ulid.to_string();
        let value = UniversalValue::Ulid(ulid);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, RecordIdKey::String(s) if s == ulid_str));
    }

    #[test]
    fn test_universal_value_to_surreal_id_unsupported() {
        let value = UniversalValue::Float64(1.23);
        let result = universal_value_to_surreal_id(&value);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unsupported UniversalValue type"));
    }

    #[test]
    fn test_universal_value_to_surreal_id_bool_unsupported() {
        let value = UniversalValue::Bool(true);
        let result = universal_value_to_surreal_id(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_universal_value_to_surreal_id_array_ints() {
        let value = UniversalValue::Array {
            elements: vec![UniversalValue::Int32(4), UniversalValue::Int64(2)],
            element_type: Box::new(sync_core::UniversalType::Int32),
        };
        let id = universal_value_to_surreal_id(&value).unwrap();
        match id {
            RecordIdKey::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(&arr[0], Value::Number(Number::Int(4))));
                assert!(matches!(&arr[1], Value::Number(Number::Int(2))));
            }
            other => panic!("expected RecordIdKey::Array, got {other:?}"),
        }
    }

    #[test]
    fn test_universal_value_to_surreal_id_array_mixed() {
        let uuid = uuid::Uuid::new_v4();
        let value = UniversalValue::Array {
            elements: vec![
                UniversalValue::Text("a_b".to_string()),
                UniversalValue::Uuid(uuid),
            ],
            element_type: Box::new(sync_core::UniversalType::Text),
        };
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, RecordIdKey::Array(arr) if arr.len() == 2));
    }

    #[test]
    fn test_universal_value_to_surreal_id_array_empty_rejected() {
        let value = UniversalValue::Array {
            elements: vec![],
            element_type: Box::new(sync_core::UniversalType::Text),
        };
        let err = universal_value_to_surreal_id(&value).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn test_universal_value_to_surreal_id_nested_array_rejected() {
        let value = UniversalValue::Array {
            elements: vec![UniversalValue::Array {
                elements: vec![UniversalValue::Int32(1)],
                element_type: Box::new(sync_core::UniversalType::Int32),
            }],
            element_type: Box::new(sync_core::UniversalType::Text),
        };
        let err = universal_value_to_surreal_id(&value).unwrap_err();
        assert!(err.to_string().contains("Nested arrays"));
    }
}
