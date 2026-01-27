//! Row-level operations for converting and writing UniversalRow to SurrealDB.

use crate::write::write_record;
use anyhow::{bail, Result};
use std::collections::HashMap;
use surrealdb::sql::{Id, Thing, Value};
use surrealdb::Surreal;
use surrealdb_types::{RecordWithSurrealValues, SurrealValue};
use sync_core::{UniversalRow, UniversalValue};

/// Convert UniversalValue ID to SurrealDB Id.
///
/// Returns an error for unsupported ID types - never falls back silently.
pub fn universal_value_to_surreal_id(value: &UniversalValue) -> Result<Id> {
    match value {
        UniversalValue::Text(s) => Ok(Id::String(s.clone())),
        UniversalValue::Int32(n) => Ok(Id::Number(*n as i64)),
        UniversalValue::Int64(n) => Ok(Id::Number(*n)),
        UniversalValue::Uuid(u) => Ok(Id::Uuid(surrealdb::sql::Uuid::from(*u))),
        UniversalValue::Ulid(u) => {
            // Convert ULID to string since SurrealDB doesn't have native ULID ID type
            Ok(Id::String(u.to_string()))
        }
        other => bail!(
            "Unsupported UniversalValue type for SurrealDB ID: {other:?}. \
             Supported types: Text, Int32, Int64, Uuid, Ulid"
        ),
    }
}

/// Convert UniversalRow to RecordWithSurrealValues.
///
/// Returns an error if the row's ID type is not supported.
pub fn universal_row_to_surreal_record(row: &UniversalRow) -> Result<RecordWithSurrealValues> {
    let id = universal_value_to_surreal_id(&row.id)?;
    let thing = Thing::from((row.table.as_str(), id));

    let data: HashMap<String, Value> = row
        .fields
        .iter()
        .map(|(k, v)| {
            let typed = v.clone().to_typed_value();
            let surreal_val: SurrealValue = typed.into();
            (k.clone(), surreal_val.into_inner())
        })
        .collect();

    Ok(RecordWithSurrealValues::new(thing, data))
}

/// Write a batch of UniversalRows to SurrealDB.
///
/// Returns an error if any row has an unsupported ID type.
pub async fn write_universal_rows(
    surreal: &Surreal<surrealdb::engine::any::Any>,
    rows: &[UniversalRow],
) -> Result<()> {
    for row in rows {
        let record = universal_row_to_surreal_record(row)?;
        write_record(surreal, &record).await?;
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
        assert!(matches!(id, Id::String(s) if s == "user123"));
    }

    #[test]
    fn test_universal_value_to_surreal_id_int32() {
        let value = UniversalValue::Int32(42);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Number(n) if n == 42));
    }

    #[test]
    fn test_universal_value_to_surreal_id_int64() {
        let value = UniversalValue::Int64(9999999999);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Number(n) if n == 9999999999));
    }

    #[test]
    fn test_universal_value_to_surreal_id_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let value = UniversalValue::Uuid(uuid);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::Uuid(_)));
    }

    #[test]
    fn test_universal_value_to_surreal_id_ulid() {
        let ulid = ulid::Ulid::new();
        let ulid_str = ulid.to_string();
        let value = UniversalValue::Ulid(ulid);
        let id = universal_value_to_surreal_id(&value).unwrap();
        assert!(matches!(id, Id::String(s) if s == ulid_str));
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
}
