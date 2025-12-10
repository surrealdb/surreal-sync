use anyhow::Result;
use sync_core::{TypedValue, UniversalValue};

/// Convert typed values to a SurrealDB record using a specified field as the ID.
/// The field is removed from the typed values map and used as a part of the record ID.
pub fn typed_values_to_record_using_field_as_id(
    mut typed_values: std::collections::HashMap<String, TypedValue>,
    id_field: &str,
    table_name: &str,
) -> anyhow::Result<crate::RecordWithSurrealValues> {
    // Extract ID from typed values
    let surreal_id = if let Some(id_value) = typed_values.remove(id_field) {
        universal_value_to_surreal_id(&id_value.value)?
    } else {
        anyhow::bail!(format!("Message has no '{id_field}' field"));
    };

    typed_values_to_record_with_id(typed_values, surreal_id, table_name)
}

/// Convert typed values to a SurrealDB record using the bytes data
/// encoded using base64 as the record ID.
pub fn typed_values_to_record_with_bytes_id(
    typed_values: std::collections::HashMap<String, TypedValue>,
    bytes_data: &[u8],
    table_name: &str,
) -> anyhow::Result<crate::RecordWithSurrealValues> {
    use base64::Engine;
    let base64_str = base64::engine::general_purpose::STANDARD.encode(bytes_data);
    let surreal_id = surrealdb::sql::Id::String(base64_str);

    typed_values_to_record_with_id(typed_values, surreal_id, table_name)
}

fn typed_values_to_record_with_id(
    typed_values: std::collections::HashMap<String, TypedValue>,
    surreal_id: surrealdb::sql::Id,
    table_name: &str,
) -> anyhow::Result<crate::RecordWithSurrealValues> {
    // Convert typed values to SurrealDB values
    let surreal_values = crate::typed_values_to_surreal_map(typed_values);

    let r = crate::RecordWithSurrealValues {
        id: surrealdb::sql::Thing::from((table_name, surreal_id)),
        data: surreal_values,
    };

    Ok(r)
}

/// Convert a UniversalValue to a SurrealDB ID.
fn universal_value_to_surreal_id(value: &UniversalValue) -> Result<surrealdb::sql::Id> {
    match value {
        UniversalValue::Int32(i) => Ok(surrealdb::sql::Id::Number(*i as i64)),
        UniversalValue::Int64(i) => Ok(surrealdb::sql::Id::Number(*i)),
        UniversalValue::Text(s) => Ok(surrealdb::sql::Id::String(s.clone())),
        UniversalValue::Uuid(u) => Ok(surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(*u))),
        other => anyhow::bail!("Cannot convert {other:?} to SurrealDB ID"),
    }
}
