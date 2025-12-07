//! Batched INSERT logic for MongoDB population.

use crate::error::MongoDBPopulatorError;
use bson::{doc, Document};
use mongodb::Collection;
use mongodb_types::forward::BsonValue;
use sync_core::{TableDefinition, TypedValue, UniversalRow};

/// Default batch size for INSERT operations.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Insert a batch of documents into a MongoDB collection.
pub async fn insert_batch(
    collection: &Collection<Document>,
    table_schema: &TableDefinition,
    rows: &[UniversalRow],
) -> Result<u64, MongoDBPopulatorError> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Convert UniversalRows to BSON documents
    let documents: Vec<Document> = rows
        .iter()
        .map(|row| internal_row_to_document(row, table_schema))
        .collect();

    // Insert all documents
    let result = collection.insert_many(documents).await?;

    Ok(result.inserted_ids.len() as u64)
}

/// Convert an UniversalRow to a BSON Document.
fn internal_row_to_document(row: &UniversalRow, table_schema: &TableDefinition) -> Document {
    let mut doc = Document::new();

    // Add the _id field
    let id_typed = TypedValue::with_type(table_schema.id.id_type.clone(), row.id.clone());
    let id_bson: BsonValue = id_typed.into();
    doc.insert("_id", id_bson.into_inner());

    // Add each field
    for field_schema in &table_schema.fields {
        let field_value = row.get_field(&field_schema.name);
        let typed_value = match field_value {
            Some(value) => TypedValue::with_type(field_schema.field_type.clone(), value.clone()),
            None => TypedValue::null(field_schema.field_type.clone()),
        };
        let bson_value: BsonValue = typed_value.into();
        doc.insert(&field_schema.name, bson_value.into_inner());
    }

    doc
}

/// Insert a single document (useful for debugging or when batch fails).
#[allow(dead_code)]
pub async fn insert_single(
    collection: &Collection<Document>,
    table_schema: &TableDefinition,
    row: &UniversalRow,
) -> Result<u64, MongoDBPopulatorError> {
    let doc = internal_row_to_document(row, table_schema);
    collection.insert_one(doc).await?;
    Ok(1)
}

/// Drop a collection if it exists.
pub async fn drop_collection(
    collection: &Collection<Document>,
) -> Result<(), MongoDBPopulatorError> {
    collection.drop().await?;
    Ok(())
}

/// Get the document count for a collection.
pub async fn count_documents(
    collection: &Collection<Document>,
) -> Result<u64, MongoDBPopulatorError> {
    let count = collection.count_documents(doc! {}).await?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::{Schema, UniversalValue};

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
seed: 42
tables:
  - name: users
    id:
      type: uuid
      generator:
        type: uuid_v4
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"
      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_internal_row_to_document() {
        let schema = test_schema();
        let table_schema = schema.get_table("users").unwrap();

        let row = UniversalRow::new(
            "users".to_string(),
            0,
            UniversalValue::Uuid(uuid::Uuid::new_v4()),
            [
                (
                    "email".to_string(),
                    UniversalValue::String("test@example.com".to_string()),
                ),
                ("age".to_string(), UniversalValue::Int32(25)),
            ]
            .into_iter()
            .collect(),
        );

        let doc = internal_row_to_document(&row, table_schema);

        assert!(doc.contains_key("_id"));
        assert_eq!(doc.get_str("email").unwrap(), "test@example.com");
        assert_eq!(doc.get_i32("age").unwrap(), 25);
    }
}
