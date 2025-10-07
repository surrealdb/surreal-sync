//! MongoDB-specific field definitions and data injection functions

use crate::testing::{
    table::{TestDataSet, TestTable},
    value::MongoDBValue,
};
use mongodb::{
    bson::{Bson, Document},
    Client, Collection, Database,
};
use std::str::FromStr;

pub async fn connect_mongodb() -> Result<Client, Box<dyn std::error::Error>> {
    let client = Client::with_uri_str("mongodb://root:root@mongodb:27017").await?;
    Ok(client)
}

pub async fn cleanup_mongodb_test_data(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let collections = [
        "users",
        "posts",
        "comments",
        "all_types_users",
        "all_types_posts",
    ];
    for collection_name in collections {
        let collection = db.collection::<serde_json::Value>(collection_name);
        collection.drop().await.ok(); // Ignore errors if collection doesn't exist
    }
    Ok(())
}

/// MongoDB-specific field representation
#[derive(Debug, Clone)]
pub struct MongoDBField {
    pub field_name: String,        // MongoDB field name
    pub field_value: MongoDBValue, // MongoDB-specific representation
    pub bson_type: String,         // "objectId", "javascript", "regex", etc.
}

/// Convert MongoDBValue to BSON for MongoDB operations
#[allow(dead_code)]
pub fn mongodb_value_to_bson(value: &MongoDBValue) -> Result<Bson, Box<dyn std::error::Error>> {
    match value {
        MongoDBValue::Null => Ok(Bson::Null),
        MongoDBValue::Bool(b) => Ok(Bson::Boolean(*b)),
        MongoDBValue::Int32(i) => Ok(Bson::Int32(*i)),
        MongoDBValue::Int64(i) => Ok(Bson::Int64(*i)),
        MongoDBValue::Float64(f) => Ok(Bson::Double(*f)),
        MongoDBValue::String(s) => Ok(Bson::String(s.clone())),
        MongoDBValue::Binary(b) => Ok(Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: b.clone(),
        })),
        MongoDBValue::Array(arr) => {
            let mut bson_array = Vec::new();
            for item in arr {
                bson_array.push(mongodb_value_to_bson(item)?);
            }
            Ok(Bson::Array(bson_array))
        }
        MongoDBValue::Document(doc) => {
            let mut bson_doc = Document::new();
            for (key, val) in doc {
                bson_doc.insert(key, mongodb_value_to_bson(val)?);
            }
            Ok(Bson::Document(bson_doc))
        }
        MongoDBValue::DateTime(dt) => Ok(Bson::DateTime(mongodb::bson::DateTime::from_chrono(*dt))),
        MongoDBValue::Timestamp {
            timestamp,
            increment,
        } => Ok(Bson::Timestamp(mongodb::bson::Timestamp {
            time: *timestamp,
            increment: *increment,
        })),
        MongoDBValue::ObjectId(oid) => {
            let object_id = mongodb::bson::oid::ObjectId::parse_str(oid)?;
            Ok(Bson::ObjectId(object_id))
        }
        MongoDBValue::JavaScript { code, scope } => {
            if let Some(scope_map) = scope {
                let mut scope_doc = Document::new();
                for (key, val) in scope_map {
                    scope_doc.insert(key, mongodb_value_to_bson(val)?);
                }
                Ok(Bson::JavaScriptCodeWithScope(
                    mongodb::bson::JavaScriptCodeWithScope {
                        code: code.clone(),
                        scope: scope_doc,
                    },
                ))
            } else {
                Ok(Bson::JavaScriptCode(code.clone()))
            }
        }
        MongoDBValue::RegularExpression { pattern, flags } => {
            Ok(Bson::RegularExpression(mongodb::bson::Regex {
                pattern: pattern.clone(),
                options: flags.clone(),
            }))
        }
        MongoDBValue::Decimal128(dec) => {
            let decimal = mongodb::bson::Decimal128::from_str(dec)?;
            Ok(Bson::Decimal128(decimal))
        }
        MongoDBValue::MinKey => Ok(Bson::MinKey),
        MongoDBValue::MaxKey => Ok(Bson::MaxKey),
        MongoDBValue::Symbol(s) => Ok(Bson::Symbol(s.clone())),
        MongoDBValue::DBPointer { namespace, id } => {
            // DBPointer fields are private, so convert to string for now
            Ok(Bson::String(format!("DBPointer({namespace}:{id})")))
        }
    }
}

pub async fn create_collections(
    db: &Database,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        create_mongodb_collection_from_test_table(db, table).await?;
    }
    for relation in &dataset.relations {
        create_mongodb_collection_from_test_table(db, relation).await?;
    }
    Ok(())
}

pub async fn cleanup(
    db: &Database,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        let collection: Collection<Document> = db.collection(&table.name);
        collection.drop().await.ok(); // Ignore errors if collection doesn't exist

        for index in &table.schema.mongodb.indexes {
            if let Some(ref name) = index.name {
                collection.drop_index(name).await.ok(); // Ignore errors if index doesn't exist
            }
        }
    }
    for relation in &dataset.relations {
        let collection: Collection<Document> = db.collection(&relation.name);
        collection.drop().await.ok(); // Ignore errors if collection doesn't exist
    }
    Ok(())
}

/// Create MongoDB collection from TestTable schema
#[allow(dead_code)]
pub async fn create_mongodb_collection_from_test_table(
    db: &Database,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use schema to create collection (required field)
    {
        let schema = &table.schema;
        // Use the collection name from schema or table name
        let collection_name = if !schema.mongodb.collection_name.is_empty() {
            &schema.mongodb.collection_name
        } else {
            &table.name
        };

        // Create collection with options if specified
        let mut options = mongodb::options::CreateCollectionOptions::default();

        // Set capped collection options if specified
        if schema.mongodb.capped {
            options.capped = Some(true);
            options.size = schema.mongodb.size;
            options.max = schema.mongodb.max;
        }

        // Set validation schema if specified
        if let Some(ref validation) = schema.mongodb.validation_schema {
            if let Ok(doc) = mongodb::bson::to_document(validation) {
                options.validator = Some(doc);
            }
        }

        // Create the collection
        db.create_collection(collection_name)
            .with_options(options)
            .await?;

        // Create indexes
        let collection: Collection<Document> = db.collection(collection_name);
        for index in &schema.mongodb.indexes {
            let mut keys = Document::new();
            for (field, direction) in &index.keys {
                keys.insert(field, direction);
            }

            let mut index_model = mongodb::IndexModel::builder().keys(keys).build();

            if let Some(ref name) = index.name {
                index_model = mongodb::IndexModel::builder()
                    .keys(index_model.keys.clone())
                    .options(
                        mongodb::options::IndexOptions::builder()
                            .name(name.clone())
                            .unique(Some(index.unique))
                            .sparse(Some(index.sparse))
                            .expire_after(
                                index
                                    .ttl
                                    .map(|ttl| std::time::Duration::from_secs(ttl as u64)),
                            )
                            .build(),
                    )
                    .build();
            }

            collection.create_index(index_model).await?;
        }
    }

    Ok(())
}

pub async fn insert_docs(
    db: &Database,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        inject_test_table_mongodb(db, table).await?;
    }
    for relation in &dataset.relations {
        inject_test_table_mongodb(db, relation).await?;
    }
    Ok(())
}

/// Inject TestTable data into MongoDB
#[allow(dead_code)]
pub async fn inject_test_table_mongodb(
    db: &Database,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    // First create the collection with schema if defined
    create_mongodb_collection_from_test_table(db, table).await?;

    if table.documents.is_empty() {
        return Ok(());
    }

    // Convert documents to BSON and insert
    let mut bson_docs = Vec::new();

    for doc in &table.documents {
        let mongodb_doc = doc.to_mongodb_doc();

        if mongodb_doc.is_empty() {
            continue;
        }

        let mut bson_doc = Document::new();
        for (field_name, mongodb_value) in mongodb_doc {
            bson_doc.insert(field_name, mongodb_value_to_bson(&mongodb_value)?);
        }

        bson_docs.push(bson_doc);
    }

    if !bson_docs.is_empty() {
        let collection: Collection<Document> = db.collection(&table.name);
        collection.insert_many(bson_docs).await?;
    }

    Ok(())
}
