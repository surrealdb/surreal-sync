//! Table and relation abstractions for test data
//!
//! Provides TestTable for entity data and TestRelation for relationship data,
//! with conversion methods to source-specific representations.

use crate::testing::{
    field::Field,
    schema::TestSchema,
    value::{MongoDBValue, MySQLValue, PostgreSQLValue, SurrealDBValue},
};
use std::collections::HashMap;

/// Source database types for field filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDatabase {
    MongoDB,
    MySQL,
    PostgreSQL,
    Neo4j,
    CSV,
    JSONL,
    Kafka,
}

/// A test document containing multiple fields
#[derive(Debug, Clone)]
pub struct TestDoc {
    pub fields: Vec<Field>,
}

impl TestDoc {
    /// Create a new test document with the given fields
    pub fn new(fields: Vec<Field>) -> Self {
        TestDoc { fields }
    }

    /// Get MongoDB representation of this document
    /// Only includes fields that have explicit MongoDB definitions
    pub fn to_mongodb_doc(&self) -> HashMap<String, MongoDBValue> {
        let mut doc = HashMap::new();
        for field in &self.fields {
            if let Some(mongodb) = &field.mongodb {
                doc.insert(mongodb.field_name.clone(), mongodb.field_value.clone());
            }
            // No fallback conversion - all MongoDB fields must be explicitly defined
        }
        doc
    }

    /// Get PostgreSQL representation of this document
    /// Only includes fields that have explicit PostgreSQL definitions
    pub fn to_postgresql_doc(&self) -> HashMap<String, PostgreSQLValue> {
        let mut doc = HashMap::new();
        for field in &self.fields {
            if let Some(postgresql) = &field.postgresql {
                doc.insert(
                    postgresql.column_name.clone(),
                    postgresql.column_value.clone(),
                );
            }
            // No fallback conversion - all PostgreSQL fields must be explicitly defined
        }
        doc
    }

    /// Get MySQL representation of this document
    /// Only includes fields that have explicit MySQL definitions
    pub fn to_mysql_doc(&self) -> HashMap<String, MySQLValue> {
        let mut doc = HashMap::new();
        for field in &self.fields {
            if let Some(mysql) = &field.mysql {
                doc.insert(mysql.column_name.clone(), mysql.column_value.clone());
            }
            // No fallback conversion - all MySQL fields must be explicitly defined
        }
        doc
    }

    /// Get Neo4j representation of this document
    /// Only includes fields that have explicit Neo4j definitions
    pub fn to_neo4j_doc(&self) -> HashMap<String, neo4rs::BoltType> {
        let mut doc = HashMap::new();
        for field in &self.fields {
            if let Some(neo4j) = &field.neo4j {
                doc.insert(neo4j.property_name.clone(), neo4j.property_value.clone());
            }
            // No fallback conversion - all Neo4j fields must be explicitly defined
        }
        doc
    }

    /// Get SurrealDB representation for validation
    pub fn to_surrealdb_doc(&self) -> HashMap<String, SurrealDBValue> {
        let mut doc = HashMap::new();
        for field in &self.fields {
            doc.insert(field.name.clone(), field.value.clone());
        }
        doc
    }

    /// Get SurrealDB representation filtered by source database
    /// Only includes fields that have explicit definitions for the specified source
    pub fn to_surrealdb_doc_for_source(
        &self,
        source: SourceDatabase,
    ) -> HashMap<String, SurrealDBValue> {
        let mut doc = HashMap::new();
        for field in &self.fields {
            let has_source_def = match source {
                SourceDatabase::MongoDB => field.mongodb.is_some(),
                SourceDatabase::MySQL => field.mysql.is_some(),
                SourceDatabase::PostgreSQL => field.postgresql.is_some(),
                SourceDatabase::Neo4j => field.neo4j.is_some(),
                // For sources without field-level definitions (CSV, JSONL, Kafka),
                // only include fields that have definitions for multiple databases
                // (i.e., not single-database-specific fields). This filters out
                // database-specific types like MongoDB's JavaScript with scope.
                SourceDatabase::CSV | SourceDatabase::JSONL | SourceDatabase::Kafka => {
                    // Count how many database-specific definitions this field has
                    let def_count = [
                        field.mongodb.is_some(),
                        field.mysql.is_some(),
                        field.postgresql.is_some(),
                        field.neo4j.is_some(),
                    ]
                    .iter()
                    .filter(|&&b| b)
                    .count();

                    // Include if:
                    // - No database-specific definitions (simple universal fields), OR
                    // - Multiple database definitions (common fields defined for several DBs)
                    // Exclude if only ONE database has a definition (DB-specific field)
                    def_count != 1
                }
            };
            if has_source_def {
                doc.insert(field.name.clone(), field.value.clone());
            }
        }
        doc
    }

    /// Get SurrealDB representation filtered to only fields that have Neo4j representations
    /// This is used for Neo4j validation where we only want to validate fields that were actually synced
    /// Deprecated: Use to_surrealdb_doc_for_source(SourceDatabase::Neo4j) instead
    pub fn to_surrealdb_doc_neo4j_filtered(&self) -> HashMap<String, SurrealDBValue> {
        self.to_surrealdb_doc_for_source(SourceDatabase::Neo4j)
    }

    /// Get field by logical name
    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get field value by logical name
    pub fn get_value(&self, name: &str) -> Option<&SurrealDBValue> {
        self.get_field(name).map(|f| &f.value)
    }
}

/// Represents table data (entities/documents)
#[derive(Debug, Clone)]
pub struct TestTable {
    pub name: String,            // Table/collection name
    pub documents: Vec<TestDoc>, // Table rows/documents
    pub schema: TestSchema,      // Required schema for table creation
}

impl TestTable {
    /// Create a new test table with schema and documents
    pub fn new(name: &str, schema: TestSchema, documents: Vec<TestDoc>) -> Self {
        TestTable {
            name: name.to_string(),
            documents,
            schema,
        }
    }

    /// Create an empty test table with only schema (for checkpoint generation)
    pub fn empty_with_schema(name: &str, schema: TestSchema) -> Self {
        TestTable {
            name: name.to_string(),
            documents: vec![],
            schema,
        }
    }

    /// Add a document to the table
    pub fn add_document(&mut self, doc: TestDoc) {
        self.documents.push(doc);
    }

    /// Get all documents as SurrealDB representation
    pub fn to_surrealdb_docs(&self) -> Vec<HashMap<String, SurrealDBValue>> {
        self.documents
            .iter()
            .map(|doc| doc.to_surrealdb_doc())
            .collect()
    }

    /// Get all documents as MongoDB representation
    pub fn to_mongodb_docs(&self) -> Vec<HashMap<String, MongoDBValue>> {
        self.documents
            .iter()
            .map(|doc| doc.to_mongodb_doc())
            .collect()
    }

    /// Get all documents as PostgreSQL representation
    pub fn to_postgresql_docs(&self) -> Vec<HashMap<String, PostgreSQLValue>> {
        self.documents
            .iter()
            .map(|doc| doc.to_postgresql_doc())
            .collect()
    }

    /// Get all documents as MySQL representation
    pub fn to_mysql_docs(&self) -> Vec<HashMap<String, MySQLValue>> {
        self.documents
            .iter()
            .map(|doc| doc.to_mysql_doc())
            .collect()
    }

    /// Get all documents as Neo4j representation
    pub fn to_neo4j_docs(&self) -> Vec<HashMap<String, neo4rs::BoltType>> {
        self.documents
            .iter()
            .map(|doc| doc.to_neo4j_doc())
            .collect()
    }
}

/// Complete test dataset with tables and relations
#[derive(Debug, Clone)]
pub struct TestDataSet {
    pub tables: Vec<TestTable>,
    pub relations: Vec<TestTable>,
}

impl TestDataSet {
    /// Create a new test dataset
    pub fn new() -> Self {
        TestDataSet {
            tables: Vec::new(),
            relations: Vec::new(),
        }
    }

    /// Add a table to the dataset
    pub fn add_table(mut self, table: TestTable) -> Self {
        self.tables.push(table);
        self
    }

    /// Add a relation to the dataset
    pub fn add_relation(mut self, relation: TestTable) -> Self {
        self.relations.push(relation);
        self
    }
}

impl Default for TestDataSet {
    fn default() -> Self {
        Self::new()
    }
}
