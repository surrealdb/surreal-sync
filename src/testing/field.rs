//! Field abstraction for test data
//!
//! Provides a unified field representation that can map logical fields
//! to source-specific implementations across different database systems.

use crate::testing::{
    mongodb::MongoDBField, mysql::MySQLField, neo4j::Neo4jField, postgresql::PostgreSQLField,
    value::SurrealDBValue,
};
// Note: Neo4jField now uses neo4rs::BoltType directly instead of Neo4jValue

/// Central field definition that can represent a logical field
/// with source-specific implementations for different databases
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,          // Logical field name
    pub value: SurrealDBValue, // Expected value in SurrealDB after sync
    pub mongodb: Option<MongoDBField>,
    pub postgresql: Option<PostgreSQLField>,
    pub mysql: Option<MySQLField>,
    pub neo4j: Option<Neo4jField>,
}

impl Field {
    /// Create a simple field that uses the same representation across all sources
    pub fn simple(name: &str, value: SurrealDBValue) -> Self {
        Field {
            name: name.to_string(),
            value,
            mongodb: None,
            postgresql: None,
            mysql: None,
            neo4j: None,
        }
    }

    /// Create a field with specific MongoDB representation
    pub fn with_mongodb(mut self, mongodb_field: MongoDBField) -> Self {
        self.mongodb = Some(mongodb_field);
        self
    }

    /// Create a field with specific PostgreSQL representation
    pub fn with_postgresql(mut self, postgresql_field: PostgreSQLField) -> Self {
        self.postgresql = Some(postgresql_field);
        self
    }

    /// Create a field with specific MySQL representation
    pub fn with_mysql(mut self, mysql_field: MySQLField) -> Self {
        self.mysql = Some(mysql_field);
        self
    }

    /// Create a field with specific Neo4j representation
    pub fn with_neo4j(mut self, neo4j_field: Neo4jField) -> Self {
        self.neo4j = Some(neo4j_field);
        self
    }
}
