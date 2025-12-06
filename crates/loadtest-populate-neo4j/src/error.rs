//! Error types for Neo4j populator.

use thiserror::Error;

/// Errors that can occur during Neo4j population.
#[derive(Error, Debug)]
pub enum Neo4jPopulatorError {
    /// Neo4j database error
    #[error("Neo4j error: {0}")]
    Neo4j(#[from] neo4rs::Error),

    /// Schema-related error
    #[error("Schema error: {0}")]
    Schema(String),

    /// Table/label not found in schema
    #[error("Label '{0}' not found in schema")]
    LabelNotFound(String),

    /// Generator error
    #[error("Generator error: {0}")]
    Generator(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),
}

impl From<sync_core::SchemaError> for Neo4jPopulatorError {
    fn from(err: sync_core::SchemaError) -> Self {
        Neo4jPopulatorError::Schema(err.to_string())
    }
}
