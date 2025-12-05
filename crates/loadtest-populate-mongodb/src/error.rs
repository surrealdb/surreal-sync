//! Error types for the MongoDB populator.

use thiserror::Error;

/// Errors that can occur during MongoDB population.
#[derive(Error, Debug)]
pub enum MongoDBPopulatorError {
    /// MongoDB connection or query error.
    #[error("MongoDB error: {0}")]
    MongoDB(#[from] mongodb::error::Error),

    /// Schema-related error.
    #[error("Schema error: {0}")]
    Schema(String),

    /// Collection (table) not found in schema.
    #[error("Collection '{0}' not found in schema")]
    CollectionNotFound(String),

    /// Generator error.
    #[error("Generator error: {0}")]
    Generator(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),
}
