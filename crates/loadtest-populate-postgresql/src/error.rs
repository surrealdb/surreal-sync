//! Error types for the PostgreSQL populator.

use thiserror::Error;

/// Errors that can occur during PostgreSQL population.
#[derive(Error, Debug)]
pub enum PostgreSQLPopulatorError {
    /// PostgreSQL connection or query error.
    #[error("PostgreSQL error: {0}")]
    PostgreSQL(#[from] tokio_postgres::Error),

    /// Schema-related error.
    #[error("Schema error: {0}")]
    Schema(String),

    /// Table not found in schema.
    #[error("Table '{0}' not found in schema")]
    TableNotFound(String),

    /// Generator error.
    #[error("Generator error: {0}")]
    Generator(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Connection error.
    #[error("Connection error: {0}")]
    Connection(String),
}
