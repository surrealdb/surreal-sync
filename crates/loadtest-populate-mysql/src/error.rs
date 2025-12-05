//! Error types for the MySQL populator.

use thiserror::Error;

/// Errors that can occur during MySQL population.
#[derive(Error, Debug)]
pub enum MySQLPopulatorError {
    /// MySQL connection or query error.
    #[error("MySQL error: {0}")]
    MySQL(#[from] mysql_async::Error),

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
}
