//! Error types for the CSV populator.

use thiserror::Error;

/// Errors that can occur during CSV population.
#[derive(Error, Debug)]
pub enum CSVPopulatorError {
    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// CSV error.
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    /// Schema-related error.
    #[error("Schema error: {0}")]
    Schema(String),

    /// Table not found in schema.
    #[error("Table '{0}' not found in schema")]
    TableNotFound(String),

    /// Generator error.
    #[error("Generator error: {0}")]
    Generator(String),
}
