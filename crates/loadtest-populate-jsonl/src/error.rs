//! Error types for JSONL populator.

use thiserror::Error;

/// Errors that can occur during JSONL population.
#[derive(Error, Debug)]
pub enum JsonlPopulatorError {
    /// Table not found in schema.
    #[error("Table not found in schema: {0}")]
    TableNotFound(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization error.
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Data generator error.
    #[error("Generator error: {0}")]
    Generator(String),
}
