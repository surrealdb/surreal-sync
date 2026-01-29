//! Error types for the streaming verifier.

use thiserror::Error;

/// Errors that can occur during verification.
#[derive(Error, Debug)]
pub enum VerifyError {
    /// Table not found in schema.
    #[error("Table not found in schema: {0}")]
    TableNotFound(String),

    /// SurrealDB connection error.
    #[error("SurrealDB error: {0}")]
    SurrealDb(Box<surrealdb2::Error>),

    /// Data generator error.
    #[error("Generator error: {0}")]
    Generator(String),

    /// Query error.
    #[error("Query error: {0}")]
    Query(String),

    /// Verification failed.
    #[error("Verification failed: {found} found, {missing} missing, {mismatched} mismatched")]
    VerificationFailed {
        found: u64,
        missing: u64,
        mismatched: u64,
    },
}

impl From<surrealdb2::Error> for VerifyError {
    fn from(err: surrealdb2::Error) -> Self {
        VerifyError::SurrealDb(Box::new(err))
    }
}
