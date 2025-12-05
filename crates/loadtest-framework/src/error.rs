//! Error types for the load testing framework.

use thiserror::Error;

/// Errors that can occur during load testing.
#[derive(Error, Debug)]
pub enum LoadTestError {
    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Source population error.
    #[error("Population error: {0}")]
    Population(String),

    /// Sync execution error.
    #[error("Sync error: {0}")]
    Sync(String),

    /// Verification error.
    #[error("Verification error: {0}")]
    Verification(String),

    /// SurrealDB error.
    #[error("SurrealDB error: {0}")]
    SurrealDb(String),

    /// MySQL error.
    #[error("MySQL error: {0}")]
    MySQL(String),

    /// PostgreSQL error.
    #[error("PostgreSQL error: {0}")]
    PostgreSQL(String),

    /// MongoDB error.
    #[error("MongoDB error: {0}")]
    MongoDB(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Schema error.
    #[error("Schema error: {0}")]
    Schema(String),
}

impl From<loadtest_populate_mysql::MySQLPopulatorError> for LoadTestError {
    fn from(err: loadtest_populate_mysql::MySQLPopulatorError) -> Self {
        LoadTestError::MySQL(err.to_string())
    }
}

impl From<loadtest_populate_postgresql::PostgreSQLPopulatorError> for LoadTestError {
    fn from(err: loadtest_populate_postgresql::PostgreSQLPopulatorError) -> Self {
        LoadTestError::PostgreSQL(err.to_string())
    }
}

impl From<loadtest_populate_mongodb::MongoDBPopulatorError> for LoadTestError {
    fn from(err: loadtest_populate_mongodb::MongoDBPopulatorError) -> Self {
        LoadTestError::MongoDB(err.to_string())
    }
}

impl From<loadtest_populate_csv::CSVPopulatorError> for LoadTestError {
    fn from(err: loadtest_populate_csv::CSVPopulatorError) -> Self {
        LoadTestError::Io(std::io::Error::other(err.to_string()))
    }
}

impl From<loadtest_populate_jsonl::JsonlPopulatorError> for LoadTestError {
    fn from(err: loadtest_populate_jsonl::JsonlPopulatorError) -> Self {
        LoadTestError::Io(std::io::Error::other(err.to_string()))
    }
}

impl From<loadtest_verify::VerifyError> for LoadTestError {
    fn from(err: loadtest_verify::VerifyError) -> Self {
        LoadTestError::Verification(err.to_string())
    }
}

impl From<surrealdb::Error> for LoadTestError {
    fn from(err: surrealdb::Error) -> Self {
        LoadTestError::SurrealDb(err.to_string())
    }
}
