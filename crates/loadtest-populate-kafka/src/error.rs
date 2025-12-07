//! Error types for the Kafka populator.

use thiserror::Error;

/// Errors that can occur during Kafka population.
#[derive(Error, Debug)]
pub enum KafkaPopulatorError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Table not found in schema: {0}")]
    TableNotFound(String),

    #[error("Proto generation error: {0}")]
    ProtoGeneration(String),

    #[error("Proto encoding error: {0}")]
    ProtoEncoding(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Generator error: {0}")]
    Generator(String),

    #[error("Topic creation error: {0}")]
    TopicCreation(String),
}

impl From<kafka_types::KafkaTypesError> for KafkaPopulatorError {
    fn from(err: kafka_types::KafkaTypesError) -> Self {
        KafkaPopulatorError::ProtoEncoding(err.to_string())
    }
}
