use kafka_types::ProtoType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Protobuf parse error: {0}")]
    ProtobufParse(String),

    #[error("Protobuf decode error: {0}")]
    ProtobufDecode(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),

    #[error("Invalid field type: expected {expected}, got {actual}")]
    InvalidFieldType {
        expected: ProtoType,
        actual: ProtoType,
    },

    #[error("Message type not found: {0}")]
    MessageTypeNotFound(String),

    #[error("No messages available")]
    NoMessagesAvailable,

    #[error("Consumer error: {0}")]
    Consumer(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Message processing error: {0}")]
    MessageProcessing(String),
}

pub type Result<T> = std::result::Result<T, Error>;
