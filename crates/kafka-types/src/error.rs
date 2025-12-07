//! Error types for kafka-types crate.

use thiserror::Error;

/// Errors that can occur during Kafka type conversion.
#[derive(Error, Debug)]
pub enum KafkaTypesError {
    #[error("Protobuf encoding error: {0}")]
    ProtobufEncode(String),

    #[error("Protobuf decoding error: {0}")]
    ProtobufDecode(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Invalid timestamp: seconds={seconds}, nanos={nanos}")]
    InvalidTimestamp { seconds: i64, nanos: u32 },

    #[error("JSON parse error for field '{field}': {message}")]
    JsonParse { field: String, message: String },

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
}

/// Result type alias for kafka-types operations.
pub type Result<T> = std::result::Result<T, KafkaTypesError>;
