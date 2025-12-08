//! Error types for Neo4j type conversions.
//!
//! This module defines error types that are returned when type conversions fail.
//! Unlike other type conversion modules that may silently fall back to default values,
//! this module returns explicit errors for all unexpected cases.

use thiserror::Error;

/// Errors that can occur during Neo4j type conversions.
#[derive(Debug, Error)]
pub enum Neo4jTypesError {
    /// Invalid IANA timezone string.
    #[error("Invalid timezone: {0}. Use IANA timezone names like 'America/New_York', 'UTC', 'Europe/London'")]
    InvalidTimezone(String),

    /// Ambiguous or invalid datetime due to DST transition or invalid date.
    #[error("Ambiguous or invalid datetime in timezone {timezone}: {datetime}")]
    AmbiguousDateTime { timezone: String, datetime: String },

    /// NaN float values cannot be represented in Neo4j.
    #[error("NaN float values cannot be represented in Neo4j")]
    NanFloat,

    /// Infinity float values cannot be represented in Neo4j.
    #[error("Infinity float values cannot be represented in Neo4j")]
    InfinityFloat,

    /// Neo4j type cannot be converted to a property value.
    #[error("Neo4j {bolt_type} cannot be converted to a property value")]
    UnsupportedBoltType { bolt_type: String },

    /// JSON parse error when converting string to JSON object.
    #[error("JSON parse error for property '{property}': {error}")]
    JsonParseError { property: String, error: String },

    /// Type mismatch during conversion.
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Invalid UUID string format.
    #[error("Invalid UUID format: {value}")]
    InvalidUuid { value: String },

    /// Invalid date string format.
    #[error("Invalid date format: {value}. Expected YYYY-MM-DD")]
    InvalidDateFormat { value: String },

    /// Invalid time string format.
    #[error("Invalid time format: {value}. Expected HH:MM:SS")]
    InvalidTimeFormat { value: String },

    /// Invalid datetime string format.
    #[error("Invalid datetime format: {value}. Expected ISO 8601 format")]
    InvalidDateTimeFormat { value: String },

    /// Invalid duration format.
    #[error("Invalid duration format: {value}. Expected ISO 8601 duration")]
    InvalidDurationFormat { value: String },

    /// Required field is missing.
    #[error("Required field '{field}' is missing")]
    MissingField { field: String },

    /// Conversion overflow (e.g., i64 to i32).
    #[error("Numeric overflow converting {value} to {target_type}")]
    NumericOverflow { value: String, target_type: String },

    /// Invalid date value.
    #[error("Invalid date: {reason}")]
    InvalidDate { reason: String },

    /// Invalid datetime value.
    #[error("Invalid datetime: {reason}")]
    InvalidDateTime { reason: String },
}

/// Result type for Neo4j type conversions.
pub type Result<T> = std::result::Result<T, Neo4jTypesError>;
