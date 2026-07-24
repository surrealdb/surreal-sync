//! Parser for wal2json output format
//!
//! This module provides parsing functionality for PostgreSQL's wal2json
//! logical replication output format.

use anyhow::{Context, Result};
use serde_json::Value;

/// Parses a wal2json formatted string into a JSON object
///
/// Takes a string containing wal2json output and returns a parsed
/// serde_json::Value that is always a JSON object containing the
/// changeset encoded by postgresql+wal2json.
///
/// # Arguments
/// * `input` - A string containing wal2json formatted data
///
/// # Returns
/// * `Result<Value>` - A JSON object containing the parsed changeset
///
/// # Example
/// ```ignore
/// let wal2json_str = r#"{"change":[...]}"#;
/// let parsed = parse_wal2json(wal2json_str)?;
/// ```
pub fn parse_wal2json(input: &str) -> Result<Value> {
    // Parse the input string as JSON
    let value: Value =
        serde_json::from_str(input).context("Failed to parse wal2json output as JSON")?;

    // Ensure the parsed value is an object
    if !value.is_object() {
        anyhow::bail!("Expected wal2json output to be a JSON object, got: {value:?}");
    }

    Ok(value)
}
