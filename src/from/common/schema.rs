//! Schema loading and parsing utilities for sync handlers.

use anyhow::Context;
use std::path::PathBuf;
use sync_core::Schema;

/// Load a schema file if provided.
pub fn load_schema_if_provided(schema_file: &Option<PathBuf>) -> anyhow::Result<Option<Schema>> {
    if let Some(schema_path) = schema_file {
        Ok(Some(Schema::from_file(schema_path).with_context(|| {
            format!("Failed to load sync schema from {schema_path:?}")
        })?))
    } else {
        Ok(None)
    }
}

/// Extract JSON field paths from a schema (e.g., ["users.profile_data", "products.metadata"]).
/// This is used to auto-populate Neo4j JSON properties from the schema file.
pub fn extract_json_fields_from_schema(schema: &Schema) -> Vec<String> {
    use sync_core::UniversalType;

    let mut json_fields = Vec::new();
    for table in &schema.tables {
        for field in &table.fields {
            if matches!(field.field_type, UniversalType::Json | UniversalType::Jsonb) {
                json_fields.push(format!("{}.{}", table.name, field.name));
            }
        }
    }
    json_fields
}

/// Extract database name from a PostgreSQL connection string.
/// Supports formats like: postgresql://user:pass@host:port/database
pub fn extract_postgresql_database(connection_string: &str) -> Option<String> {
    // Try to extract from connection string
    // Format: postgresql://user:pass@host:port/database?params
    if let Some(db_start) = connection_string.rfind('/') {
        let after_slash = &connection_string[db_start + 1..];
        // Remove query params if present
        let db_name = after_slash.split('?').next().unwrap_or(after_slash);
        if !db_name.is_empty() {
            return Some(db_name.to_string());
        }
    }

    None
}
