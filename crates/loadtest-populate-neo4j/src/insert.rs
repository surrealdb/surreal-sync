//! Batched INSERT logic for Neo4j population.

use crate::error::Neo4jPopulatorError;
use neo4rs::{query, Graph, Query};
use sync_core::{TableDefinition, TypedValue, UniversalRow, UniversalType, UniversalValue};
use tracing::debug;

/// Default batch size for INSERT operations.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Insert a batch of nodes into Neo4j.
///
/// Neo4j uses labels instead of tables - each "table" becomes a node label.
pub async fn insert_batch(
    graph: &Graph,
    table_schema: &TableDefinition,
    rows: &[UniversalRow],
) -> Result<u64, Neo4jPopulatorError> {
    if rows.is_empty() {
        return Ok(0);
    }

    let label = &table_schema.name;
    let mut inserted = 0u64;

    // Neo4j doesn't have great batch insert like SQL's multi-row INSERT,
    // so we use individual CREATE statements
    for row in rows {
        let query = build_create_node_query(label, row, table_schema)?;
        graph.run(query).await?;
        inserted += 1;
    }

    Ok(inserted)
}

/// Build a CREATE query for a single node.
fn build_create_node_query(
    label: &str,
    row: &UniversalRow,
    table_schema: &TableDefinition,
) -> Result<Query, Neo4jPopulatorError> {
    // Build property map for the node
    let mut props = Vec::new();

    // Add the id field - always as string since Neo4j full sync expects string IDs
    // The Neo4j full sync uses the 'id' property to create SurrealDB record IDs
    let id_string = match &row.id {
        UniversalValue::TinyInt { value, .. } => value.to_string(),
        UniversalValue::SmallInt(i) => i.to_string(),
        UniversalValue::Int(i) => i.to_string(),
        UniversalValue::BigInt(i) => i.to_string(),
        UniversalValue::Text(s) => s.clone(),
        UniversalValue::Char { value, .. } => value.clone(),
        UniversalValue::VarChar { value, .. } => value.clone(),
        UniversalValue::Uuid(u) => u.to_string(),
        other => format!("{other:?}"),
    };
    props.push(format!("id: '{}'", id_string.replace('\'', "\\'")));

    // Add each field
    for field_schema in &table_schema.fields {
        let field_value = row.get_field(&field_schema.name);
        let typed_value = match field_value {
            Some(value) => {
                TypedValue::try_with_type(field_schema.field_type.clone(), value.clone())
                    .expect("generator produced invalid type-value combination for field")
            }
            None => TypedValue::null(field_schema.field_type.clone()),
        };
        let neo4j_literal = typed_to_neo4j_literal(&typed_value);
        props.push(format!("{}: {}", field_schema.name, neo4j_literal));
    }

    let props_str = props.join(", ");
    let cypher = format!("CREATE (n:{label} {{{props_str}}})");

    debug!("Neo4j CREATE query: {}", cypher);

    Ok(query(&cypher))
}

/// Convert a TypedValue to a Neo4j literal string for use in Cypher queries.
fn typed_to_neo4j_literal(typed: &TypedValue) -> String {
    match &typed.value {
        UniversalValue::Null => "null".to_string(),
        UniversalValue::Bool(b) => b.to_string(),

        // Integer types - strict 1:1 matching
        UniversalValue::TinyInt { value, .. } => value.to_string(),
        UniversalValue::SmallInt(i) => i.to_string(),
        UniversalValue::Int(i) => i.to_string(),
        UniversalValue::BigInt(i) => i.to_string(),

        // Float types - strict 1:1 matching
        UniversalValue::Float(f) => {
            if f.is_nan() {
                "null".to_string()
            } else {
                f.to_string()
            }
        }
        UniversalValue::Double(f) => {
            if f.is_nan() {
                "null".to_string() // Neo4j doesn't support NaN
            } else {
                f.to_string()
            }
        }
        UniversalValue::Decimal { value, .. } => {
            // Store decimal as numeric string in Neo4j
            value.to_string()
        }

        // String types - strict 1:1 matching
        UniversalValue::Text(s) => escape_neo4j_string(s),
        UniversalValue::Char { value, .. } => escape_neo4j_string(value),
        UniversalValue::VarChar { value, .. } => escape_neo4j_string(value),

        UniversalValue::Uuid(u) => escape_neo4j_string(&u.to_string()),

        // DateTime types - strict 1:1 matching
        UniversalValue::DateTime(dt) => {
            format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S%.fZ"))
        }
        UniversalValue::DateTimeNano(dt) => {
            format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S%.fZ"))
        }
        UniversalValue::TimestampTz(dt) => {
            format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S%.fZ"))
        }
        UniversalValue::Date(dt) => {
            format!("date('{}')", dt.format("%Y-%m-%d"))
        }
        UniversalValue::Time(dt) => {
            format!("time('{}')", dt.format("%H:%M:%S"))
        }

        // Binary types - strict 1:1 matching
        UniversalValue::Bytes(b) => {
            // Neo4j doesn't have native bytes, store as hex string
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            escape_neo4j_string(&hex)
        }
        UniversalValue::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            escape_neo4j_string(&hex)
        }

        // JSON types - strict 1:1 matching
        UniversalValue::Json(json_val) => {
            let json_str = serde_json::to_string(&json_val).unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }
        UniversalValue::Jsonb(json_val) => {
            let json_str = serde_json::to_string(&json_val).unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }

        // Enum type - strict 1:1 matching
        UniversalValue::Enum { value, .. } => escape_neo4j_string(value),

        // Set type - strict 1:1 matching
        UniversalValue::Set { elements, .. } => {
            let element_strs: Vec<String> =
                elements.iter().map(|s| escape_neo4j_string(s)).collect();
            format!("[{}]", element_strs.join(", "))
        }

        // Geometry type - store as GeoJSON string
        UniversalValue::Geometry { data, .. } => {
            use sync_core::GeometryData;
            let GeometryData(json) = data;
            let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }

        // Array type - recursive handling
        UniversalValue::Array { elements, .. } => {
            // Neo4j lists
            let element_strs: Vec<String> = elements
                .iter()
                .map(|v| {
                    // For arrays, we need to handle each element
                    generated_to_neo4j_literal(v, &typed.sync_type)
                })
                .collect();
            format!("[{}]", element_strs.join(", "))
        }
    }
}

/// Convert a UniversalValue to a Neo4j literal (for array elements).
fn generated_to_neo4j_literal(value: &UniversalValue, parent_type: &UniversalType) -> String {
    // Get element type if this is an array
    let element_type = match parent_type {
        UniversalType::Array { element_type } => element_type.as_ref().clone(),
        _ => UniversalType::Text, // Fallback
    };

    let typed = TypedValue::try_with_type(element_type, value.clone())
        .expect("generator produced invalid type-value combination for array element");
    typed_to_neo4j_literal(&typed)
}

/// Escape a string for Neo4j Cypher.
fn escape_neo4j_string(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("'{escaped}'")
}

/// Delete all nodes with a specific label.
pub async fn delete_all_nodes(graph: &Graph, label: &str) -> Result<(), Neo4jPopulatorError> {
    let cypher = format!("MATCH (n:{label}) DETACH DELETE n");
    graph.run(query(&cypher)).await?;
    Ok(())
}

/// Count nodes with a specific label.
pub async fn count_nodes(graph: &Graph, label: &str) -> Result<u64, Neo4jPopulatorError> {
    let cypher = format!("MATCH (n:{label}) RETURN count(n) as count");
    let mut result = graph.execute(query(&cypher)).await?;

    if let Some(row) = result.next().await? {
        let count: i64 = row
            .get("count")
            .map_err(|e| Neo4jPopulatorError::Schema(format!("Failed to get count: {e}")))?;
        Ok(count as u64)
    } else {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_neo4j_string() {
        assert_eq!(escape_neo4j_string("hello"), "'hello'");
        assert_eq!(escape_neo4j_string("it's"), "'it\\'s'");
        assert_eq!(escape_neo4j_string("line\nbreak"), "'line\\nbreak'");
    }

    #[test]
    fn test_typed_to_neo4j_literal() {
        let int_val = TypedValue::int(42);
        assert_eq!(typed_to_neo4j_literal(&int_val), "42");

        let str_val = TypedValue::text("hello");
        assert_eq!(typed_to_neo4j_literal(&str_val), "'hello'");

        let bool_val = TypedValue::bool(true);
        assert_eq!(typed_to_neo4j_literal(&bool_val), "true");

        let null_val = TypedValue::null(UniversalType::Text);
        assert_eq!(typed_to_neo4j_literal(&null_val), "null");
    }
}
