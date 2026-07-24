//! Batched INSERT logic for Neo4j population.

use crate::error::Neo4jPopulatorError;
use neo4rs::{query, Graph, Query};
use surreal_sync_core::{GeneratorTableDefinition, Row, Type, TypedValue, Value};
use tracing::debug;

/// Default batch size for INSERT operations.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Insert a batch of nodes into Neo4j.
///
/// Neo4j uses labels instead of tables - each "table" becomes a node label.
pub async fn insert_batch(
    graph: &Graph,
    table_schema: &GeneratorTableDefinition,
    rows: &[Row],
) -> Result<u64, Neo4jPopulatorError> {
    if rows.is_empty() {
        return Ok(0);
    }

    let label = &table_schema.name;
    let mut inserted = 0u64;

    // Neo4j doesn't have great batch insert like SQL's multi-row INSERT,
    // so we use individual CREATE statements
    for row in rows.iter() {
        let query = build_create_node_query(label, row, table_schema)?;
        graph.run(query).await?;
        inserted += 1;
    }

    Ok(inserted)
}

/// Build a CREATE query for a single node.
fn build_create_node_query(
    label: &str,
    row: &Row,
    table_schema: &GeneratorTableDefinition,
) -> Result<Query, Neo4jPopulatorError> {
    // Build property map for the node
    let mut props = Vec::new();

    // Add the id field - always as string since Neo4j full sync expects string IDs
    // The Neo4j full sync uses the 'id' property to create SurrealDB record IDs
    let id_string = match &row.id {
        Value::Int8 { value, .. } => value.to_string(),
        Value::Int16(i) => i.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Text(s) => s.clone(),
        Value::Char { value, .. } => value.clone(),
        Value::VarChar { value, .. } => value.clone(),
        Value::Uuid(u) => u.to_string(),
        other => panic!(
            "Unsupported ID type for Neo4j node: {other:?}. \
             Supported types: Int8, Int16, Int32, Int64, Text, Char, VarChar, Uuid"
        ),
    };
    props.push(format!("id: '{}'", id_string.replace('\'', "\\'")));

    // Add each field from the schema
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
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),

        // Integer types - strict 1:1 matching
        Value::Int8 { value, .. } => value.to_string(),
        Value::Int16(i) => i.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),

        // Float types - strict 1:1 matching
        Value::Float32(f) => {
            if f.is_nan() {
                "null".to_string()
            } else {
                f.to_string()
            }
        }
        Value::Float64(f) => {
            if f.is_nan() {
                "null".to_string() // Neo4j doesn't support NaN
            } else {
                f.to_string()
            }
        }
        Value::Decimal { value, .. } => {
            // Store decimal as numeric string in Neo4j
            value.to_string()
        }

        // String types - strict 1:1 matching
        Value::Text(s) => escape_neo4j_string(s),
        Value::Char { value, .. } => escape_neo4j_string(value),
        Value::VarChar { value, .. } => escape_neo4j_string(value),

        Value::Uuid(u) => escape_neo4j_string(&u.to_string()),

        // ULID - encode as string
        Value::Ulid(u) => escape_neo4j_string(&u.to_string()),

        // DateTime types - strict 1:1 matching
        Value::LocalDateTime(dt) => {
            format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S%.fZ"))
        }
        Value::LocalDateTimeNano(dt) => {
            format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S%.fZ"))
        }
        Value::ZonedDateTime(dt) => {
            format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S%.fZ"))
        }
        Value::Date(dt) => {
            format!("date('{}')", dt.format("%Y-%m-%d"))
        }
        Value::Time(dt) => {
            format!("time('{}')", dt.format("%H:%M:%S"))
        }

        // Binary types - strict 1:1 matching
        Value::Bytes(b) => {
            // Neo4j doesn't have native bytes, store as hex string
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            escape_neo4j_string(&hex)
        }
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            escape_neo4j_string(&hex)
        }

        // JSON types - strict 1:1 matching
        Value::Json(json_val) => {
            let json_str = serde_json::to_string(&json_val).unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }
        Value::Jsonb(json_val) => {
            let json_str = serde_json::to_string(&json_val).unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }

        // Enum type - strict 1:1 matching
        Value::Enum { value, .. } => escape_neo4j_string(value),

        // Set type - strict 1:1 matching
        Value::Set { elements, .. } => {
            let element_strs: Vec<String> =
                elements.iter().map(|s| escape_neo4j_string(s)).collect();
            format!("[{}]", element_strs.join(", "))
        }

        // Geometry type - store as GeoJSON string
        Value::Geometry { data, .. } => {
            use surreal_sync_core::GeometryData;
            let GeometryData(json) = data;
            let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }

        // Array type - recursive handling
        Value::Array { elements, .. } => {
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

        // Duration type - Neo4j duration
        Value::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                format!("duration('PT{secs}S')")
            } else {
                format!("duration('PT{secs}.{nanos:09}S')")
            }
        }

        // Thing - record reference as string in "table:id" format
        Value::Thing { table, id } => {
            let id_str = match id.as_ref() {
                Value::Text(s) => s.clone(),
                Value::Int32(i) => i.to_string(),
                Value::Int64(i) => i.to_string(),
                Value::Uuid(u) => u.to_string(),
                other => panic!(
                    "Unsupported Thing ID type for Neo4j: {other:?}. \
                     Supported types: Text, Int32, Int64, Uuid"
                ),
            };
            format!("'{table}:{id_str}'")
        }

        // Object - serialize as JSON string
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_to_json(v)))
                .collect();
            let json_str = serde_json::to_string(&serde_json::Value::Object(obj))
                .unwrap_or_else(|_| "{}".to_string());
            escape_neo4j_string(&json_str)
        }

        // TimeTz - stored as string to preserve timezone format
        Value::TimeTz(s) => escape_neo4j_string(s),

        Value::ZeroTemporal {
            intended_type,
            source,
        } => {
            let s = source
                .as_deref()
                .unwrap_or_else(|| Value::canonical_zero_literal(intended_type));
            escape_neo4j_string(s)
        }
    }
}

/// Convert a Value to a Neo4j literal (for array elements).
fn generated_to_neo4j_literal(value: &Value, parent_type: &Type) -> String {
    // Get element type if this is an array
    let element_type = match parent_type {
        Type::Array { element_type } => element_type.as_ref().clone(),
        _ => Type::Text, // Fallback
    };

    let typed = TypedValue::try_with_type(element_type, value.clone())
        .expect("generator produced invalid type-value combination for array element");
    typed_to_neo4j_literal(&typed)
}

/// Convert a Value to a serde_json::Value for Object serialization.
fn universal_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::json!(*b),
        Value::Int8 { value, .. } => serde_json::json!(*value),
        Value::Int16(i) => serde_json::json!(*i),
        Value::Int32(i) => serde_json::json!(*i),
        Value::Int64(i) => serde_json::json!(*i),
        Value::Float32(f) => serde_json::json!(*f),
        Value::Float64(f) => serde_json::json!(*f),
        Value::Text(s) => serde_json::json!(s),
        Value::Char { value, .. } => serde_json::json!(value),
        Value::VarChar { value, .. } => serde_json::json!(value),
        Value::Uuid(u) => serde_json::json!(u.to_string()),
        Value::Ulid(u) => serde_json::json!(u.to_string()),
        Value::LocalDateTime(dt) | Value::LocalDateTimeNano(dt) | Value::ZonedDateTime(dt) => {
            serde_json::json!(dt.to_rfc3339())
        }
        Value::Date(dt) => serde_json::json!(dt.format("%Y-%m-%d").to_string()),
        Value::Time(dt) => serde_json::json!(dt.format("%H:%M:%S").to_string()),
        Value::Bytes(b) | Value::Blob(b) => {
            serde_json::json!(b
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>())
        }
        Value::Json(j) | Value::Jsonb(j) => (**j).clone(),
        Value::Decimal { value, .. } => serde_json::json!(value),
        Value::Enum { value, .. } => serde_json::json!(value),
        Value::Set { elements, .. } => serde_json::json!(elements),
        Value::Array { elements, .. } => {
            serde_json::Value::Array(elements.iter().map(universal_to_json).collect())
        }
        Value::Geometry { data, .. } => {
            use surreal_sync_core::GeometryData;
            let GeometryData(json) = data;
            json.clone()
        }
        Value::Duration(d) => {
            let secs = d.as_secs();
            let nanos = d.subsec_nanos();
            if nanos == 0 {
                serde_json::json!(format!("PT{secs}S"))
            } else {
                serde_json::json!(format!("PT{secs}.{nanos:09}S"))
            }
        }
        Value::Thing { table, id } => {
            let id_str = match id.as_ref() {
                Value::Text(s) => s.clone(),
                Value::Int32(i) => i.to_string(),
                Value::Int64(i) => i.to_string(),
                Value::Uuid(u) => u.to_string(),
                _ => "unknown".to_string(),
            };
            serde_json::json!(format!("{table}:{id_str}"))
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), universal_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        // TimeTz - stored as string to preserve timezone format
        Value::TimeTz(s) => serde_json::json!(s),
        Value::ZeroTemporal {
            intended_type,
            source,
        } => {
            let s = source
                .as_deref()
                .unwrap_or_else(|| Value::canonical_zero_literal(intended_type));
            serde_json::json!(s)
        }
    }
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
        let int_val = TypedValue::int32(42);
        assert_eq!(typed_to_neo4j_literal(&int_val), "42");

        let str_val = TypedValue::text("hello");
        assert_eq!(typed_to_neo4j_literal(&str_val), "'hello'");

        let bool_val = TypedValue::bool(true);
        assert_eq!(typed_to_neo4j_literal(&bool_val), "true");

        let null_val = TypedValue::null(Type::Text);
        assert_eq!(typed_to_neo4j_literal(&null_val), "null");
    }
}
