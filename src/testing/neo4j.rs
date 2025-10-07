//! Neo4j-specific field definitions and data injection functions

use crate::testing::{table::TestDataSet, table::TestTable};
use chrono::{DateTime, Utc};
use neo4rs::{BoltBoolean, BoltFloat, BoltInteger, BoltString, BoltType, Graph};

#[derive(Clone)]
pub struct Neo4jConfig {
    uri: String,
    username: String,
    password: String,
    database: String,
}

impl Default for Neo4jConfig {
    fn default() -> Self {
        default_config()
    }
}

impl Neo4jConfig {
    pub fn get_uri(&self) -> String {
        self.uri.clone()
    }

    pub fn get_username(&self) -> String {
        self.username.clone()
    }

    pub fn get_password(&self) -> String {
        self.password.clone()
    }

    pub fn get_database(&self) -> String {
        self.database.clone()
    }
}

pub fn default_config() -> Neo4jConfig {
    let uri = std::env::var("NEO4J_TEST_URL").unwrap_or_else(|_| "bolt://neo4j:7687".to_string());
    Neo4jConfig {
        uri,
        username: "neo4j".to_string(),
        password: "password".to_string(),
        database: "neo4j".to_string(),
    }
}

pub async fn delete_nodes_and_relationships(
    graph: &Graph,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clean all nodes and relationships
    let cleanup_query = "MATCH (n) DETACH DELETE n";
    graph.run(neo4rs::query(cleanup_query)).await?;
    Ok(())
}

/// Helper functions to create BoltType values for test data
pub mod bolt {
    use super::*;

    pub fn string(s: &str) -> BoltType {
        BoltType::String(BoltString::new(s))
    }

    pub fn int(i: i64) -> BoltType {
        BoltType::Integer(BoltInteger::new(i))
    }

    pub fn float(f: f64) -> BoltType {
        BoltType::Float(BoltFloat::new(f))
    }

    pub fn bool(b: bool) -> BoltType {
        BoltType::Boolean(BoltBoolean::new(b))
    }

    pub fn datetime(dt: DateTime<Utc>) -> BoltType {
        BoltType::DateTime(neo4rs::BoltDateTime::from(dt.fixed_offset()))
    }
}

/// Neo4j-specific field representation
#[derive(Debug, Clone)]
pub struct Neo4jField {
    pub property_name: String,            // Neo4j property name
    pub property_value: neo4rs::BoltType, // Neo4j-specific representation using native BoltType
}

/// Convert BoltType to a format suitable for Neo4j Cypher query literals
/// This generates Cypher-compatible syntax for inline values in queries
///
/// For simple types (Int, Float, Bool, String), we use native Cypher literals.
/// For complex types (Lists, Maps), we generate Cypher collection/map syntax.
///
/// This uses neo4rs::BoltType directly for proper Neo4j-native representations.
#[allow(dead_code)]
fn bolttype_to_cypher_literal(value: &BoltType) -> String {
    match value {
        // Simple types - Cypher literals
        BoltType::Null(_) => "null".to_string(),
        BoltType::Boolean(b) => b.value.to_string(),
        BoltType::Integer(i) => i.value.to_string(),
        BoltType::Float(f) => f.value.to_string(),
        BoltType::String(s) => format!("'{}'", s.value.replace('\'', "\\'")), // Escape single quotes
        BoltType::Bytes(b) => {
            // Store as base64 string in Cypher
            format!(
                "'{}'",
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &b.value)
            )
        }
        // Lists - Cypher list syntax: [item1, item2, ...]
        BoltType::List(list) => {
            let items: Vec<String> = list.value.iter().map(bolttype_to_cypher_literal).collect();
            format!("[{}]", items.join(", "))
        }
        // Maps - Cypher map syntax: {key1: value1, key2: value2, ...}
        BoltType::Map(map) => {
            let items: Vec<String> = map
                .value
                .iter()
                .map(|(k, v)| format!("{}: {}", k, bolttype_to_cypher_literal(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        // Date/Time types - Cypher date/time/datetime functions
        BoltType::Date(d) => {
            // Convert to NaiveDate - clone to get owned value for conversion
            let naive_date: chrono::NaiveDate = d.clone().try_into().expect("Invalid date");
            format!("date('{naive_date}')")
        }
        BoltType::Time(t) => {
            // BoltTime doesn't have direct conversion, use string representation
            format!("time('{t:?}')")
        }
        BoltType::LocalTime(t) => {
            let naive_time: chrono::NaiveTime = t.clone().into();
            format!("localtime('{naive_time}')")
        }
        BoltType::DateTime(dt) => {
            // BoltDateTime doesn't have direct conversion, use string representation
            format!("datetime('{dt:?}')")
        }
        BoltType::LocalDateTime(ldt) => {
            let naive_dt: chrono::NaiveDateTime =
                ldt.clone().try_into().expect("Invalid local datetime");
            format!("localdatetime('{naive_dt}')")
        }
        BoltType::DateTimeZoneId(dt) => {
            // Use string representation for datetime with zone
            format!("datetime('{dt:?}')")
        }
        // Point - Cypher point function
        BoltType::Point2D(point) => {
            format!(
                "point({{x: {}, y: {}, srid: {}}})",
                point.x.value, point.y.value, point.sr_id.value
            )
        }
        BoltType::Point3D(point) => {
            format!(
                "point({{x: {}, y: {}, z: {}, srid: {}}})",
                point.x.value, point.y.value, point.z.value, point.sr_id.value
            )
        }
        // Duration - Cypher duration function
        BoltType::Duration(duration) => {
            // Convert to std::time::Duration - clone to get owned value
            let std_duration: std::time::Duration = duration.clone().into();
            let secs = std_duration.as_secs();
            let nanos = std_duration.subsec_nanos();
            // Note: std::time::Duration doesn't have months/days fields
            // For full fidelity, we'd need to use ISO 8601 duration string
            format!("duration({{seconds: {secs}, nanoseconds: {nanos}}})")
        }
        // Complex graph types - these should not appear in property values
        BoltType::Node(node) => {
            // Serialize node as map for storage (should be rare in test data)
            let labels_str: Vec<String> = node.labels.iter().map(|l| format!("'{l}'")).collect();
            let props_str: Vec<String> = node
                .properties
                .value
                .iter()
                .map(|(k, v)| format!("{}: {}", k, bolttype_to_cypher_literal(v)))
                .collect();
            format!(
                "{{id: {}, labels: [{}], properties: {{{}}}}}",
                node.id.value,
                labels_str.join(", "),
                props_str.join(", ")
            )
        }
        BoltType::Relation(rel) => {
            // Serialize relationship as map for storage (should be rare in test data)
            let props_str: Vec<String> = rel
                .properties
                .value
                .iter()
                .map(|(k, v)| format!("{}: {}", k, bolttype_to_cypher_literal(v)))
                .collect();
            format!(
                "{{id: {}, start: {}, end: {}, type: '{}', properties: {{{}}}}}",
                rel.id.value,
                rel.start_node_id.value,
                rel.end_node_id.value,
                rel.typ,
                props_str.join(", ")
            )
        }
        BoltType::UnboundedRelation(rel) => {
            // Unbounded relation (used in Path) - serialize as simplified map
            let props_str: Vec<String> = rel
                .properties
                .value
                .iter()
                .map(|(k, v)| format!("{}: {}", k, bolttype_to_cypher_literal(v)))
                .collect();
            format!(
                "{{id: {}, type: '{}', properties: {{{}}}}}",
                rel.id.value,
                rel.typ,
                props_str.join(", ")
            )
        }
        BoltType::Path(_path) => {
            // Path is complex - for simplicity just return a placeholder
            // Paths are typically query results, not stored property values
            "'[Path]'".to_string()
        }
    }
}

/// Get the label for a table from its schema or fallback to table name
fn get_table_label(table: &TestTable) -> String {
    table
        .schema
        .neo4j
        .node_labels
        .first()
        .unwrap_or(&table.name)
        .clone()
}

/// Create Neo4j schema from TestTable
#[allow(dead_code)]
pub async fn create_neo4j_schema_from_test_table(
    graph: &Graph,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use schema to create constraints and indexes (required field)
    {
        let schema = &table.schema;
        // Create constraints for unique properties
        for constraint in &schema.neo4j.constraints {
            let constraint_cypher = match &constraint.constraint_type {
                crate::testing::schema::Neo4jConstraintType::Unique { label, property } => {
                    format!(
                        "CREATE CONSTRAINT {} IF NOT EXISTS FOR (n:{}) REQUIRE n.{} IS UNIQUE",
                        constraint.name, label, property
                    )
                }
                crate::testing::schema::Neo4jConstraintType::NodeKey { label, properties } => {
                    format!(
                        "CREATE CONSTRAINT {} IF NOT EXISTS FOR (n:{}) REQUIRE ({}) IS NODE KEY",
                        constraint.name,
                        label,
                        properties
                            .iter()
                            .map(|p| format!("n.{p}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
                crate::testing::schema::Neo4jConstraintType::Exists { label, property } => {
                    format!(
                        "CREATE CONSTRAINT {} IF NOT EXISTS FOR (n:{}) REQUIRE n.{} IS NOT NULL",
                        constraint.name, label, property
                    )
                }
            };

            // Execute constraint creation
            graph.run(neo4rs::query(&constraint_cypher)).await?;
        }

        // Create indexes
        for index in &schema.neo4j.indexes {
            let index_cypher = match index.index_type {
                crate::testing::schema::Neo4jIndexType::Range => {
                    format!(
                        "CREATE INDEX {} IF NOT EXISTS FOR (n:{}) ON ({})",
                        index.name,
                        index.label,
                        index
                            .properties
                            .iter()
                            .map(|p| format!("n.{p}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
                crate::testing::schema::Neo4jIndexType::Text => {
                    format!(
                        "CREATE TEXT INDEX {} IF NOT EXISTS FOR (n:{}) ON (n.{})",
                        index.name,
                        index.label,
                        index.properties.first().unwrap_or(&String::new())
                    )
                }
                crate::testing::schema::Neo4jIndexType::Point => {
                    format!(
                        "CREATE POINT INDEX {} IF NOT EXISTS FOR (n:{}) ON (n.{})",
                        index.name,
                        index.label,
                        index.properties.first().unwrap_or(&String::new())
                    )
                }
                crate::testing::schema::Neo4jIndexType::Fulltext => {
                    format!(
                        "CREATE FULLTEXT INDEX {} IF NOT EXISTS FOR (n:{}) ON EACH [{}]",
                        index.name,
                        index.label,
                        index
                            .properties
                            .iter()
                            .map(|p| format!("n.{p}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            };

            // Execute index creation
            graph.run(neo4rs::query(&index_cypher)).await?;
        }
    }

    Ok(())
}

/// Create a single node in Neo4j from a document
async fn create_node_from_document(
    graph: &Graph,
    label: &str,
    neo4j_doc: &std::collections::HashMap<String, BoltType>,
    add_timestamp: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if neo4j_doc.is_empty() && !add_timestamp {
        return Ok(());
    }

    let mut props = Vec::new();

    // Add updated_at timestamp if requested (for incremental sync)
    // This is a Cypher function call, not a parameter
    if add_timestamp {
        props.push("updated_at: datetime()".to_string());
    }

    // Build property list with parameter placeholders
    // Each property will be bound using .param() to preserve types
    for key in neo4j_doc.keys() {
        props.push(format!("{key}: ${key}"));
    }

    // Build the CREATE statement
    let create_cypher = if props.is_empty() {
        format!("CREATE (n:{label})")
    } else {
        format!("CREATE (n:{label} {{{}}})", props.join(", "))
    };

    let mut query = neo4rs::query(&create_cypher);
    for (key, value) in neo4j_doc {
        query = query.param(key, value.clone());
    }

    graph.run(query).await?;
    Ok(())
}

/// Inject TestTable data into Neo4j
#[allow(dead_code)]
pub async fn inject_test_table_neo4j(
    graph: &Graph,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    // First create the schema if defined
    create_neo4j_schema_from_test_table(graph, table).await?;

    if table.documents.is_empty() {
        return Ok(());
    }

    let label = get_table_label(table);

    // Insert data for each document
    for doc in &table.documents {
        let neo4j_doc = doc.to_neo4j_doc();
        create_node_from_document(graph, &label, &neo4j_doc, false).await?;
    }

    Ok(())
}

/// Inject only data into Neo4j (assumes schema/constraints already exist)
/// This is used for incremental sync tests where schema is created separately
#[allow(dead_code)]
pub async fn inject_test_table_data_only_neo4j(
    graph: &Graph,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    if table.documents.is_empty() {
        return Ok(());
    }

    let label = get_table_label(table);

    // Insert data for each document with timestamp for incremental tracking
    for doc in &table.documents {
        let neo4j_doc = doc.to_neo4j_doc();
        create_node_from_document(graph, &label, &neo4j_doc, true).await?;
    }

    Ok(())
}

/// Create Neo4j schema for all tables in a dataset
/// This creates constraints and indexes for all tables
#[allow(dead_code)]
pub async fn create_constraints_and_indices(
    graph: &Graph,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        create_neo4j_schema_from_test_table(graph, table).await?;
    }
    Ok(())
}

/// Create nodes for all tables in a dataset
/// This inserts all test data with timestamps for incremental sync tracking
#[allow(dead_code)]
pub async fn create_nodes(
    graph: &Graph,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        inject_test_table_data_only_neo4j(graph, table).await?;
    }
    Ok(())
}
