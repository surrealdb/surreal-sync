//! Batched INSERT logic for PostgreSQL population.

use crate::error::PostgreSQLPopulatorError;
use postgresql_types::forward::PostgreSQLValue;
use sync_core::{Schema, TableDefinition, TypedValue, UniversalRow};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

/// Default batch size for INSERT operations.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Insert a batch of rows into a PostgreSQL table.
pub async fn insert_batch(
    client: &Client,
    table_schema: &TableDefinition,
    rows: &[UniversalRow],
) -> Result<u64, PostgreSQLPopulatorError> {
    if rows.is_empty() {
        return Ok(0);
    }

    // Build column list: id + all fields
    let mut columns: Vec<String> = vec!["id".to_string()];
    columns.extend(table_schema.field_names().iter().map(|s| s.to_string()));

    // Build INSERT statement with placeholders
    let col_count = columns.len();
    let mut placeholders: Vec<String> = Vec::new();
    let mut param_idx = 1;

    for _ in 0..rows.len() {
        let row_placeholders: Vec<String> = (0..col_count)
            .map(|_| {
                let p = format!("${param_idx}");
                param_idx += 1;
                p
            })
            .collect();
        placeholders.push(format!("({})", row_placeholders.join(", ")));
    }

    let sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES {}",
        table_schema.name,
        columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );

    // Build parameters - we need to collect all values and create boxed trait objects
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

    for row in rows {
        // Add the ID
        let id_typed = TypedValue::with_type(table_schema.id.id_type.clone(), row.id.clone());
        let id_pg: PostgreSQLValue = id_typed.into();
        params.push(pg_value_to_boxed(id_pg));

        // Add each field in order
        for field_schema in &table_schema.fields {
            let field_value = row.get_field(&field_schema.name);
            let typed_value = match field_value {
                Some(value) => {
                    TypedValue::with_type(field_schema.field_type.clone(), value.clone())
                }
                None => TypedValue::null(field_schema.field_type.clone()),
            };
            let pg_value: PostgreSQLValue = typed_value.into();
            params.push(pg_value_to_boxed(pg_value));
        }
    }

    // Convert to references for execution
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    // Execute the INSERT
    client.execute(&sql, &param_refs).await?;

    Ok(rows.len() as u64)
}

/// Convert a PostgreSQLValue to a boxed ToSql trait object.
fn pg_value_to_boxed(value: PostgreSQLValue) -> Box<dyn ToSql + Sync + Send> {
    match value {
        PostgreSQLValue::Null => Box::new(None::<String>),
        PostgreSQLValue::Bool(b) => Box::new(b),
        PostgreSQLValue::Int16(i) => Box::new(i),
        PostgreSQLValue::Int32(i) => Box::new(i),
        PostgreSQLValue::Int64(i) => Box::new(i),
        PostgreSQLValue::Float32(f) => Box::new(f),
        PostgreSQLValue::Float64(f) => Box::new(f),
        PostgreSQLValue::Decimal(d) => Box::new(d),
        PostgreSQLValue::Text(s) => Box::new(s),
        PostgreSQLValue::Bytes(b) => Box::new(b),
        PostgreSQLValue::Uuid(u) => Box::new(u),
        PostgreSQLValue::Date(d) => Box::new(d),
        PostgreSQLValue::Time(t) => Box::new(t),
        PostgreSQLValue::Timestamp(ts) => Box::new(ts),
        PostgreSQLValue::TimestampTz(ts) => Box::new(ts),
        PostgreSQLValue::Json(j) => Box::new(j),
        PostgreSQLValue::TextArray(arr) => Box::new(arr),
        PostgreSQLValue::Int32Array(arr) => Box::new(arr),
        PostgreSQLValue::Int64Array(arr) => Box::new(arr),
        PostgreSQLValue::Float64Array(arr) => Box::new(arr),
        PostgreSQLValue::BoolArray(arr) => Box::new(arr),
        PostgreSQLValue::Point(x, y) => {
            // PostgreSQL POINT is typically represented as (x, y)
            // Using geo_types::Point for tokio-postgres
            Box::new(geo_types::Point::new(x, y))
        }
    }
}

/// Insert a single row (useful for debugging or when batch fails).
#[allow(dead_code)]
pub async fn insert_single(
    client: &Client,
    table_schema: &TableDefinition,
    row: &UniversalRow,
) -> Result<u64, PostgreSQLPopulatorError> {
    // Build column list: id + all fields
    let mut columns: Vec<String> = vec!["id".to_string()];
    columns.extend(table_schema.field_names().iter().map(|s| s.to_string()));

    // Build INSERT statement with placeholders
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();

    let sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES ({})",
        table_schema.name,
        columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );

    // Build parameters
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

    // Add the ID
    let id_typed = TypedValue::with_type(table_schema.id.id_type.clone(), row.id.clone());
    let id_pg: PostgreSQLValue = id_typed.into();
    params.push(pg_value_to_boxed(id_pg));

    // Add each field in order
    for field_schema in &table_schema.fields {
        let field_value = row.get_field(&field_schema.name);
        let typed_value = match field_value {
            Some(value) => TypedValue::with_type(field_schema.field_type.clone(), value.clone()),
            None => TypedValue::null(field_schema.field_type.clone()),
        };
        let pg_value: PostgreSQLValue = typed_value.into();
        params.push(pg_value_to_boxed(pg_value));
    }

    // Convert to references for execution
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    // Execute the INSERT
    client.execute(&sql, &param_refs).await?;

    Ok(1)
}

/// Generate CREATE TABLE statement from schema.
pub fn generate_create_table(schema: &Schema, table_name: &str) -> Option<String> {
    let table_schema = schema.get_table(table_name)?;

    // Build column definitions
    let columns: Vec<(String, sync_core::UniversalType, bool)> = table_schema
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.field_type.clone(), f.nullable))
        .collect();

    let ddl = postgresql_types::ddl::PostgreSQLDdl;
    Some(ddl.to_create_table_with_pk(table_name, "id", &table_schema.id.id_type, &columns))
}

/// Generate DROP TABLE statement.
pub fn generate_drop_table(table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS \"{table_name}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let yaml = r#"
version: 1
seed: 42
tables:
  - name: users
    id:
      type: uuid
      generator:
        type: uuid_v4
    fields:
      - name: email
        type:
          type: var_char
          length: 255
        generator:
          type: pattern
          pattern: "user_{index}@example.com"
      - name: age
        type: int
        generator:
          type: int_range
          min: 18
          max: 80
"#;
        Schema::from_yaml(yaml).unwrap()
    }

    #[test]
    fn test_generate_create_table() {
        let schema = test_schema();
        let sql = generate_create_table(&schema, "users").unwrap();

        assert!(sql.contains("CREATE TABLE \"users\""));
        assert!(sql.contains("\"id\" UUID NOT NULL"));
        assert!(sql.contains("\"email\" VARCHAR(255) NOT NULL"));
        assert!(sql.contains("\"age\" INTEGER NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_generate_drop_table() {
        let sql = generate_drop_table("users");
        assert_eq!(sql, "DROP TABLE IF EXISTS \"users\"");
    }
}
