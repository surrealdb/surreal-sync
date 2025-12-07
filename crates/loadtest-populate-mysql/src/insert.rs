//! Batched INSERT logic for MySQL population.

use crate::error::MySQLPopulatorError;
use mysql_async::{prelude::*, Params, Pool, Value};
use mysql_types::forward::MySQLValue;
use sync_core::{Schema, TableDefinition, TypedValue, UniversalRow};

/// Default batch size for INSERT operations.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Insert a batch of rows into a MySQL table.
pub async fn insert_batch(
    pool: &Pool,
    table_schema: &TableDefinition,
    rows: &[UniversalRow],
) -> Result<u64, MySQLPopulatorError> {
    if rows.is_empty() {
        return Ok(0);
    }

    let mut conn = pool.get_conn().await?;

    // Build column list: id + all fields
    let mut columns: Vec<String> = vec!["id".to_string()];
    columns.extend(table_schema.field_names().iter().map(|s| s.to_string()));

    // Build INSERT statement with placeholders
    let col_placeholders: Vec<&str> = columns.iter().map(|_| "?").collect();
    let row_template = format!("({})", col_placeholders.join(", "));
    let rows_template: Vec<&str> = (0..rows.len()).map(|_| row_template.as_str()).collect();

    let sql = format!(
        "INSERT INTO `{}` ({}) VALUES {}",
        table_schema.name,
        columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", "),
        rows_template.join(", ")
    );

    // Build parameters
    let mut params: Vec<Value> = Vec::with_capacity(rows.len() * columns.len());

    for row in rows {
        // Add the ID
        let id_typed = TypedValue::try_with_type(table_schema.id.id_type.clone(), row.id.clone())
            .expect("generator produced invalid type-value combination for id");
        let id_mysql: MySQLValue = id_typed.into();
        params.push(id_mysql.into_inner());

        // Add each field in order
        for field_schema in &table_schema.fields {
            let field_value = row.get_field(&field_schema.name);
            let typed_value = match field_value {
                Some(value) => {
                    TypedValue::try_with_type(field_schema.field_type.clone(), value.clone())
                        .expect("generator produced invalid type-value combination for field")
                }
                None => TypedValue::null(field_schema.field_type.clone()),
            };
            let mysql_value: MySQLValue = typed_value.into();
            params.push(mysql_value.into_inner());
        }
    }

    // Execute the INSERT
    conn.exec_drop(&sql, Params::Positional(params)).await?;

    Ok(rows.len() as u64)
}

/// Insert rows one at a time (useful for debugging or when batch fails).
#[allow(dead_code)]
pub async fn insert_single(
    pool: &Pool,
    table_schema: &TableDefinition,
    row: &UniversalRow,
) -> Result<u64, MySQLPopulatorError> {
    let mut conn = pool.get_conn().await?;

    // Build column list: id + all fields
    let mut columns: Vec<String> = vec!["id".to_string()];
    columns.extend(table_schema.field_names().iter().map(|s| s.to_string()));

    // Build INSERT statement
    let placeholders: Vec<&str> = columns.iter().map(|_| "?").collect();
    let sql = format!(
        "INSERT INTO `{}` ({}) VALUES ({})",
        table_schema.name,
        columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );

    // Build parameters
    let mut params: Vec<Value> = Vec::with_capacity(columns.len());

    // Add the ID
    let id_typed = TypedValue::try_with_type(table_schema.id.id_type.clone(), row.id.clone())
        .expect("generator produced invalid type-value combination for id");
    let id_mysql: MySQLValue = id_typed.into();
    params.push(id_mysql.into_inner());

    // Add each field in order
    for field_schema in &table_schema.fields {
        let field_value = row.get_field(&field_schema.name);
        let typed_value = match field_value {
            Some(value) => {
                TypedValue::try_with_type(field_schema.field_type.clone(), value.clone())
                    .expect("generator produced invalid type-value combination for field")
            }
            None => TypedValue::null(field_schema.field_type.clone()),
        };
        let mysql_value: MySQLValue = typed_value.into();
        params.push(mysql_value.into_inner());
    }

    // Execute the INSERT
    conn.exec_drop(&sql, Params::Positional(params)).await?;

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

    let ddl = mysql_types::ddl::MySQLDdl;
    Some(ddl.to_create_table_with_pk(table_name, "id", &table_schema.id.id_type, &columns))
}

/// Generate DROP TABLE statement.
pub fn generate_drop_table(table_name: &str) -> String {
    format!("DROP TABLE IF EXISTS `{table_name}`")
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

        assert!(sql.contains("CREATE TABLE `users`"));
        assert!(sql.contains("`id` CHAR(36) NOT NULL"));
        assert!(sql.contains("`email` VARCHAR(255) NOT NULL"));
        assert!(sql.contains("`age` INT NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (`id`)"));
    }

    #[test]
    fn test_generate_drop_table() {
        let sql = generate_drop_table("users");
        assert_eq!(sql, "DROP TABLE IF EXISTS `users`");
    }
}
