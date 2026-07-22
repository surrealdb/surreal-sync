//! Table discovery for a Snowflake schema.

use anyhow::Result;

use crate::client::SnowflakeClient;

/// List base-table names in `database.schema` via `INFORMATION_SCHEMA.TABLES`.
///
/// Snowflake stores unquoted identifiers upper-cased, so `schema` is upper-cased
/// before matching. Views and temporary tables are excluded (`TABLE_TYPE = 'BASE TABLE'`).
pub async fn list_tables(
    client: &SnowflakeClient,
    database: &str,
    schema: &str,
) -> Result<Vec<String>> {
    let db = database.to_ascii_uppercase();
    let schema_upper = schema.to_ascii_uppercase();
    let sql = format!(
        "SELECT TABLE_NAME FROM \"{db}\".INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_SCHEMA = '{schema_upper}' AND TABLE_TYPE = 'BASE TABLE' \
         ORDER BY TABLE_NAME"
    );

    let result = client.execute_query(&sql).await?;
    let tables = result
        .rows
        .iter()
        .filter_map(|row| row.first())
        .filter_map(|cell| cell.as_str())
        .map(|s| s.to_string())
        .collect();
    Ok(tables)
}
