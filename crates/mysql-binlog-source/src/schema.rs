//! MySQL schema collection for type-aware conversion.

use std::collections::HashMap;

use anyhow::Result;
use mysql_async::prelude::*;
use mysql_types::mysql_column_to_universal_type;
use sync_core::{ColumnDefinition, DatabaseSchema, TableDefinition, Type};

/// Collect schema information for all tables in the current database.
pub async fn collect_mysql_database_schema(conn: &mut mysql_async::Conn) -> Result<DatabaseSchema> {
    let columns_query = "
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let column_rows: Vec<mysql_async::Row> = conn.query(columns_query).await?;

    let pk_query = "
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE CONSTRAINT_NAME = 'PRIMARY'
            AND TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME, ORDINAL_POSITION";

    let pk_rows: Vec<mysql_async::Row> = conn.query(pk_query).await?;

    let json_columns = {
        let current_db: Option<String> = conn.query_first("SELECT DATABASE()").await?;
        match current_db {
            Some(db) => {
                surreal_sync_mysql_trigger_source::json_columns::get_json_columns(conn, &db).await?
            }
            None => HashMap::new(),
        }
    };

    let mut pk_columns: HashMap<String, Vec<String>> = HashMap::new();
    for row in pk_rows {
        let table_name: String = row.get(0).ok_or_else(|| anyhow::anyhow!("missing table"))?;
        let column_name: String = row
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("missing column"))?;
        pk_columns.entry(table_name).or_default().push(column_name);
    }

    let mut table_columns: HashMap<String, Vec<(String, Type)>> = HashMap::new();
    for row in column_rows {
        let table_name: String = row.get(0).ok_or_else(|| anyhow::anyhow!("missing table"))?;
        let column_name: String = row
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("missing column"))?;
        let data_type: String = row
            .get(2)
            .ok_or_else(|| anyhow::anyhow!("missing data type"))?;
        let column_type: String = row
            .get(3)
            .ok_or_else(|| anyhow::anyhow!("missing column type"))?;
        let precision: Option<u32> = row.get::<Option<u32>, _>(4).unwrap_or(None);
        let scale: Option<u32> = row.get::<Option<u32>, _>(5).unwrap_or(None);

        let is_json = json_columns
            .get(&table_name)
            .is_some_and(|cols| cols.contains(&column_name));
        let universal_type = if is_json {
            Type::Json
        } else {
            mysql_column_to_universal_type(&data_type, &column_type, precision, scale)
        };

        table_columns
            .entry(table_name)
            .or_default()
            .push((column_name, universal_type));
    }

    let mut tables = Vec::new();
    for (table_name, columns) in table_columns {
        let pk_list = pk_columns
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| vec!["id".to_string()]);
        let pk_col_name = pk_list[0].clone();

        let mut primary_key = None;
        let mut other_columns = Vec::new();
        for (col_name, col_type) in columns {
            if col_name == pk_col_name {
                primary_key = Some(ColumnDefinition::new(col_name, col_type));
            } else {
                other_columns.push(ColumnDefinition::new(col_name, col_type));
            }
        }

        let pk = primary_key.unwrap_or_else(|| ColumnDefinition::new(pk_col_name, Type::Int64));
        let mut table_def = TableDefinition::new(table_name, pk, other_columns);
        if pk_list.len() > 1 {
            table_def.composite_primary_key = Some(pk_list);
        }
        tables.push(table_def);
    }

    Ok(DatabaseSchema::new(tables))
}

/// Column names in `ORDINAL_POSITION` order (matches binlog table maps).
pub async fn get_table_column_names_ordinal(
    conn: &mut mysql_async::Conn,
    table: &str,
) -> Result<Vec<String>> {
    let rows: Vec<mysql_async::Row> = conn
        .exec(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (table,),
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.get::<String, _>(0))
        .collect())
}
