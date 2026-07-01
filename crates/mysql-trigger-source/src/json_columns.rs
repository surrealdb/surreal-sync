//! JSON-column detection shared by MySQL and MariaDB.
//!
//! MySQL exposes a native `JSON` type: such columns report `DATA_TYPE = 'json'`
//! in `information_schema.COLUMNS` and arrive over the wire as
//! `MYSQL_TYPE_JSON`. MariaDB instead implements `JSON` as a `LONGTEXT` alias
//! guarded by an auto-generated `json_valid(...)` `CHECK` constraint, so those
//! columns report `DATA_TYPE = 'longtext'` and arrive as `MYSQL_TYPE_BLOB`.
//!
//! To make both engines behave identically for nested JSON we detect JSON
//! columns here (native `json` columns UNION MariaDB `json_valid`-checked
//! columns) and reuse that list to (a) force JSON conversion on read
//! (`RowConversionConfig::json_columns`) and (b) wrap trigger values in
//! `JSON_EXTRACT(..., '$')` so audit rows nest real JSON instead of an escaped
//! string.

use anyhow::Result;
use mysql_async::{prelude::*, Row};
use std::collections::HashMap;
use tracing::debug;

/// Detect JSON columns per table for the given database, working for both MySQL
/// and MariaDB.
///
/// Returns a map from table name to the names of its JSON columns.
pub async fn get_json_columns(
    conn: &mut mysql_async::Conn,
    database: &str,
) -> Result<HashMap<String, Vec<String>>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();

    // Native JSON columns (MySQL). Works everywhere; MariaDB simply reports no
    // rows here because it stores JSON as `longtext`.
    let native: Vec<Row> = conn
        .exec(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND DATA_TYPE = 'json'",
            (database,),
        )
        .await?;
    for row in native {
        record_json_column(&mut out, &row);
    }

    // MariaDB: JSON is a `LONGTEXT` alias backed by a `json_valid(...)` CHECK
    // constraint. `information_schema.CHECK_CONSTRAINTS` carries `TABLE_NAME` on
    // MariaDB but not on MySQL, so this query only succeeds (and is only
    // relevant) on MariaDB; on MySQL it errors and is safely ignored because
    // native JSON was already collected above.
    let mariadb: std::result::Result<Vec<Row>, _> = conn
        .exec(
            "SELECT cc.TABLE_NAME, col.COLUMN_NAME \
             FROM information_schema.CHECK_CONSTRAINTS cc \
             JOIN information_schema.COLUMNS col \
               ON col.TABLE_SCHEMA = cc.CONSTRAINT_SCHEMA \
              AND col.TABLE_NAME = cc.TABLE_NAME \
             WHERE cc.CONSTRAINT_SCHEMA = ? \
               AND cc.CHECK_CLAUSE LIKE CONCAT('%json_valid(`', col.COLUMN_NAME, '`)%')",
            (database,),
        )
        .await;
    match mariadb {
        Ok(rows) => {
            for row in rows {
                record_json_column(&mut out, &row);
            }
        }
        Err(e) => {
            debug!("json_valid CHECK-constraint detection skipped (not MariaDB?): {e}");
        }
    }

    Ok(out)
}

/// Record a `(TABLE_NAME, COLUMN_NAME)` detection row into the output map,
/// avoiding duplicates (a column could be reported by both queries).
fn record_json_column(out: &mut HashMap<String, Vec<String>>, row: &Row) {
    let table: Option<String> = row.get(0);
    let column: Option<String> = row.get(1);
    if let (Some(table), Some(column)) = (table, column) {
        let cols = out.entry(table).or_default();
        if !cols.contains(&column) {
            cols.push(column);
        }
    }
}

/// Build the SQL expression used for a column's value inside a trigger's
/// `JSON_OBJECT(...)`.
///
/// For JSON columns the raw reference is wrapped as `JSON_EXTRACT(<alias>.<col>,
/// '$')` so the value nests as real JSON in the audit `new_data`/`old_data`
/// object on both engines. On MariaDB a bare `LONGTEXT`-backed JSON column would
/// otherwise be escaped into a JSON string; wrapping native MySQL JSON columns
/// the same way is a no-op, keeping one code path for both engines.
pub fn json_object_value_expr(row_alias: &str, column: &str, json_columns: &[String]) -> String {
    if json_columns.iter().any(|c| c == column) {
        format!("JSON_EXTRACT({row_alias}.{column}, '$')")
    } else {
        format!("{row_alias}.{column}")
    }
}
