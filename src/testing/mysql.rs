//! MySQL-specific field definitions and data injection functions

use crate::testing::{table::TestTable, value::MySQLValue};
use chrono::{Datelike, Timelike};
use mysql_async::prelude::*;

#[derive(Clone)]
pub struct MySQLConfig {
    connection_string: String,
}

impl MySQLConfig {
    pub fn get_connection_string(&self) -> String {
        self.connection_string.clone()
    }
}

pub fn create_mysql_config() -> MySQLConfig {
    let connection_string = std::env::var("MYSQL_TEST_URL")
        .unwrap_or_else(|_| "mysql://root:root@mysql:3306/testdb".to_string());
    MySQLConfig { connection_string }
}

pub async fn cleanup_mysql_test_data(
    conn: &mut mysql_async::Conn,
) -> Result<(), Box<dyn std::error::Error>> {
    // Default tables to clean up for test data
    let test_tables = [
        "posts",
        "users",
        "test_users",
        "test_products",
        "test_orders",
    ];

    // Use centralized cleanup function
    crate::testing::mysql_cleanup::full_cleanup(conn, &test_tables).await?;
    Ok(())
}

/// MySQL-specific field representation
#[derive(Debug, Clone)]
pub struct MySQLField {
    pub column_name: String,      // MySQL column name
    pub column_value: MySQLValue, // MySQL-specific representation
    pub data_type: String,        // "ENUM", "SET", "YEAR", etc.
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    pub enum_values: Option<Vec<String>>, // For ENUM/SET types
}

/// Convert MySQLValue to mysql_async::Value for prepared statements
#[allow(dead_code)]
pub fn mysql_value_to_param(value: &MySQLValue) -> mysql_async::Value {
    match value {
        MySQLValue::Null => mysql_async::Value::NULL,
        MySQLValue::TinyInt(i) => mysql_async::Value::Int(*i as i64),
        MySQLValue::SmallInt(i) => mysql_async::Value::Int(*i as i64),
        MySQLValue::MediumInt(i) => mysql_async::Value::Int(*i as i64),
        MySQLValue::Int(i) => mysql_async::Value::Int(*i as i64),
        MySQLValue::BigInt(i) => mysql_async::Value::Int(*i),
        MySQLValue::Float(f) => mysql_async::Value::Float(*f),
        MySQLValue::Double(f) => mysql_async::Value::Double(*f),
        MySQLValue::Decimal { value, .. } => mysql_async::Value::Bytes(value.as_bytes().to_vec()),
        MySQLValue::Char(s) | MySQLValue::Varchar(s) | MySQLValue::Text(s) => {
            mysql_async::Value::Bytes(s.as_bytes().to_vec())
        }
        MySQLValue::Binary(b) | MySQLValue::VarBinary(b) | MySQLValue::Blob(b) => {
            mysql_async::Value::Bytes(b.clone())
        }
        MySQLValue::Date(d) => {
            mysql_async::Value::Date(d.year() as u16, d.month() as u8, d.day() as u8, 0, 0, 0, 0)
        }
        MySQLValue::Time(t) => mysql_async::Value::Time(
            false,
            0,
            t.hour() as u8,
            t.minute() as u8,
            t.second() as u8,
            0,
        ),
        MySQLValue::DateTime(dt) | MySQLValue::Timestamp(dt) => mysql_async::Value::Date(
            dt.year() as u16,
            dt.month() as u8,
            dt.day() as u8,
            dt.hour() as u8,
            dt.minute() as u8,
            dt.second() as u8,
            0,
        ),
        MySQLValue::Enum(e) => mysql_async::Value::Bytes(e.as_bytes().to_vec()),
        MySQLValue::Set(set_vals) => {
            let set_str = set_vals.join(",");
            mysql_async::Value::Bytes(set_str.as_bytes().to_vec())
        }
        MySQLValue::Year(y) => mysql_async::Value::UInt(*y as u64),
        MySQLValue::Bit(bits) => {
            let bit_val: u64 = bits
                .iter()
                .enumerate()
                .map(|(i, &b)| if b { 1u64 << i } else { 0 })
                .sum();
            mysql_async::Value::UInt(bit_val)
        }
        MySQLValue::Json(j) => mysql_async::Value::Bytes(j.to_string().as_bytes().to_vec()),
        MySQLValue::Geometry(g) => mysql_async::Value::Bytes(g.as_bytes().to_vec()),
    }
}

/// Generate MySQL column definition from field
#[allow(dead_code)]
pub fn generate_mysql_column_def(field: &MySQLField) -> String {
    let mut def = format!("{} {}", field.column_name, field.data_type);

    match field.data_type.as_str() {
        "VARCHAR" | "CHAR" => {
            // VARCHAR and CHAR require a size
            def.push_str("(255)");
        }
        "DECIMAL" | "NUMERIC" => {
            if let (Some(precision), Some(scale)) = (field.precision, field.scale) {
                def.push_str(&format!("({precision},{scale})"));
            }
        }
        "ENUM" | "SET" => {
            if let Some(values) = &field.enum_values {
                let value_list = values
                    .iter()
                    .map(|v| format!("'{v}'"))
                    .collect::<Vec<_>>()
                    .join(",");
                def.push_str(&format!("({value_list})"));
            }
        }
        _ => {}
    }

    def
}

/// Create MySQL table from TestTable schema
#[allow(dead_code)]
pub async fn create_mysql_table_from_test_table(
    conn: &mut mysql_async::Conn,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    // Drop table if it exists
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table.name);
    conn.query_drop(&drop_sql).await?;

    // Use schema to create table (required field, no fallback)
    let create_sql = table.schema.mysql.to_create_table_ddl(&table.name);
    eprintln!("DEBUG: Creating MySQL table with schema SQL: {create_sql}");
    conn.query_drop(&create_sql).await?;

    // Create indexes if defined
    for index in &table.schema.mysql.indexes {
        let index_sql = table.schema.mysql.to_create_index_ddl(&table.name, index);
        conn.query_drop(&index_sql).await?;
    }
    Ok(())
}

/// Inject TestTable data into MySQL
#[allow(dead_code)]
pub async fn inject_test_table_mysql(
    conn: &mut mysql_async::Conn,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    inject_test_table_data_only_mysql(conn, table).await
}

/// Inject TestTable data into MySQL WITHOUT creating the table
/// Use this when table already exists (e.g., with triggers)
#[allow(dead_code)]
pub async fn inject_test_table_data_only_mysql(
    conn: &mut mysql_async::Conn,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    if table.documents.is_empty() {
        return Ok(());
    }

    // Insert data for each document
    for doc in &table.documents {
        let mysql_doc = doc.to_mysql_doc();

        if mysql_doc.is_empty() {
            continue;
        }

        let columns: Vec<String> = mysql_doc.keys().cloned().collect();
        let placeholders: Vec<String> = (0..columns.len()).map(|_| "?".to_string()).collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table.name,
            columns.join(", "),
            placeholders.join(", ")
        );

        let params: Vec<mysql_async::Value> = columns
            .iter()
            .map(|col| mysql_value_to_param(&mysql_doc[col]))
            .collect();

        conn.exec_drop(&insert_sql, params).await?;
    }

    Ok(())
}

pub async fn create_tables_and_indices(
    conn: &mut mysql_async::Conn,
    dataset: &crate::testing::table::TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        create_mysql_table_from_test_table(conn, table).await?;
    }
    Ok(())
}

/// Insert all rows from a dataset into MySQL
///
/// This function iterates through all tables in the dataset and injects
/// their data into MySQL, creating the tables if they don't exist.
pub async fn insert_rows(
    conn: &mut mysql_async::Conn,
    dataset: &crate::testing::table::TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        inject_test_table_mysql(conn, table).await?;
    }
    Ok(())
}
