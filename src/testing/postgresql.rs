//! PostgreSQL-specific field definitions and data injection functions

use crate::testing::{
    table::{TestDataSet, TestTable},
    value::PostgreSQLValue,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio_postgres::{types::ToSql, Client};
use uuid::Uuid;

#[derive(Clone)]
pub struct PostgresConfig {
    connection_string: String,
}

impl PostgresConfig {
    pub fn get_connection_string(&self) -> String {
        self.connection_string.clone()
    }
}

pub fn create_postgres_config() -> PostgresConfig {
    let connection_string = std::env::var("POSTGRESQL_TEST_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@postgresql:5432/testdb".to_string());
    PostgresConfig { connection_string }
}

pub async fn cleanup_postgresql_test_data(
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    // More aggressive cleanup to handle parallel test contamination
    let tables = [
        "posts",
        "users",
        "surreal_sync_changes",
        "test_users",
        "test_products",
    ];
    for table in &tables {
        let _ = client
            .execute(&format!("DROP TABLE IF EXISTS {table} CASCADE"), &[])
            .await;
    }

    // Also drop any triggers and functions
    let _ = client
        .execute(
            "DROP FUNCTION IF EXISTS surreal_sync_track_changes() CASCADE",
            &[],
        )
        .await;

    Ok(())
}

/// PostgreSQL-specific field representation
#[derive(Debug, Clone)]
pub struct PostgreSQLField {
    pub column_name: String,           // PostgreSQL column name
    pub column_value: PostgreSQLValue, // PostgreSQL-specific representation
    pub data_type: String,             // "UUID", "JSONB", "INET", etc.
    pub precision: Option<u32>,
    pub scale: Option<u32>,
}

/// Convert PostgreSQLValue to a PostgreSQL parameter for prepared statements
pub fn postgresql_value_to_param(value: &PostgreSQLValue) -> anyhow::Result<Box<dyn ToSql + Sync>> {
    match value {
        PostgreSQLValue::Null => Ok(Box::new(None::<i32>)),
        PostgreSQLValue::Bool(b) => Ok(Box::new(*b)),
        PostgreSQLValue::SmallInt(i) => Ok(Box::new(*i)),
        PostgreSQLValue::Int(i) => Ok(Box::new(*i)),
        PostgreSQLValue::BigInt(i) => Ok(Box::new(*i)),
        PostgreSQLValue::Real(f) => Ok(Box::new(*f)),
        PostgreSQLValue::Double(f) => Ok(Box::new(*f)),
        PostgreSQLValue::Text(s) | PostgreSQLValue::Varchar(s) | PostgreSQLValue::Char(s) => {
            Ok(Box::new(s.clone()))
        }
        PostgreSQLValue::Bytea(b) => Ok(Box::new(b.clone())),
        PostgreSQLValue::Date(d) => Ok(Box::new(*d)),
        PostgreSQLValue::Time(t) => Ok(Box::new(*t)),
        PostgreSQLValue::Timestamp(ts) => Ok(Box::new(*ts)),
        PostgreSQLValue::TimestampTz(ts) => Ok(Box::new(*ts)),
        PostgreSQLValue::Uuid(u) => {
            Ok(Box::new(Uuid::parse_str(u).unwrap_or_else(|_| Uuid::nil())))
        }
        PostgreSQLValue::Json(j) | PostgreSQLValue::Jsonb(j) => Ok(Box::new(j.clone())),
        PostgreSQLValue::Inet(ip) | PostgreSQLValue::Cidr(ip) | PostgreSQLValue::MacAddr(ip) => {
            Ok(Box::new(ip.clone()))
        }
        PostgreSQLValue::Interval(i) => Ok(Box::new(i.clone())),
        PostgreSQLValue::Money(m) => Ok(Box::new(m.clone())),
        PostgreSQLValue::Numeric { value, .. } => {
            Ok(Box::new(Decimal::from_str(value).unwrap_or(Decimal::ZERO)))
        }
        PostgreSQLValue::Range(r) => Ok(Box::new(r.clone())),
        // Complex types converted to strings for simplicity
        PostgreSQLValue::Point { x, y } => Ok(Box::new(format!("({x},{y})"))),
        PostgreSQLValue::Path(points) => {
            let path_str = points
                .iter()
                .map(|(x, y)| format!("({x},{y})"))
                .collect::<Vec<_>>()
                .join(",");
            Ok(Box::new(format!("[{path_str}]")))
        }
        PostgreSQLValue::Polygon(points) => {
            let poly_str = points
                .iter()
                .map(|(x, y)| format!("({x},{y})"))
                .collect::<Vec<_>>()
                .join(",");
            Ok(Box::new(format!("({poly_str})")))
        }
        PostgreSQLValue::Array(items) => {
            let mut strs: Vec<String> = vec![];
            for item in items.iter() {
                let s = match item {
                    PostgreSQLValue::Text(s)
                    | PostgreSQLValue::Varchar(s)
                    | PostgreSQLValue::Char(s) => s.clone(),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported array item type in PostgreSQL array for item: {item:?}",
                        ));
                    }
                };
                strs.push(s);
            }
            Ok(Box::new(strs))
        }
        PostgreSQLValue::Hstore(h) => {
            // Convert hstore to string representation
            let hstore_str = h
                .iter()
                .map(|(k, v)| match v {
                    Some(val) => format!("\"{k}\"=>\"{val}\""),
                    None => format!("\"{k}\"=>NULL"),
                })
                .collect::<Vec<_>>()
                .join(",");
            Ok(Box::new(hstore_str))
        }
    }
}

pub async fn create_tables_and_indices(
    client: &Client,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        create_postgresql_table_from_test_table(client, table).await?;
    }
    for relation in &dataset.relations {
        create_postgresql_table_from_test_table(client, relation).await?;
    }
    Ok(())
}

#[derive(Debug)]
pub enum PostgresError {
    SQL(String, Box<tokio_postgres::error::Error>),
    UndefinedField(String),
}

impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresError::SQL(msg, err) => write!(f, "PostgreSQL SQL Error: {msg}: {err}"),
            PostgresError::UndefinedField(field) => {
                write!(f, "PostgreSQL Undefined Field Error: {field}")
            }
        }
    }
}

impl std::error::Error for PostgresError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PostgresError::SQL(_, e) => Some(&**e),
            PostgresError::UndefinedField(_) => None,
        }
    }
}
/// Create PostgreSQL table from TestTable schema
pub async fn create_postgresql_table_from_test_table(
    client: &Client,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use schema to create table (required field, no fallback)
    let create_sql = table.schema.postgresql.to_create_table_ddl(&table.name);
    client.execute(&create_sql, &[]).await.map_err(|e| {
        PostgresError::SQL(
            format!(
                "Error creating table {}: {}\nSQL: {}",
                table.name,
                e,
                create_sql.replace("\n", "")
            ),
            Box::new(e),
        )
    })?;

    // Create indexes if defined
    for index in &table.schema.postgresql.indexes {
        let index_sql = table
            .schema
            .postgresql
            .to_create_index_ddl(&table.name, index);
        client.execute(&index_sql, &[]).await.map_err(|e| {
            PostgresError::SQL(
                format!(
                    "Error creating index on table {}: {}\nSQL: {}",
                    table.name, e, index_sql
                ),
                Box::new(e),
            )
        })?;
    }
    Ok(())
}

pub async fn insert_rows(
    client: &Client,
    dataset: &TestDataSet,
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &dataset.tables {
        inject_test_table_postgresql(client, table).await?;
    }
    for relation in &dataset.relations {
        inject_test_table_postgresql(client, relation).await?;
    }
    Ok(())
}

/// Inject TestTable data into PostgreSQL
pub async fn inject_test_table_postgresql(
    client: &Client,
    table: &TestTable,
) -> Result<(), Box<dyn std::error::Error>> {
    if table.documents.is_empty() {
        return Err("No documents to insert".into());
    }

    // Insert data for each document
    for doc in &table.documents {
        let pg_doc = doc.to_postgresql_doc();

        if pg_doc.is_empty() {
            continue;
        }

        let columns: Vec<String> = pg_doc.keys().cloned().collect();
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table.name,
            columns.join(", "),
            placeholders.join(", ")
        );

        let params: Vec<Box<dyn ToSql + Sync>> = columns
            .iter()
            .map(|col| postgresql_value_to_param(&pg_doc[col]).unwrap())
            .collect();

        let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

        client.execute(&insert_sql, &param_refs).await?;
    }

    Ok(())
}
