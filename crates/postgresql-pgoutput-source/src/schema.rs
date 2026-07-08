//! PostgreSQL schema collection for type-aware conversion.

use anyhow::Result;
use surreal_sync_postgresql::schema::collect_database_schema_with_fks;
use sync_core::DatabaseSchema;
use tokio_postgres::Client;

pub async fn collect_postgresql_database_schema(client: &Client) -> Result<DatabaseSchema> {
    collect_database_schema_with_fks(client).await
}

pub async fn get_table_column_names_ordinal(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}
